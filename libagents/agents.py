import queue
import threading
import functools

class ResourcePool:
    """An adjustable-capacity generic-resource pool:
    The user specifies the methods to construct and dispose their resource of choice, this class simply
    manages the usage of those resources."""

    def __init__(self, max_resources, initialize_resource, dispose_resource):
        """Initializes an empty, unfrozen resource pool with an initial capacity.
        No resources are initialized until reserve_resource or initialize_to_capacity is called."""
        self.__max_resources = max_resources
        self.__initialize_resource = initialize_resource
        self.__dispose_resource = dispose_resource

        self.__resource_queue = queue.Queue()
        self.__live_resources = 0
        self.__lock = threading.Condition(threading.RLock())

        self.__frozen = False
        self.__frozen_lock = threading.Condition(threading.RLock())

    """The below are used in python "with" statements, in order to thread-lock multiple calls to the pool at once, e.g.:
        pool = ResourcePool(...)
        with pool:
            # all of these happen together (atomically)
            pool.set_max_resources(2)
            pool.freeze()
            pool.join()
    """

    def __enter__(self):
        self.__lock.acquire()

    def __exit__(self):
        self.__lock.release()

    """Resource queue access notes:
        (related to the methods below)

        The queue's built-in "join()" counts opposite to what we want here. queue.join() natively returns
        when the queue is empty. We want ResourcePool.join(), and by extension resource_queue.join(), to
        return when the queue is back up to full - all of the resources are free.

        To facilitate this, there are various extra calls to the resource queue around the class that
        offset eachother.

        For reference, queue.put(...) increases the join() distance, and queue.task_done() decreases the
        join distance. The join distance is recorded internally in the queue - you can only keep track of
        it by keeping track of the put(...) and task_done() calls.

        The creation of a resource (or rather, addind a new resource to the queue):
             - should not adjust the join-distance: the number of live resources increases, as well as
                the number of free resources
             - therefore, the code is:
                create # +/- 0
                live_resources++ # +/- 0
                queue.put # +1
                queue.task_done # -1

        The reservation of a resource:
             - should increase the join-distance: the number of live resources stays the same, but the
                number of free resources decreases (so it takes one more step to reach all free resources)
             - therefore, the code is:
                queue.get # +/- 0
                queue.put # +1
                queue.get # +/- 0

        The return of a resource:
             - should decrease the join-distance: the number of live resources stays the same, but the
                number of free resources increases (so we are one step closer to all free resources)
             - therefore, the code is:
                queue.put # +1
                queue.task_done # -1
                queue.task_done # -1 -- note that this offsets the +1 in reservation

        The death/disposal of a resource (or rather, not adding an existing resource back into the queue):
             - should not adjust the join-distance: the number of live resources decreases, as well as
                the number of free resources
             - therefore, the code is:
                live_resources-- # +/- 0
    """

    """
    The code below directly matches the pseudocode explained above.
    """

    def __create_resource_and_put_into_queue(self):
        resource = self.__initialize_resource()
        self.__live_resources += 1
        self.__resource_queue.put(resource)
        self.__resource_queue.task_done()

    def __reserve_resource_from_queue(self):
        self.__resource_queue.put( self.__resource_queue.get() )
        return self.__resource_queue.get()

    def __return_resource_to_queue(self, resource):
        self.__resource_queue.put(resource)
        self.__resource_queue.task_done()
        self.__resource_queue.task_done()

    def __remove_resource_that_would_have_gone_into_queue(self, resource):
        self.__live_resources -= 1

    def reserve_resource(self, *, block=True):
        """Select and return an arbitrary resource from the pool, or create and return a new resource if none are free and there is free capacity in the pool.
        If block is true, waits until the pool is not frozen, and there are free resources or free capacity in the pool.
        If block is false, returns immediately if the pool is frozen, or if there are no free resources and no free capacity in the pool."""
        with self.__frozen_lock:
            if self.__frozen and not block:
                return None
            else:
                while self.__frozen:
                    self.__frozen_lock.wait()

        with self.__lock:
            if not self.__resource_queue.empty():
                return self.__reserve_resource_from_queue()
            elif self.__live_resources < self.__max_resources:
                self.__create_resource_and_put_into_queue()
                return self.__reserve_resource_from_queue()
            elif block:
                self.__lock.wait()
                return self.reserve_resource(block=True)
            else:
                return None

    def return_resource(self, resource, *, isDead=False, dispose=False):
        """Return the resource to the pool, to be considered as a free resource. Further reservations can take this resource.
        If isDead or dispose are true, the resource is not added back to the free pool -- the number of live resources decreases.
        If dispose is true, the resource is disposed with the method passed into the pool constructor.
        If there are more live resources than capacity of the pool allows, the resource is disposed."""
        with self.__lock:
            if self.__resource_queue.qsize() == self.__live_resources:
                raise ValueError('self: all resources already returned')
            elif isDead:
                self.__remove_resource_that_would_have_gone_into_queue(resource)
            elif dispose or self.__live_resources > self.__max_resources:
                self.__dispose_resource(resource)
                self.__remove_resource_that_would_have_gone_into_queue(resource)
            else:
                self.__return_resource_to_queue(resource)
                self.__lock.notify()

    def get_max_resources(self):
        """The capacity for active resources in the pool.
        May temporarily be lesser than the number of active resources in the pool if decreased with set_max_resources."""
        with self.__lock:
            return self.__max_resources

    def set_max_resources(self, max_resources):
        """Set a new capacity for the pool.
        If increasing, allows further creation of resources when reserving or initializing to capacity.
        If decreasing, disposes free resources until the capacity is met. If not enough free resources can be disposed, resources
        are disposed when they are returned."""
        with self.__lock:
            difference = max_resources - self.__max_resources
            self.__max_resources = max_resources

            if difference < 0:
                resources_to_remove = min(abs(difference), self.__resource_queue.qsize())
                for i in range(resources_to_remove):
                    resource = self.__resource_queue.get()
                    self.__dispose_resource(resource)
                    self.__remove_resource_that_would_have_gone_into_queue(resource)
            else:
                self.__lock.notify(difference)

    def get_live_resources(self):
        """The number of active resources in the pool, either free or reserved.
        If less than the capacity of the pool, more resources can be created at reservation or initialization to capacity."""
        with self.__lock:
            return self.__live_resources

    def get_free_resources(self):
        """The number of active resources not being used, and ready to be reserved."""
        with self.__lock:
            return self.__resource_queue.qsize()

    def get_reserved_resources(self):
        """The number of active resources currently being used, and waiting to be returned."""
        with self.__lock:
            return self.get_live_resources() - self.get_free_resources()

    def initialize_to_capacity(self):
        """Initialize enough resources to fill the capacity of the pool, and add the new resources as free resources ready to be reserved."""
        with self.__lock:
            new_resources = self.__max_resources - self.__live_resources
            for i in range(new_resources):
                self.__create_resource_and_put_into_queue()
            self.__lock.notify(new_resources)

    def join(self):
        """Block until all resources are free."""
        self.__resource_queue.join()

    def freeze(self):
        """Suspend further reservation of resources. Continue accepting returns of resources."""
        with self.__frozen_lock:
            self.__frozen = True

    def is_frozen(self):
        with self.__frozen_lock:
            return self.__frozen

    def unfreeze(self):
        """Resume reservation of resources."""
        with self.__frozen_lock:
            self.__frozen = False
            self.__frozen_lock.notify_all()

class Agent:
    """A manager of a queue of work (calls to a single method), using an adjustable-capacity ResourcePool of work-executing threads.
    More simply, a thread manager.

    Per the workings of ResourcePool, typically no work-executing threads are created until they are needed, but they are kept around
    for further use after they are created."""

    __Mailbox = lambda: queue.Queue(maxsize=1)

    __worker_job_type_finalize = 1_0
    __worker_job_type_dowork = 1_1

    __manager_job_type_finalize = 2_0
    __manager_job_type_dowork = 2_1
    __manager_job_type_reset = 2_2

    def __init__(self, method, max_workers=1):
        """Initialize an Agent which operates on the given method, and has an initial given worker-thread capacity.
        Starts the internal thread to manage the work queue -- the Agent is ready to receive work."""

        self.__method = method
        self.__worker_pool = ResourcePool(
            max_resources = max_workers,
            initialize_resource = self.__initialize_worker,
            dispose_resource = self.__dispose_worker)

        self.__work_queue = queue.Queue()
        self.__work_queue_enabled = True
        self.__work_queue_enabled_lock = threading.Lock()
        threading.Thread(target=self.__manager_loop, daemon=False).start()

    def __initialize_worker(self):
        mailbox = Agent.__Mailbox()
        threading.Thread(target=self.__worker_loop, args=(mailbox,)).start()
        return mailbox

    def __dispose_worker(self, mailbox):
        mailbox.put((Agent.__worker_job_type_finalize, None))

    def __worker_loop(self, mailbox):
        while (True):
            (job_type, job_args) = mailbox.get()
            try:
                if job_type == Agent.__worker_job_type_dowork:
                    (method_args, result_return) = job_args
                    try:
                        result = self.__method(*method_args)
                        if result_return:
                            result_return(result)
                    except:
                        if result_return:
                            result_return(None)
                        raise
                    self.__worker_pool.return_resource(mailbox)
                elif job_type == Agent.__worker_job_type_finalize:
                    break
                else:
                    raise ValueError("job_type: agent worker received invalid job type")
            except:
                self.__worker_pool.return_resource(mailbox, isDead=True)
                raise
            finally:
                mailbox.task_done()

    def __manager_loop(self):
        while (True):
            (job_type, job_args) = self.__work_queue.get()

            try:
                if job_type == Agent.__manager_job_type_dowork:
                    worker_mailbox = self.__worker_pool.reserve_resource()
                    worker_mailbox.put((Agent.__worker_job_type_dowork, job_args))
                elif job_type == Agent.__manager_job_type_finalize:
                    self.__worker_pool.set_max_resources(0)
                    self.__worker_pool.freeze()
                    break
                elif job_type == Agent.__manager_job_type_reset:
                    (callback,) = job_args

                    self.__worker_pool.freeze()
                    max_resources = self.__worker_pool.get_max_resources()
                    self.__worker_pool.set_max_resources(0)
                    self.__worker_pool.join()
                    self.__worker_pool.set_max_resources(max_resources)
                    self.__worker_pool.unfreeze()

                    if callback:
                        callback()
                else:
                    raise ValueError("job_type: agent manager received invalid job type")
            finally:
                self.__work_queue.task_done()

    def __queue_job(self, job_type, job_args):
        with self.__work_queue_enabled_lock:
            if self.__work_queue_enabled:
                self.__work_queue.put((job_type, job_args))
            else:
                raise ValueError('self: cannot add more jobs to a finalized Agent')

    def __queue_work(self, method_args, callback=None):
        self.__queue_job(Agent.__manager_job_type_dowork, (method_args, callback))

    def execute_async(self, *method_args):
        """Queue the specified arguments to be run in the Agent's method, and return immediately after queueing the work.
        When a worker-thread is available it will run the method and not return any result."""
        self.__queue_work(method_args)

    def execute(self, *method_args):
        """Queue the specified arguments to be run in the Agent's method, and block until the Agent's method call is done.
        When a worker-thread is available it will run the method and return the result out of this call."""
        blocking_mailbox = Agent.__Mailbox()
        def job_callback(result=None):
            blocking_mailbox.put(result)

        self.__queue_work(method_args, job_callback)

        result = blocking_mailbox.get()
        blocking_mailbox.task_done()
        return result

    def finalize(self):
        """Permanently stop further queueing of work, and dispose of all worker-threads and the Agent thread when all work is complete, return immediately."""
        with self.__work_queue_enabled_lock:
            self.__work_queue_enabled = False
            self.__work_queue.put((Agent.__manager_job_type_finalize, None))

    def initialize_all_workers(self):
        """Pre-emptively startup any additional worker threads to fill the capacity of the Agent."""
        self.__worker_pool.initialize_to_capacity()

    def join(self):
        """Block until there is no work left in the work queue and all worker-threads are finished working.
        Any additional work added while this call blocks will also be waited on by this call - this call returns
        when all worker-threads are not receiving any work."""
        while (not self.__work_queue.empty()) or (self.__worker_pool.get_reserved_resources() > 0):
            self.__work_queue.join()
            self.__worker_pool.join()

    def minimize(self):
        """Immediately dispose of any worker-threads which are not currently in use, reducing the memory used by the Agent."""
        max_resources = self.__worker_pool.get_max_resources()
        self.__worker_pool.set_max_resources(0)
        self.__worker_pool.set_max_resources(max_resources)

    def pause(self):
        """Immediately pause the allocation of work to worker-threads.
        Currently running worker-threads will continue their work until finished, and then wait for the Agent to be unpaused."""
        self.__worker_pool.freeze()

    def unpause(self):
        """Immediately resume the allocation of work to worker-threads."""
        self.__worker_pool.unfreeze()

    def reset(self, *, block=False):
        """Queue a job for the Agent to dispose inactive worker-threads until all threads are disposed, and then resume allocation of work to new worker-threads.
        If block is True, this call returns once the allocation of work is to new worker-threads is resumed."""
        if block:
            monitor = threading.Condition(threading.Lock())
            def job_callback(*args, **kwargs):
                with monitor:
                    monitor.notify_all()

            with monitor:
                self.__queue_job(Agent.__manager_job_type_reset, (job_callback,))
                monitor.wait()
        else:
            self.__queue_job(Agent.__manager_job_type_reset, (None,))

    def get_queued_work_count(self):
        """Return the number of queued Agent work items that are not currently being worked on."""
        return self.__work_queue.qsize()

    def get_active_work_count(self):
        """Return the number of Agent work items that are currently being run in a worker-thread."""
        return self.__worker_pool.get_reserved_resources()

    def get_work_count(self):
        """Return the total number of Agent work items being tracked - either queued for work or currently being run in a worker-thread."""
        return self.get_queued_work_count() + self.get_active_work_count()

    def set_max_workers(self, max_workers):
        """Adjust the maximum number of worker-threads, and start disposal of worker threads, if necessary.
        If the capacity has been decreased, no running worker-threads will be stopped abruptly, but worker-threads will
        continue to be stopped as they finish and become ready for more work until the new maximum is reached.
        If the capacity has been increased, no new worker-threads will be initialized until they are needed, or are forced
        to be initialized by initialize_all_workers."""
        self.__worker_pool.set_max_resources(max_workers)
