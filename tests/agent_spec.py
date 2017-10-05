from context import lagents
from lagents import *

import queue
import threading
import functools
import unittest
import time, multiprocessing

class AgentSpec_Safety(unittest.TestCase):
    """Intended to be run before AgentSpec, this spec tests the cleanup capabilities of the Agent, so that other tests will not
    cause resource-holding-related issues."""

    def __pause(delay):
        def it():
            time.sleep(delay)
        return it

    def finalize_an_agent(pause_delay):
        agent = Agent(AgentSpec_Safety.__pause(pause_delay), max_workers=3)
        agent.execute_async()
        agent.finalize()

    def test_finalize(self):
        pause_delay = 1
        p = multiprocessing.Process(target=AgentSpec_Safety.finalize_an_agent, args=(1,))
        p.start()
        time_limit = pause_delay + 1 #seconds
        p.join(time_limit)

        if p.is_alive():
            p.terminate()
            p.join()
            raise AssertionError("finalization did not clean up all threads.")


class AgentSpec(unittest.TestCase):

    def __TestMethodBasic(pause=None):
        def it(x):
            if pause:
                time.sleep(pause)
            return x * 2
        return it

    def __TestMethodMailbox(mailbox, pause=None):
        def it(x):
            if pause:
                time.sleep(pause)
            mailbox.put_nowait(x * 2)
        return it

    def __setup(self, *args, **kwargs):
        self._agent = Agent(*args, **kwargs)

    def __tearDown(self):
        self._agent.finalize()

    def assertExecutionTime(self, f, max_time, min_time=None):
        output = queue.Queue(maxsize=1)
        def run_function():
            output.put_nowait(f())

        f_thread = threading.Thread(target=run_function, daemon=True)
        start_time = time.time()
        f_thread.start()
        f_thread.join(max_time)
        end_time = time.time()

        if f_thread.is_alive():
            raise AssertionError("The function took too much time to produce a result. (expecting less than " + str(max_time) + " seconds)")
        elif min_time and (end_time - start_time) < min_time:
            raise AssertionError("The function didn't take enough time to produce a result.\n(It took " + str(end_time - start_time) + " seconds but was expecting at least " + str(min_time) + " seconds)")
        else:
            result = output.get()
            output.task_done()
            return result


    def test_execute(self):
        mailbox = queue.Queue()
        self.__setup(AgentSpec.__TestMethodMailbox(mailbox), max_workers=3)

        try:
            self._agent.execute(3)
            self.assertEqual(6, mailbox.get_nowait())

            self._agent.execute(5)
            self._agent.execute(10)
            self.assertEqual(10, mailbox.get_nowait())
            self.assertEqual(20, mailbox.get_nowait())
        finally:
            self.__tearDown()

    def test_execute_blocks(self):
        pause = 0.4
        self.__setup(AgentSpec.__TestMethodBasic(pause), max_workers=3)

        try:
            result = self.assertExecutionTime(lambda: self._agent.execute(3), min_time=pause, max_time=(pause + 0.1))
            self.assertEqual(6, result)
        finally:
            self.__tearDown()

    def test_execute_async(self):
        mailbox = queue.Queue()
        self.__setup(AgentSpec.__TestMethodMailbox(mailbox, pause=0.4), max_workers=3)

        try:
            inputs = [1, 5, 10]
            outputs = [2, 10, 20]
            for input in inputs:
                self.assertExecutionTime(lambda: self._agent.execute_async(input), max_time=0.1)

            for i in range(len(outputs)):
                output = mailbox.get()
                self.assertTrue(output in outputs)
                outputs.remove(output)
                mailbox.task_done()

            self.assertTrue(mailbox.empty())
        finally:
            self.__tearDown()

    def test_error_handling(self):
        normal_input = 2
        error_input = 3
        def test_method_error(x):
            if x == error_input:
                raise Exception("This is a test error -- please ignore, this does not indicate a failed test")
            else:
                return x * 2

        self.__setup(test_method_error, max_workers=1)

        try:
            self.assertEqual(None, self._agent.execute(error_input))
            self.assertEqual(normal_input * 2, self.assertExecutionTime(lambda: self._agent.execute(normal_input), max_time=1))
        finally:
            self.__tearDown()

    def test_join(self):
        pause = 0.5
        max_workers = 3
        self.__setup(AgentSpec.__TestMethodBasic(pause), max_workers)

        try:
            for i in range(max_workers * 4):
                self._agent.execute_async(i)
            self.assertExecutionTime(lambda: self._agent.join(), min_time=((pause * 4) - 0.2), max_time=((pause * max_workers * 3) + 0.1))
        finally:
            self.__tearDown()

    def test_initialize_all_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.initialize_all_workers()
        finally:
            self.__tearDown()

    def test_reset_block_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.initialize_all_workers()
            self._agent.reset(block=True)
        finally:
            self.__tearDown()

    def test_reset_async_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.initialize_all_workers()
            self._agent.reset()
        finally:
            self.__tearDown()

    def test_count_empty_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.get_queued_work_count()
            self._agent.get_active_work_count()
            self._agent.get_work_count()
        finally:
            self.__tearDown()

    def test_count_some_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.execute_async(0)
            self._agent.get_queued_work_count()
            self._agent.get_active_work_count()
            self._agent.get_work_count()
        finally:
            self.__tearDown()

    def test_count_many_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            for i in range(9):
                self._agent.execute_async(0)
            time.sleep(0.1)
            self._agent.get_queued_work_count()
            self._agent.get_active_work_count()
            self._agent.get_work_count()
        finally:
            self.__tearDown()

    def test_minimize_empty_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.minimize()
        finally:
            self.__tearDown()

    def test_minimize_all_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.initialize_all_workers()
            self._agent.minimize()
        finally:
            self.__tearDown()

    def test_minimize_some_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.initialize_all_workers()
            self._agent.execute_async(0)
            self._agent.minimize()
        finally:
            self.__tearDown()

    def test_minimize_none_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:
            self._agent.execute_async(0)
            self._agent.minimize()
        finally:
            self.__tearDown()

    def test_pause_doesnt_error(self):
        self.__setup(AgentSpec.__TestMethodBasic(0.5), 3)
        try:

            for i in range(9):
                self._agent.execute_async(0)
            self._agent.pause()
            time.sleep(0.7)
            self._agent.get_queued_work_count()
            self._agent.get_active_work_count()
            self._agent.get_work_count()
            self._agent.unpause()
        finally:
            self.__tearDown()

if __name__ == "__main__":
    print('Running safety tests to determine if Agents can be disposed of correctly...\n')
    safety_test = unittest.main(exit=False, defaultTest="AgentSpec_Safety");

    if len(safety_test.result.errors) == 0 and len(safety_test.result.failures) == 0:
        print('\n\nSafety tests passed. Running Agent unit tests...\n')
        time.sleep(0.5)
        unittest.main(exit=True, defaultTest="AgentSpec")
