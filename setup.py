from setuptools import setup, find_packages

setup(name='lagents',
      version='0.1.0',
      description='queue tasks to be done later',
      url='http://github.com/snickers10m/lagents',
      author='Charlie Moreley',
      author_email='charlie@gmail.com',
      license='MIT',
      packages=find_packages(exclude=('tests', 'docs'))
      )
