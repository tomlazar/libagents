from setuptools import setup, find_packages

setup(name='libagents',
      version='0.1.1',
      description='queue tasks to be done later',
      url='http://github.com/snickers10m/lagents',
      author='Charlie Morley',
      author_email='charlie@gmail.com',
      license='MIT',
      packages=find_packages(exclude=('tests', 'docs'))
      )
