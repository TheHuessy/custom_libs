from distutils.core import setup
# from setuptools import setup, find_packages

setup(
        name='custom_libs',
        version='0.1.0',
        author='James Huessy',
        packages=['custom_libs'],
        license='LICENSE.txt',
        description='Useful tools and utils used frequently.',
        long_description=open('README.md').read(),
        install_requires=[
            "sqlalchemy",
            "pyaml",
            "pandas"
            ],
        )

