from setuptools import setup, find_packages


setup(
    name='pysqlar',
    version='0.1.1',
    description='Pure Python library for working with SQLite Archives.',
    long_description="""# pysqlar

Module for working with [SQLite Archive files](https://www.sqlite.org/sqlar.html)
with an API mimicking the zipfile module.

The module requires the zlib and sqlite3 support, but has no external dependencies.
""",
    long_description_content_type='text/markdown',
    url='http://github.com/Frojdholm/pysqlar',
    author='Hampus Fröjdholm',
    author_email='hampus.frojdholm@gmail.com',
    license='MIT',
    packages=find_packages(),
    python_requires='>=3',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ]
)
