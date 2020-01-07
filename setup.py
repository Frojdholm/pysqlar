from setuptools import setup, find_packages
from pathlib import Path


this_folder = Path(__file__).parent
with open(this_folder / "README.md", encoding="utf-8") as f:
    long_description = f.read()


setup(
    name='pysqlar',
    version='0.1',
    description='Pure Python library for working with SQLite Archives.',
    long_description=long_description,
    url='http://github.com/Frojdholm/pysqlar',
    author='Hampus Fröjdholm',
    author_email='hampus.frojdholm@gmail.com',
    license='MIT',
    packages=find_packages(),
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ]
)
