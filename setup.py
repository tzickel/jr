#!/usr/bin/env python
from setuptools import setup

from justredis import __version__

setup(
    name='justredis',
    version=__version__,
    packages=['justredis'],
    python_requires=">=2.7",
    install_requires=['hiredis>=0.1.3'],
    test_suite="tests"
)
