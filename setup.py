#!/usr/bin/env python
from setuptools import setup

with open('justredis/__init__.py') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.strip().split('\'')[-2]

setup(
    name='justredis',
    version=version,
    packages=['justredis'],
    # TODO which version ?
    python_requires=">=3.7",
    install_requires=['hiredis>=0.1.3'],
    test_suite="tests"
)
