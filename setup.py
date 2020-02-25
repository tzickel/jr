#!/usr/bin/env python
from setuptools import setup

version = None
with open('justredis/__init__.py') as f:
    for line in f:
        if line.startswith('__version__'):
            version = line.strip().split('\'')[-2]
            break

if not version:
    raise Exception('Version for justredis not found')

setup(
    name='justredis',
    version=version,
    packages=['justredis'],
    python_requires=">=3.6",
    install_requires=['hiredis>=0.1.3'],
    test_suite="tests"
)
