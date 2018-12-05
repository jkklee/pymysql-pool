#!/usr/bin/env python
from setuptools import setup

setup(
    name="pymysql-pool",
    version="0.3.0",
    url='https://github.com/jkklee/pymysql-pool',
    author='ljk',
	py_modules=['pymysqlpool'],
	license='GPLv3',
    author_email='chaoyuemyself@hotmail.com',
    description='MySQL connection pool based pymysql',
	long_description='A simple connection pool based PyMySQL. Mainly focus on "multi threads" or "async" mode when use "pymysql", but also compatible with single thread mode for convenience when you need to use these two mode together. Within multi threads mode support the multiplexing similar feature(when use connection with "Context Manager Protocol").',
    python_requires=">=3.4",
    install_requires=['pymysql>=0.7.10'],
    keywords=[
	    'pymysql pool',
        'mysql connection pool',
        'mysql multi threads'
    ]
)
