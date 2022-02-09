# -*- coding: utf-8 -*-
from setuptools import setup

readme = 'README.md'
setup(
    name="pymysql-pool",
    version="0.3.5",
    url="https://github.com/jkklee/pymysql-pool",
    author="ljk",
    py_modules=['pymysqlpool'],
    license="GPLv3",
    author_email="chaoyuemyself@hotmail.com",
    description="MySQL connection pool based pymysql",
    long_description=open(readme, encoding='utf-8').read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.4",
    install_requires=['pymysql>=0.7.10'],
    keywords=[
        'pymysql pool',
        'mysql connection pool',
        'mysql multi threads'
    ]
)
