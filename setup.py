# -*- coding: utf-8 -*-
"""Setup for Indigo

Copyright 2015 Archive Analytics Solutions

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""
import inspect
import os
from distutils.core import setup
from setuptools import setup, find_packages


setup(
    name='indigo',
    version="1.1",
    description='Indigo core library',
    extras_require={},
    long_description="Core library for Indigo development",
    author='Archive Analytics',
    maintainer_email='@archiveanalytics.com',
    license="Apache License, Version 2.0",
    url='https://bitbucket.org/archivea/libindigo',
    install_requires=[
        "cassandra-driver==3.2.2",
        "passlib==1.6.2",
        "nose==1.3.6",
        "blist==1.3.6",
        "requests==2.7.0",
        "crcmod==1.7"
    ],
    entry_points={
        'console_scripts': [
            "indigo = indigo.cli:main"
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Internet :: WWW/HTTP :: WSGI",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Middleware",
        "Topic :: System :: Archiving"
    ],
)
