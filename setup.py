# -*- coding: utf-8 -*-
"""Setup for Drastic
"""
__copyright__ = "Copyright (C) 2016 University of Maryland"
__license__ = "GNU AFFERO GENERAL PUBLIC LICENSE, Version 3"


import inspect
import os
from distutils.core import setup
from setuptools import setup, find_packages


setup(
    name='drastic',
    version="1.0",
    description='Drastic core library',
    extras_require={},
    long_description="Core library for Drastic development",
    author='Archive Analytics',
    maintainer_email='@archiveanalytics.com',
    license="Apache License, Version 2.0",
    url='https://bitbucket.org/archivea/libdrastic',
    install_requires=[
        "cassandra-driver==2.5.1",
        "passlib==1.6.2",
        "nose==1.3.6",
        "blist==1.3.6",
        "requests==2.7.0",
        "crcmod==1.7"
    ],
    entry_points={
        'console_scripts': [
            "drastic = drastic.cli:main"
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