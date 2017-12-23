#!/usr/bin/env python

import os
import sys
sys.version_info
from setuptools import setup, find_packages

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

install_requires = [
    'elasticsearch == 2.4.1',
    'elasticsearch-dsl == 2.2.0',
    'PycURL'
    ]

if sys.version_info < (3,):
    install_requires += ['configparser']

setup(
    name = "EsCmd",
    version = "0.1",
    author = "Fabrice Bacchella",
    author_email = "fabrice.bacchella@3ds.com",
    description = "Command line tool to manage ElasticSearch.",
    license = "Apache",
    keywords = "CLI Elasticsearch",
    install_requires = install_requires,
    url = "https://github.com/fbacchella/eslib",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "escmd=eslib.escmd:main_wrap",
            "escmd%s=eslib.escmd:main_wrap" % sys.version[:1],
            "escmd%s=eslib.escmd:main_wrap" % sys.version[:3],
        ],
    },
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: Apache Software License",
        "Classifier: Operating System :: OS Independent",
        "Environment :: Console",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 3",
    ],
    platforms=["Posix", "MacOS X"],
)