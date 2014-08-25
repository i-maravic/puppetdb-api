#! /usr/bin/env python
# -*- coding: utf-8 -*-
# Copyright (c) 2014 Igor Maravić

import setuptools
__VERSION__ = '1.0'
setuptools.setup(
    name='puppetdb_api',
    author='Igor Maravić',
    author_email='igor@spotify.com',
    url='https://github.com/i-maravic/puppetdb-api',
    download_url='https://github.com/i-maravic/puppetdb-api/tarball/' + __VERSION__,
    version=__VERSION__,
    description='Python wrapper for puppetDB API',
    packages=['puppetdb_api'],
    requires=['requests', 'unittest2', 'httpmock'],
    license='Apache 2.0',
    classifiers=(
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7'
    ),
)
