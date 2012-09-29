#!/usr/bin/env python

from distutils.core import setup

setup( name = 'ingestion',
       version = '0.1',
       description='DPLA Ingestion Subsystem',
       author='Mark Baker',
       author_email='mark@zepheira.com',
       url='http://dp.la',
       scripts=['scripts/poll_profiles'],
)
