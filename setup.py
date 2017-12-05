#
# Copyright 2017 University of Southern California
# Distributed under the Apache License, Version 2.0. See LICENSE for more info.
#

""" Installation script for the deriva package.
"""

from setuptools import setup, find_packages
import re
import io

__version__ = re.search(
    r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
    io.open('deriva/core/__init__.py', encoding='utf_8_sig').read()
    ).group(1)

setup(
    name="deriva",
    description="DERIVA Python APIs",
    url='https://github.com/informatics-isi-edu/deriva-py',
    maintainer='USC Information Sciences Institute ISR Division',
    maintainer_email='misd-support@isi.edu',
    version=__version__,
    packages=find_packages(),
    package_data={},
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'deriva-upload-cli = deriva.transfer.upload.__main__:main',
            'deriva-hatrac-cli = deriva.transfer.hatrac.cli:main',
            'deriva-acl-config = deriva.config.acl_config:main',
            'deriva-annotation-config = deriva.config.annotation_config:main',
            'deriva-annotation-dump = deriva.config.dump_catalog_annotations:main'
        ]
    },
    requires=[
        'os',
        'sys',
        'time',
        'datetime',
        'platform',
        'logging',
        'hashlib',
        'base64',
        'errno',
        'json',
        'mimetypes',
        'scandir',
        'requests',
        'pika',
        'portalocker',
        'setuptools'],
    install_requires=[
        'requests',
        'pika',
        'portalocker>=1.1.0'
    ],
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        "Operating System :: POSIX",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5'
    ]
)

