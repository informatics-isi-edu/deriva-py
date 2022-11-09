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


def get_readme_contents():
    with io.open('README.md') as readme_file:
        return readme_file.read()


url = "https://github.com/informatics-isi-edu/deriva-py"
author = 'USC Information Sciences Institute, Informatics Systems Research Division'
author_email = 'isrd-support@isi.edu'


setup(
    name='deriva',
    description='Python APIs and CLIs (Command-Line Interfaces) for the DERIVA platform.',
    long_description='For further information, visit the project [homepage](%s).' % url,
    long_description_content_type='text/markdown',
    url=url,
    author=author,
    author_email=author_email,
    maintainer=author,
    maintainer_email=author_email,
    version=__version__,
    packages=find_packages(exclude=["tests"]),
    package_data={
        'deriva.config': ['examples/*.json'],
        'deriva.core': ['schemas/*.schema.json']
    },
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, <4',
    entry_points={
        'console_scripts': [
            'deriva-upload-cli = deriva.transfer.upload.__main__:main',
            'deriva-download-cli = deriva.transfer.download.__main__:main',
            'deriva-hatrac-cli = deriva.core.hatrac_cli:main',
            'deriva-acl-config = deriva.config.acl_config:main',
            'deriva-annotation-config = deriva.config.annotation_config:main',
            'deriva-annotation-dump = deriva.config.dump_catalog_annotations:main',
            'deriva-annotation-rollback = deriva.config.rollback_annotation:main',
            'deriva-annotation-validate = deriva.config.annotation_validate:main',
            'deriva-sitemap-cli = deriva.seo.sitemap_cli:main',
            'deriva-backup-cli = deriva.transfer.backup.__main__:main',
            'deriva-restore-cli = deriva.transfer.restore.__main__:main',
            'deriva-globus-auth-utils = deriva.core.utils.globus_auth_utils:main'
        ]
    },
    install_requires=[
        'setuptools>=20.2',
        'packaging',
        'requests',
        'certifi',
        'pika',
        'urllib3>=1.1.5,<2.0',
        'portalocker>=1.2.1',
        'portalocker<2.0; python_version <= "3.5"',
        'scandir; python_version <= "2.7"',
        'bdbag>=1.6.0',
        'globus_sdk<4',
        'fair-research-login>=0.2.6',
        'fair-identifiers-client>=0.5',
        'jsonschema>=3.1'
    ],
    license='Apache 2.0',
    classifiers=[
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ]
)
