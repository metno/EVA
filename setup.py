#!/usr/bin/env python3
#
# Follows Semantic Versioning, as described here: http://semver.org/
#
# Given a version number MAJOR.MINOR.PATCH, increment the:
#
# MAJOR version when you make incompatible API changes,
# MINOR version when you add functionality in a backwards-compatible manner, and
# PATCH version when you make backwards-compatible bug fixes.
#
# Additional labels for pre-release and build metadata are available as
# extensions to the MAJOR.MINOR.PATCH format.
#

VERSION = [4, 3, 0]

config = {
    'description': 'The Event Adapter',
    'author': 'Team IT Modellprod @ MET Norway',
    'url': 'https://github.com/metno/eva',
    'download_url': 'https://github.com/metno/eva',
    'version': '.'.join([str(x) for x in VERSION]),
    'install_requires': [
        'nose==1.3.7',
        'python-dateutil==2.5.0',
        'productstatus-client==6.4.0',
        'paramiko==2.10.1',
        'httmock==1.2.4',
        'jinja2==2.11.3',
        'kazoo==2.2.1',
        'Sphinx==1.4.8',
        'falcon==1.1.0',
    ],
    'packages': [],
    'scripts': [],
    'name': 'eva'
}

if __name__ == '__main__':
    try:
        from setuptools import setup
    except ImportError:
        from distutils.core import setup

    setup(**config)
