# -*- coding: utf-8 -*-
"""Python toolkit for Tinker Pop 3 Gremlin Server"""

import os

from setuptools import find_packages, setup

tests_require = [
    'check-manifest>=0.25',
    'coverage>=4.0',
    'isort>=4.2.2',
    'pydocstyle>=1.0.0',
    'pytest-asyncio>=0.8.0',
    'pytest-cache>=1.0',
    'pytest-cov>=2.5.1',
    'pytest-pep8>=1.0.6',
    'pytest>=3.2.1',
    'uvloop>=0.8.1',
]

extras_require = {
    'docs': [
        'Sphinx>=1.6.3',
        'alabaster>=0.7.10',
    ],
    'tests': tests_require,
}

extras_require['all'] = []
for name, reqs in extras_require.items():
    extras_require['all'].extend(reqs)

setup_requires = [
    'pytest-runner>=2.6.2',
]

install_requires = [
    'aiogremlin==3.2.6rc1',
    'gremlinpython==3.2.6',
    'inflection==0.3.1',
]

packages = find_packages()

# Get the version string. Cannot be done with import!
g = {}
with open(os.path.join('goblin', 'version.py'), 'rt') as fp:
    exec(fp.read(), g)
    version = g['__version__']


setup(
    name='goblin',
    version=version,
    url='https://github.com/davebshow/goblin',
    license='Apache License 2.0',
    author='davebshow',
    author_email='davebshow@gmail.com',
    description=__doc__,
    packages=packages,
    install_requires=install_requires,
    extras_require=extras_require,
    test_suite='tests',
    setup_requires=setup_requires,
    tests_require=tests_require,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache License, Version 2.0',
        'Operating System :: OS Independent',
    ],
)
