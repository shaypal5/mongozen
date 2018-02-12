"""Setup for the mongozen package."""

# !/usr/bin/env python
# -*- coding: utf-8 -*-

import setuptools
import versioneer


TEST_REQUIRES = [
    # testing and coverage
    'pytest', 'coverage', 'pytest-cov',
    # unmandatory dependencies of the package itself
    'birch', 'pandas', 'pandas.compat',
    # to be able to run `python setup.py checkdocs`
    'collective.checkdocs', 'pygments',
]

with open('README.rst') as f:
    README = f.read()

setuptools.setup(
    author="Shay Palachy",
    author_email="shay.palachy@gmail.com",
    name='mongozen',
    description='MongoDB utilities for Python',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    long_description=README,
    url='https://github.com/shaypal5/mongozen',
    packages=setuptools.find_packages(),
    include_package_data=True,
    # entry_points='''
    # [console_scripts]
    #     mongozen=mongozen.scripts.mongozen_cli:cli
    # ''',
    python_requires=">=3.5",
    install_requires=[
        'pymongo', 'numpy', 'prettytable', 'pandas',
        'strct>=0.0.14', 'utilitime>=0.0.3', 'comath>=0.0.3',
    ],
    extras_require={
        'test': TEST_REQUIRES,
    },
    platforms=['any'],
    keywords='mongodb',
    classifiers=[
        # Trove classifiers
        # (https://pypi.python.org/pypi?%3Aaction=list_classifiers)
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
    ],
)
