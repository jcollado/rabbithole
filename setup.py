#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'PyYAML',
    'blinker',
    'pika',
    'sqlalchemy',
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='rabbithole',
    version='0.2.0',
    description="Store messages from an AMQP server into a SQL database",
    long_description=readme + '\n\n' + history,
    author="Javier Collado",
    author_email='javier@gigaspaces.com',
    url='https://github.com/jcollado/rabbithole',
    packages=[
        'rabbithole',
    ],
    package_dir={'rabbithole':
                 'rabbithole'},
    entry_points={
        'console_scripts': [
            'rabbithole=rabbithole.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='rabbithole',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
