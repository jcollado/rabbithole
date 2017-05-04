#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Project global configuration."""

from setuptools import setup
from setuptools.command.test import test as TestCommand

with open('README.rst') as readme_file:
    README = readme_file.read()

with open('HISTORY.rst') as history_file:
    HISTORY = history_file.read()

REQUIREMENTS = [
    'PyYAML',
    'blinker',
    'pika',
    'sqlalchemy',
    'typing;python_version<"3"',
]

TEST_REQUIREMENTS = [
    'coveralls',
    'mock',
    'pytest',
    'tox',
]


class PyTest(TestCommand):

    """Command to run test cases through pytest."""

    def finalize_options(self):
        """Set custom argumengs fro pytest."""
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        """Execute pytest with custom arguments."""
        # import here, cause outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)

setup(
    name='rabbithole',
    version='0.3.0',
    description="Store messages from an AMQP server into a SQL database",
    long_description=README + '\n\n' + HISTORY,
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
    install_requires=REQUIREMENTS,
    license="MIT license",
    zip_safe=False,
    keywords='rabbithole',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=TEST_REQUIREMENTS,
    cmdclass={'test': PyTest},
    extras_require={
        'postgresql': ['psycopg2'],
    },
)
