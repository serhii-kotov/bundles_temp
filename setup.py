"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the temp_kotov_python project.
"""
from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datetime
import temp_kotov_python

setup(
    name="temp_kotov_python",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=temp_kotov_python.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    url="https://databricks.com",
    author="serhii.kotov@stuzo.com",
    description="wheel file based on temp_kotov_python/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=temp_kotov_python.main:main"
        ]
    },
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools"
    ],
)