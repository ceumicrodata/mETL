
from setuptools import setup, find_packages
import sys, os

version = '0.1.8.1'

setup(
    name = 'mETL',
    version = version,
    description = "ETL processes with easy config",
    packages = find_packages( exclude = [ 'ez_setup'] ),
    include_package_data = True,
    zip_safe = False,
    entry_points={
        'console_scripts': [
            'metl = metl.script:main',
            'metl-transform = metl.script:metl_transform',
            'metl-walk = metl.script:metl_walk',
            'metl-aggregate = metl.script:metl_aggregate',
            'metl-differences = metl.script:metl_differences',
            'metl-generate = metl.script:metl_generate'
        ],
    },
    author = 'Bence Faludi',
    author_email = 'b.faludi@mito.hu',
    license = 'GPL',
    dependency_links = [
        'https://github.com/ceumicrodata/tarr/archive/tarr-0.1.1.zip'
    ],
    install_requires = [
        'python-dateutil',
        'xlrd',
        'gdata',
        'demjson',
        'pyyaml',
        'sqlalchemy>=0.8',
        'xlwt',
        'nltk',
        'tarr',
        'xlutils',
        'xmlsquash',
        'gspread'
    ],
    test_suite = "tests"
)