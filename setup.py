
from setuptools import setup, find_packages
import sys, os

version = '1.0.2'

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
            'metl-generate = metl.script:metl_generate',
            'metl-transfer = metl.script:metl_transfer'
        ],
    },
    author = 'Bence Faludi',
    author_email = 'b.faludi@mito.hu',
    license = 'GPL',
    install_requires = [
        'python-dateutil',
        'xlrd',
        'gdata',
        'demjson',
        'pyyaml',
        'sqlalchemy>=0.8',
        'xlwt',
        'nltk',
        'xlutils',
        'xmlsquash',
        'gspread',
        'py2neo'
    ],
    test_suite = "tests"
)
