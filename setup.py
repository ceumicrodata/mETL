
from setuptools import setup, find_packages
import sys, os

version = '0.1.6.11'

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
            'metl-differences = metl.script:metl_differences'
        ],
    },
    author = 'Bence Faludi',
    author_email = 'b.faludi@mito.hu',
    license = 'GPL',
    dependency_links = [
        'https://github.com/ceumicrodata/tarr/archive/tarr-0.1.1.zip',
        'https://github.com/bfaludi/XML2Dict/archive/master.zip#egg=xml2dict-0.1'
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
        'xml2dict'
    ],
    test_suite = "tests"
)