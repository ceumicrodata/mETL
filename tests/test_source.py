
# -*- coding: utf-8 -*-

"""
mETLapp is a Python tool for do ETL processes with easy config.
Copyright (C) 2013, Bence Faludi (b.faludi@mito.hu)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program. If not, <see http://www.gnu.org/licenses/>.
"""

import unittest, metl.filter.base, hashlib
from metl import tarr
from metl.manager import Manager
from metl.configparser import ConfigParser
from metl.config import Config

class Test_Source( unittest.TestCase ):

    def test_tsv_source( self ):

        self.cfg_file = 'tests/config/test_tsv_source.yml'

    def test_tsv_source_with_header( self ):

        self.cfg_file = 'tests/config/test_tsv_source_with_header.yml'

    def test_csv_source_del_comma( self ):

        self.cfg_file = 'tests/config/test_csv_source_del_comma.yml'

    def test_csv_source_del_comma( self ):

        self.cfg_file = 'tests/config/test_csv_source_del_line.yml'

    def test_csv_source_with_header( self ):

        self.cfg_file = 'tests/config/test_csv_source_with_header.yml'

    def test_xls_source_xls( self ):

        self.cfg_file = 'tests/config/test_xls_source_xls.yml'

    def test_xls_source_xls_multiple_sheets( self ):

        self.cfg_file = 'tests/config/test_xls_source_xls_multiple_sheets.yml'

    def test_xls_source_xlsx( self ):

        self.cfg_file = 'tests/config/test_xls_source_xlsx.yml'

    def test_json_source( self ):

        self.cfg_file = 'tests/config/test_json_source.yml'

    def test_json_source_list( self ):

        self.cfg_file = 'tests/config/test_json_source_list.yml'

    def test_xml_source( self ):

        self.cfg_file = 'tests/config/test_xml_source.yml'

    def test_fwt_source( self ):

        self.cfg_file = 'tests/config/test_fwt_source.yml'

    def test_yaml_source( self ):

        self.cfg_file = 'tests/config/test_yaml_source.yml'

    def test_database_source_via_table( self ):

        self.cfg_file = 'tests/config/test_db_source_via_table.yml'

    def test_database_source_via_statement( self ):

        self.cfg_file = 'tests/config/test_db_source_via_statement.yml'

    def test_google_spreadshet_source( self ):

        self.cfg_file = 'tests/config/test_gs_source.yml'

    def tearDown( self ):

        configparser = ConfigParser( Config( self.cfg_file ) )
        target = configparser.getTarget()
        manager = Manager( target ).run()
        results = target.getResults()

        self.assertEqual( len( results ), 187 )
        self.assertEqual( len( results[0].getFieldNames() ), 10 )
        self.assertEqual( results[0].getField('formatted_name').getValue(), u'III. kerület, Óbuda-Békásmegyer, Óbudaisziget' )
        self.assertEqual( results[-1].getField('formatted_name').getValue(), u'XI. kerület, Újbuda, Lágymányos' )
        self.assertEqual( results[1].getID(), u'2-XIII. kerület, Angyalföld-Újlipótváros, Margitsziget:169062457054928006' )
        self.assertEqual( results[-2].getID(), u'212-XI. kerület, Újbuda, Infopark:-2687868729458697361' )
        self.assertEqual( hashlib.md5( ''.join([ str(r.getHash()) for r in results ]) ).hexdigest(), '9d639582807ea57625405cd611a3a581' )

@tarr.rule
def convertToRomanNumber( field ):

    if field.getValue() is None:
        return None

    number = int( field.getValue() )
    ints = (1000, 900, 500, 400, 100,  90, 50, 40, 10, 9, 5, 4, 1)
    nums = ('M', 'CM', 'D', 'CD', 'C', 'XC', 'L', 'XL', 'X', 'IX', 'V', 'IV', 'I')

    result = ""
    for i in range( len( ints ) ):
        count   = int( number / ints[i] )
        result += nums[i] * count
        number -= ints[i] * count

    field.setValue( '%s.' % ( result ) )
    return field

class DropIfSameNameAndDistrict( metl.filter.base.Filter ):

    def isFiltered( self, record ):

        if record.getField('district_search').getValue() == record.getField('name_search').getValue():
            return True

        return False

if __name__ == '__main__':
    unittest.main()
