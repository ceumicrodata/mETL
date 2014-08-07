
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
from metl.manager import Manager
from metl.configparser import ConfigParser
from metl.config import Config

class Test_Single_Source( unittest.TestCase ):

    def test_xml_source( self ):

        self.cfg_file = 'tests/config/test_xml_single_source.yml'

    def test_json_source( self ):

        self.cfg_file = 'tests/config/test_json_single_source.yml'

    def tearDown( self ):

        configparser = ConfigParser( Config( self.cfg_file ) )
        target = configparser.getTarget()
        manager = Manager( target ).run()
        results = target.getResults()

        self.assertEqual( len( results ), 1 )
        self.assertEqual( results[0].getField('formatted_name').getValue(), u'III. kerület, Óbuda-Békásmegyer, Óbudaisziget' )
        self.assertEqual( results[0].getID(), u'1-III. kerület, Óbuda-Békásmegyer, Óbudaisziget:1666851785540340219' )

if __name__ == '__main__':
    unittest.main()
