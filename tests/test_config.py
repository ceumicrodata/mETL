
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

import unittest
from metl.config import Config

class Test_Config( unittest.TestCase ):

    def test_open_yaml_config( self ):

        with self.assertRaises( IOError ):
            Config( 'tests/config/test_not_existing_config.yml' )

        self.assertIsNotNone( Config( 'tests/config/test_config.yml' ) )

    def test_open_yaml_config_with_base( self ):

        cfg = Config( 'tests/config/test_csv_source_del_line.yml' )
        self.assertIsNotNone( cfg )
        self.assertEqual( cfg['source']['delimiter'], '|' )

if __name__ == '__main__':
    unittest.main()