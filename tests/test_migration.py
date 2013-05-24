
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
from metl.migration import Migration

class Test_Migration( unittest.TestCase ):

    def test_list_type_migration( self ):

        m1 = Migration('new')
        m1.migration_data = set([1,2,3,4,5])
        m1.migration_type = list

        m2 = Migration('old')
        m2.migration_data = set([4,5,6,8])
        m2.migration_type = list

        self.assertEqual( m1.getNews( m2 ), [1,2,3] )
        self.assertEqual( m1.getUpdated( m2 ), [] )
        self.assertEqual( m1.getDeleted( m2 ), [8,6] )
        self.assertEqual( m1.getUnchanged( m2 ), [4,5] )

    def test_list_dict_migration( self ):

        m1 = Migration('new')
        m1.migration_data = {
            '1': '1st',
            '2': '2nd',
            '3': '3rd',
            '4': '4th',
            '5': '5th'
        }
        m1.migration_type = dict

        m2 = Migration('old')
        m2.migration_data = {
            '2': '2th',
            '4': '4th',
            '5': '5th',
            '6': '6th',
            '8': '8th'
        }
        m2.migration_type = dict

        self.assertEqual( m1.getNews( m2 ), ['1','3'] )
        self.assertEqual( m1.getUpdated( m2 ), ['2'] )
        self.assertEqual( m1.getDeleted( m2 ), ['8','6'] )
        self.assertEqual( m1.getUnchanged( m2 ), ['5','4'] )

if __name__ == '__main__':
    unittest.main()