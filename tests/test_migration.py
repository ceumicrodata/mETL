
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
from metl.utils import FieldSet, Field, IntegerFieldType, StringFieldType

class Test_Migration( unittest.TestCase ):

    def test_list_type_status( self ):

        m1 = Migration('old')
        m1.migration_data = set([
            539441420818803538, # 4
            2487733185322457123, # 5
            4436034508626774120, # 6
            6383806974187745185, # 7
            5476236365203126734 # 8
        ])
        m1.migration_type = list

        self.assertEqual(
            m1.getRecordStatus( FieldSet([ Field( 'record', IntegerFieldType(), defaultValue = 2 ) ]) ),
            { 'exists': False }
        )

        self.assertEqual(
            m1.getRecordStatus( FieldSet([ Field( 'record', IntegerFieldType(), defaultValue = 4 ) ]) ),
            { 'exists': True }
        )

    def test_list_dict_status( self ):

        m1 = Migration('old')
        m1.migration_data = {
            '1': 8023953037754625229, # 1th
            '2': -4284787060753282099, # 2th
            '3': -8490596773977039351, # 3th
            '4': -4533584156133735455, # 4th
            '5': 4196589367963501245 # 5th
        }
        m1.migration_type = dict

        self.assertEqual(
            m1.getRecordStatus( FieldSet([ 
                Field( 'id', IntegerFieldType(), defaultValue = 8, key = True ), 
                Field( 'record', StringFieldType(), defaultValue = 'any value' )
            ]) ),
            { 'exists': False }
        )

        self.assertEqual(
            m1.getRecordStatus( FieldSet([ 
                Field( 'id', IntegerFieldType(), defaultValue = 4, key = True ), 
                Field( 'record', StringFieldType(), defaultValue = '4th' )
            ]) ),
            { 'exists': True, 'modified': False }
        )

        self.assertEqual(
            m1.getRecordStatus( FieldSet([ 
                Field( 'id', IntegerFieldType(), defaultValue = 4, key = True ), 
                Field( 'record', StringFieldType(), defaultValue = 'other value' )
            ]) ),
            { 'exists': True, 'modified': True }
        )

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