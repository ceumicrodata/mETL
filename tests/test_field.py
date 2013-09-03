
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

import datetime, unittest, metl.field
from metl.fieldtype.datetimefieldtype import DateTimeFieldType
from metl.fieldtype.datefieldtype import DateFieldType
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.transform.settransform import SetTransform

class Test_Field( unittest.TestCase ):

    def setUp( self ):

        self.startType    = DateTimeFieldType()
        self.endType      = DateFieldType()
        self.defaultValue = datetime.datetime.now() - datetime.timedelta( days = 5 )
        self.transValue   = self.defaultValue - datetime.timedelta( days = 2 ) 
        self.transforms   = [ SetTransform( value = self.transValue ) ]

        self.field = metl.field.Field(
            'test_name',
            self.startType,
            key = True,
            field_final_type = self.endType,
            defaultValue = self.defaultValue
        )

    def test_name( self ):

        self.assertEqual( self.field.getName(), 'test_name' )
        self.field.setName( 'test_modified_name' )
        self.assertEqual( self.field.getName(), 'test_modified_name' )

    def test_key( self ):

        self.assertTrue( self.field.isKey() )

    def test_key( self ):

        self.assertTrue( self.field.isKey() )

    def test_value( self ):

        self.assertEqual( self.field.getValue(), self.defaultValue )
        current_timestamp = datetime.datetime.now()
        self.field.setValue( str( current_timestamp ) )
        self.assertEqual( current_timestamp, self.field.getValue() )
        self.field.run()
        self.assertEqual( datetime.date( current_timestamp.year, current_timestamp.month, current_timestamp.day ), self.field.getValue() )

    def test_convertable( self ):

        self.assertTrue( self.field.isConvertable( DateFieldType() ) )
        self.assertTrue( self.field.isConvertable( StringFieldType() ) )

    def test_fieldtype( self ):

        self.assertEqual( self.field.getType(), self.startType )
        self.field.run()
        self.assertEqual( self.field.getType(), self.endType )

    def test_limit( self ):

        self.assertEqual( self.field.getLimit(), None )
        self.field.setLimit( 100 )
        self.assertEqual( self.field.getLimit(), 100 )

    def test_fieldtype_value( self ):

        self.assertEqual( self.field.getType(), self.startType )
        self.field.run()
        self.assertEqual( self.field.getType(), self.endType )

    def test_transform( self ):

        self.field.setTransforms( self.transforms )
        self.assertEqual( self.field.getTransforms(), self.transforms )
        self.field.run()
        self.assertEqual( self.field.getValue(), self.field.getType().getValue( self.transValue ) )

    def test_value_modifiers( self ):

        self.assertEqual( self.field.getValue(), self.defaultValue )
        self.assertEqual( self.field.getValue( to_string = True ), str( self.defaultValue ) )
        self.assertEqual( self.field.getValue( class_to_string = True ), str( self.defaultValue ) )

        self.field.setValue( None )

        self.assertIsNone( self.field.getValue() )
        self.assertEqual( self.field.getValue( to_string = True ), '' )
        self.assertIsNone( self.field.getValue( class_to_string = True ) )

if __name__ == '__main__':
    unittest.main()