
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

import unittest, datetime
from metl.fieldtype.booleanfieldtype import BooleanFieldType
from metl.fieldtype.datefieldtype import DateFieldType
from metl.fieldtype.datetimefieldtype import DateTimeFieldType
from metl.fieldtype.floatfieldtype import FloatFieldType
from metl.fieldtype.integerfieldtype import IntegerFieldType
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.fieldtype.textfieldtype import TextFieldType
from metl.transform.titletransform import TitleTransform
from metl.transform.lowercasetransform import LowerCaseTransform
from metl.fieldset import FieldSet
from metl.fieldmap import FieldMap
from metl.field import Field
from metl.exception import *

class Test_FieldSet( unittest.TestCase ):

    def setUp( self ):

        self.fs = FieldSet([
            Field( 'id', IntegerFieldType(), defaultValue = 1, key = True ),
            Field( 'name', StringFieldType(), defaultValue = 'John doe' ),
            Field( 'email', StringFieldType(), key = True ),
            Field( 'birth_date', DateFieldType(), field_final_type = StringFieldType() ),
            Field( 'favourite_number', FloatFieldType() ),
            Field( 'created', DateTimeFieldType(), defaultValue = '2013-04-04 18:44:32' ),
            Field( 'updated', DateTimeFieldType(), defaultValue = '2013-04-04 18:44:32' )
        ])

        self.fm = FieldMap({
            'id': 0,
            'name': 1,
            'email': 2,
            'birth_date': 3,
            'favourite_number': 4,
            'created': 5,
            'updated': 6,
            'not_existing_field_name': 6
        })

    def test_field_exists( self ):

        with self.assertRaises( FieldNotExistsError ):
            self.fs.getField( 'key' )

        self.assertTrue( self.fs.hasField( 'id' ) )
        self.assertFalse( self.fs.hasField( 'key' ) )
        self.assertIsNotNone( self.fs.getField( 'id' ) )
        self.assertEqual( len( self.fs.getFields() ), 7 )

    def test_field_names( self ):

        self.assertEqual( self.fs.getFieldNames(), ['id','name','email','birth_date','favourite_number','created','updated'] )

    def test_field_manipulation( self ):

        with self.assertRaises( FieldNotExistsError ):
            self.fs.deleteField( 'key' )

        with self.assertRaises( TypeError ):
            self.fs.addField( 'key' )

        self.assertEqual( len( self.fs.getFields() ), 7 )
        self.fs.deleteField( 'birth_date' )
        self.assertEqual( len( self.fs.getFields() ), 6 )
        self.fs.addField( Field( 'language', StringFieldType() ) )
        self.assertEqual( len( self.fs.getFields() ), 7 )

    def test_field_key_and_id( self ):

        self.assertEqual( self.fs.getKeyFieldList(), ['id','email'] )

        self.assertEqual( self.fs.getKey(), '1-' )
        self.assertEqual( self.fs.getHash(), -1351344983494022270 )
        self.assertEqual( self.fs.getID(), '1-:-1351344983494022270' )
        self.fs.getField('email').setValue('b.faludi@mito.hu')
        self.assertEqual( self.fs.getKey(), '1-b.faludi@mito.hu' )
        self.assertEqual( self.fs.getHash(), -2046033589228461811 )
        self.assertEqual( self.fs.getID(), '1-b.faludi@mito.hu:-2046033589228461811' )

    def test_fieldmap( self ):

        with self.assertRaises( TypeError ):
            self.fs.setFieldMap( 'fieldmap' )

        self.assertIsNotNone( self.fs.getFieldMap() )
        self.assertEqual( self.fs.getFieldMap().getRules(), { 
            'id': 'id', 
            'name': 'name',
            'email': 'email',
            'birth_date': 'birth_date',
            'favourite_number': 'favourite_number',
            'created': 'created',
            'updated': 'updated'
        } )
        self.fs.setFieldMap( self.fm )
        self.assertEqual( self.fs.getFieldMap().getRules(), self.fm.getRules() )

    def test_values( self ):

        self.assertEqual( self.fs.getValues(), {
            'id': 1,
            'name': 'John doe',
            'email': None,
            'favourite_number': None,
            'birth_date': None,
            'created': datetime.datetime( 2013, 4, 4, 18, 44, 32 ),
            'updated': datetime.datetime( 2013, 4, 4, 18, 44, 32 )
        } )

        self.assertEqual( self.fs.getValuesList(), [ 1, 'John doe', None, None, None, datetime.datetime( 2013, 4, 4, 18, 44, 32 ), datetime.datetime( 2013, 4, 4, 18, 44, 32 ) ] )

    def test_values_without_none( self ):

        self.assertEqual( self.fs.getValues( without_none = True ), {
            'id': 1,
            'name': 'John doe',
            'created': datetime.datetime( 2013, 4, 4, 18, 44, 32 ),
            'updated': datetime.datetime( 2013, 4, 4, 18, 44, 32 )
        } )

    def test_values_class_to_string( self ):

        self.assertEqual( self.fs.getValues( class_to_string = True ), {
            'id': 1,
            'name': 'John doe',
            'email': None,
            'favourite_number': None,
            'birth_date': None,
            'created': '2013-04-04 18:44:32',
            'updated': '2013-04-04 18:44:32'
        } )

        self.assertEqual( self.fs.getValuesList( class_to_string = True ), [ 1, 'John doe', None, None, None, '2013-04-04 18:44:32', '2013-04-04 18:44:32' ] )

    def test_values_to_string( self ):

        self.assertEqual( self.fs.getValues( to_string = True ), {
            'id': '1',
            'name': 'John doe',
            'email': '',
            'favourite_number': '',
            'birth_date': '',
            'created': '2013-04-04 18:44:32',
            'updated': '2013-04-04 18:44:32'
        } )

        self.assertEqual( self.fs.getValuesList( to_string = True ), [ '1', 'John doe', '', '', '', '2013-04-04 18:44:32', '2013-04-04 18:44:32' ] )

    def test_value_setter( self ):

        self.fs.setFieldMap( self.fm )

        self.fs.setValues([ 2, 'Foo Bar', 'foo@bar.com', '1976-05-12', 3.22 ])
        self.assertEqual( self.fs.getValuesList(), [ 2, 'Foo Bar', 'foo@bar.com', datetime.date( 1976, 5, 12 ), 3.22, None, None ] )

        self.fs.setValues([ 2, 'Foo Bar', 'foo@bar.com', '1976-05-12', 3.22, None, None, 'test data', 'more test data' ])
        self.assertEqual( self.fs.getValuesList(), [ 2, 'Foo Bar', 'foo@bar.com', datetime.date( 1976, 5, 12 ), 3.22, None, None ] )

    def test_transform( self ):

        self.fs.setFieldMap( self.fm )

        self.fs.getField('name').setTransforms([ TitleTransform ])
        self.fs.getField('email').setTransforms([ LowerCaseTransform ])

        self.fs.setValues([ 2, 'John doe', 'johnDoe@mETL.com', '1976-05-12', 3.22 ])
        self.assertEqual( self.fs.getField('birth_date').getValue(), datetime.date( 1976, 5, 12 ) )
        self.fs.transform()

        self.assertEqual( self.fs.getField('name').getValue(), 'John Doe' )
        self.assertEqual( self.fs.getField('email').getValue(), 'johndoe@metl.com' )
        self.assertEqual( self.fs.getField('birth_date').getValue(), '1976-05-12' )

if __name__ == '__main__':
    unittest.main()