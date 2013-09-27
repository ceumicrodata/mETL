
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
from metl.fieldtype.listfieldtype import ListFieldType
from metl.fieldtype.complexfieldtype import ComplexFieldType
from metl.fieldtype.picklefieldtype import PickleFieldType
from metl.exception import *

class Test_FieldType( unittest.TestCase ):

    def test_boolean_field_type( self ):

        ft = BooleanFieldType()

        self.assertTrue( ft.getValue( 1 ) )
        self.assertFalse( ft.getValue( 0 ) )
        self.assertTrue( ft.getValue( '1' ) )
        self.assertFalse( ft.getValue( '0' ) )
        self.assertTrue( ft.getValue( 'tRuE' ) )
        self.assertFalse( ft.getValue( 'fAlSe' ) )
        self.assertTrue( ft.getValue( 't' ) )
        self.assertFalse( ft.getValue( 'F' ) )
        self.assertTrue( ft.getValue( True ) )
        self.assertFalse( ft.getValue( False ) )

    def test_pickle_field_type( self ):

        ft = PickleFieldType()

        self.assertEqual( ft.getValue('monkey'), 'monkey' )
        self.assertEqual( ft.getValue('[{"animal":"monkey","country":"hungary"}]'),'[{"animal":"monkey","country":"hungary"}]')
        self.assertEqual( ft.getValue('{"animal":"monkey","country":"hungary"}'),'{"animal":"monkey","country":"hungary"}')
        self.assertEqual( ft.getValue([1,2,3]), [1,2,3] )
        self.assertEqual( ft.getValue(set([1,2,3,3,3,3])), set([1,2,3]) )
        self.assertEqual( ft.getValue(5), 5 )
        self.assertEqual( ft.getValue({'a':1,'b':2}), {'a':1,'b':2} )

    def test_complex_field_type( self ):

        ft = ComplexFieldType()

        self.assertEqual( ft.getValue('monkey'), 'monkey' )
        self.assertEqual( ft.getValue('[{"animal":"monkey","country":"hungary"}]'),[{'animal':'monkey','country':'hungary'}])
        self.assertEqual( ft.getValue('{"animal":"monkey","country":"hungary"}'),{'animal':'monkey','country':'hungary'})
        self.assertEqual( ft.getValue([1,2,3]), [1,2,3] )
        self.assertEqual( ft.getValue(set([1,2,3,3,3,3])), set([1,2,3]) )
        self.assertEqual( ft.getValue(5), 5 )

    def test_list_field_type( self ):

        ft = ListFieldType()

        self.assertEqual( ft.getValue('monkey'), [ 'monkey' ] )
        self.assertEqual( ft.getValue('[{"animal":"monkey","country":"hungary"}]'),[{'animal':'monkey','country':'hungary'}])
        self.assertEqual( ft.getValue('{"animal":"monkey","country":"hungary"}'),[{'animal':'monkey','country':'hungary'}])
        self.assertEqual( ft.getValue([1,2,3]), [1,2,3] )
        self.assertEqual( ft.getValue(set([1,2,3,3,3,3])), [1,2,3] )

    def test_date_field_type( self ):

        ft = DateFieldType()

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 'invalid' )

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 321312312 )

        with self.assertRaises( FieldTypeError ):
            ft.getValue( '2013-02-31' )

        self.assertEqual( ft.getValue('2013-02-03'), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( ft.getValue('2013/02/03'), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( ft.getValue('2013.02.03'), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( ft.getValue('02/03/2013'), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( ft.getValue('Dec 26, 2012 10:01:26 AM'), datetime.date( 2012, 12, 26 ) )
        self.assertEqual( ft.getValue('2012.12.26 10.01.26'), datetime.date( 2012, 12, 26 ) )
        self.assertEqual( ft.getValue( datetime.date( 2013, 2, 3 ) ), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( ft.getValue( datetime.datetime.now().replace( year = 2013, month = 2, day = 3 ) ), datetime.date( 2013, 2, 3 ) )
        self.assertIsNone( ft.getValue( None ) )

    def test_datetime_field_type( self ):

        ft = DateTimeFieldType()

        current_timestamp = datetime.datetime.now()

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 'invalid' )

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 321312312 )

        with self.assertRaises( FieldTypeError ):
            ft.getValue( '2013-02-31 11:32:33' )

        self.assertEqual( ft.getValue('2013-02-03 11:32:33'), datetime.datetime( 2013, 2, 3, 11, 32, 33 ) )
        self.assertEqual( ft.getValue('2013.02.03 11:32:33'), datetime.datetime( 2013, 2, 3, 11, 32, 33 ) )
        self.assertEqual( ft.getValue('2013/02/03 11:32:33'), datetime.datetime( 2013, 2, 3, 11, 32, 33 ) )
        self.assertEqual( ft.getValue('02/03/2013 11:32:33'), datetime.datetime( 2013, 2, 3, 11, 32, 33 ) )
        self.assertEqual( ft.getValue( current_timestamp ), current_timestamp )
        self.assertEqual( ft.getValue('2013-04-04 16:06:58.929515'), datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) )
        self.assertIsNone( ft.getValue( None ) )

    def test_float_field_type( self ):

        ft = FloatFieldType()

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 'invalid' )

        self.assertEqual( ft.getValue( 4.1232 ), 4.1232 )
        self.assertEqual( ft.getValue('4.1232'), 4.1232 )
        self.assertEqual( ft.getValue('4,1232'), 4.1232 )
        self.assertEqual( ft.getValue('3'), 3.0 )
        self.assertIsNone( ft.getValue( None ) )

    def test_integer_field_type( self ):

        ft = IntegerFieldType()

        with self.assertRaises( FieldTypeError ):
            ft.getValue( 'invalid' )

        self.assertEqual( ft.getValue( 5 ), 5 )
        self.assertEqual( ft.getValue( '5' ), 5 )
        self.assertEqual( ft.getValue( '5.32' ), 5 )
        self.assertEqual( ft.getValue( '5,32' ), 5 )
        self.assertIsNone( ft.getValue( None ) )

    def test_string_field_type( self ):

        ft = StringFieldType()

        self.assertEqual( ft.getValue('first of all'), 'first of all' )
        self.assertEqual( ft.getValue(3), '3' )
        self.assertEqual( ft.getValue(3.32), '3.32' )
        self.assertEqual( ft.getValue( datetime.date.today() ), str( datetime.date.today() ) )
        self.assertIsNone( ft.getValue( '' ) )
        self.assertIsNone( ft.getValue( None ) )

    def test_text_field_type( self ):

        ft = TextFieldType()

        self.assertEqual( ft.getValue('first of all'), 'first of all' )
        self.assertEqual( ft.getValue(3), '3' )
        self.assertEqual( ft.getValue(3.32), '3.32' )
        self.assertEqual( ft.getValue( datetime.date.today() ), str( datetime.date.today() ) )
        self.assertIsNone( ft.getValue( '' ) )
        self.assertIsNone( ft.getValue( None ) )

    def test_field_type_convert_to_boolean( self ):

        fs = BooleanFieldType()

        self.assertTrue( fs.getValue( BooleanFieldType().getValue( True ) ) )
        self.assertTrue( fs.getValue( DateFieldType().getValue( datetime.date.today() ) ) )
        self.assertTrue( fs.getValue( DateTimeFieldType().getValue( datetime.datetime.now() ) ) )
        self.assertTrue( fs.getValue( FloatFieldType().getValue( 4.32112 ) ) )
        self.assertTrue( fs.getValue( IntegerFieldType().getValue( 5 ) ) )
        self.assertTrue( fs.getValue( StringFieldType().getValue('test') ) )
        self.assertTrue( fs.getValue( TextFieldType().getValue('test') ) )
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_date( self ):

        fs = DateFieldType()

        with self.assertRaises( FieldTypeError ):
            fs.getValue( BooleanFieldType().getValue( True ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( FloatFieldType().getValue( 4.32112 ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( IntegerFieldType().getValue( 5 ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( StringFieldType().getValue('test') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( TextFieldType().getValue('test') )

        self.assertEqual( fs.getValue( DateFieldType().getValue( datetime.date.today() ) ), datetime.date.today() )
        self.assertEqual( fs.getValue( DateTimeFieldType().getValue( datetime.datetime.now() ) ), datetime.date.today() )
        self.assertEqual( fs.getValue( StringFieldType().getValue('2013-02-03') ), datetime.date( 2013, 2, 3 ) )
        self.assertEqual( fs.getValue( TextFieldType().getValue('2013-02-03') ), datetime.date( 2013, 2, 3 ) )
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_datetime( self ):

        fs = DateTimeFieldType()

        with self.assertRaises( FieldTypeError ):
            fs.getValue( BooleanFieldType().getValue( True ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( FloatFieldType().getValue( 4.32112 ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( IntegerFieldType().getValue( 5 ) )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( StringFieldType().getValue('test') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( TextFieldType().getValue('test') )

        self.assertEqual( fs.getValue( DateFieldType().getValue('2013-04-04 16:06:58.929515') ), datetime.datetime( 2013, 4, 4 ) )
        self.assertEqual( fs.getValue( DateTimeFieldType().getValue( datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) ) ), datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) )
        self.assertEqual( fs.getValue( StringFieldType().getValue('2013-04-04 16:06:58.929515') ), datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) )
        self.assertEqual( fs.getValue( TextFieldType().getValue('2013-04-04 16:06:58.929515') ), datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) )
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_float( self ):

        fs = FloatFieldType()

        with self.assertRaises( FieldTypeError ):
            fs.getValue( DateFieldType().getValue('2013-02-03') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( DateFieldType().getValue('2013-04-04 16:06:58.929515') ) 

        with self.assertRaises( FieldTypeError ):
            fs.getValue( StringFieldType().getValue('test') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( TextFieldType().getValue('test') )

        self.assertEqual( fs.getValue( BooleanFieldType().getValue( True ) ), 1.0 )
        self.assertEqual( fs.getValue( BooleanFieldType().getValue( False ) ), 0.0 )
        self.assertEqual( fs.getValue( FloatFieldType().getValue( 4.232 ) ), 4.232 )
        self.assertEqual( fs.getValue( IntegerFieldType().getValue( 5 ) ), 5.0 )
        self.assertEqual( fs.getValue( StringFieldType().getValue('5.232') ), 5.232 )
        self.assertEqual( fs.getValue( TextFieldType().getValue('5.232') ), 5.232 )
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_integer( self ):

        fs = IntegerFieldType()

        with self.assertRaises( FieldTypeError ):
            fs.getValue( DateFieldType().getValue('2013-02-03') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( DateFieldType().getValue('2013-04-04 16:06:58.929515') ) 

        with self.assertRaises( FieldTypeError ):
            fs.getValue( StringFieldType().getValue('test') )

        with self.assertRaises( FieldTypeError ):
            fs.getValue( TextFieldType().getValue('test') )

        self.assertEqual( fs.getValue( BooleanFieldType().getValue( True ) ), 1 )
        self.assertEqual( fs.getValue( BooleanFieldType().getValue( False ) ), 0 )
        self.assertEqual( fs.getValue( FloatFieldType().getValue( 4.232 ) ), 4 )
        self.assertEqual( fs.getValue( IntegerFieldType().getValue( 5 ) ), 5 )
        self.assertEqual( fs.getValue( StringFieldType().getValue('5') ), 5 )
        self.assertEqual( fs.getValue( TextFieldType().getValue('5') ), 5 )
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_string( self ):

        fs = StringFieldType()

        self.assertEqual( fs.getValue( BooleanFieldType().getValue( True ) ), 'True' )
        self.assertEqual( fs.getValue( BooleanFieldType().getValue( False ) ), 'False' )
        self.assertEqual( fs.getValue( DateFieldType().getValue( datetime.date( 2013, 2, 3 ) ) ), '2013-02-03' )
        self.assertEqual( fs.getValue( DateTimeFieldType().getValue( datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) ) ), '2013-04-04 16:06:58.929515' )
        self.assertEqual( fs.getValue( FloatFieldType().getValue( 4.232 ) ), '4.232' )
        self.assertEqual( fs.getValue( IntegerFieldType().getValue( 5 ) ), '5' )
        self.assertEqual( fs.getValue( StringFieldType().getValue('text') ), 'text' )
        self.assertEqual( fs.getValue( TextFieldType().getValue('text') ), 'text' )      
        self.assertIsNone( fs.getValue( None ) )

    def test_field_type_convert_to_text( self ):

        fs = TextFieldType()

        self.assertEqual( fs.getValue( BooleanFieldType().getValue( True ) ), 'True' )
        self.assertEqual( fs.getValue( BooleanFieldType().getValue( False ) ), 'False' )
        self.assertEqual( fs.getValue( DateFieldType().getValue( datetime.date( 2013, 2, 3 ) ) ), '2013-02-03' )
        self.assertEqual( fs.getValue( DateTimeFieldType().getValue( datetime.datetime( 2013, 4, 4, 16, 06, 58, 929515 ) ) ), '2013-04-04 16:06:58.929515' )
        self.assertEqual( fs.getValue( FloatFieldType().getValue( 4.232 ) ), '4.232' )
        self.assertEqual( fs.getValue( IntegerFieldType().getValue( 5 ) ), '5' )
        self.assertEqual( fs.getValue( StringFieldType().getValue('text') ), 'text' )
        self.assertEqual( fs.getValue( TextFieldType().getValue('text') ), 'text' )      
        self.assertIsNone( fs.getValue( None ) )

if __name__ == '__main__':
    unittest.main()