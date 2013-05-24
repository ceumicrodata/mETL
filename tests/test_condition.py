
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
from metl.condition.isbetweencondition import IsBetweenCondition
from metl.condition.isemptycondition import IsEmptyCondition
from metl.condition.isequalcondition import IsEqualCondition
from metl.condition.isgreaterandequalcondition import IsGreaterAndEqualCondition
from metl.condition.isgreatercondition import IsGreaterCondition
from metl.condition.islessandequalcondition import IsLessAndEqualCondition
from metl.condition.islesscondition import IsLessCondition
from metl.condition.ismatchbyregexpcondition import IsMatchByRegexpCondition
from metl.condition.isincondition import IsInCondition
from metl.condition.isinsourcecondition import IsInSourceCondition
from metl.fieldtype.booleanfieldtype import BooleanFieldType
from metl.fieldtype.datefieldtype import DateFieldType
from metl.fieldtype.datetimefieldtype import DateTimeFieldType
from metl.fieldtype.floatfieldtype import FloatFieldType
from metl.fieldtype.integerfieldtype import IntegerFieldType
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.fieldtype.textfieldtype import TextFieldType
from metl.fieldset import FieldSet
from metl.field import Field

class Test_Condition( unittest.TestCase ):

    def setUp( self ):

        self.integer_field = Field( 'integer', IntegerFieldType(), defaultValue = 4 )
        self.float_field = Field( 'float', FloatFieldType(), defaultValue = 4.32 ) 
        self.date_field = Field( 'date', DateFieldType(), defaultValue = '2013-02-05' )
        self.string_field = Field( 'string', StringFieldType(), defaultValue = 'test' )
        self.empty_int_field = Field( 'integer_empty', IntegerFieldType() )
        self.empty_str_field = Field( 'string_empty', StringFieldType(), defaultValue = '' )

    def test_is_between_condition( self ):

        self.assertTrue( IsBetweenCondition( 2, '65' ).getResult( self.integer_field ) )
        self.assertTrue( IsBetweenCondition( 2, 4 ).getResult( self.integer_field ) )
        self.assertFalse( IsBetweenCondition( 5, 65 ).getResult( self.integer_field ) )
        self.assertTrue( IsBetweenCondition( 2, 65 ).getResult( self.float_field ) )
        self.assertTrue( IsBetweenCondition( 4.32, 65 ).getResult( self.float_field ) )
        self.assertFalse( IsBetweenCondition( 5, '65' ).getResult( self.float_field ) )
        self.assertTrue( IsBetweenCondition( '2013-02-02', '2013-02-07' ).getResult( self.date_field ) )
        self.assertFalse( IsBetweenCondition( '2013-02-06', '2013-02-07' ).getResult( self.date_field ) )
        self.assertFalse( IsBetweenCondition( 2, 65 ).getResult( self.empty_int_field ) )

    def test_is_empty_condition( self ):

        self.assertFalse( IsEmptyCondition( self.integer_field ) )
        self.assertFalse( IsEmptyCondition( self.float_field ) )
        self.assertFalse( IsEmptyCondition( self.date_field ) )
        self.assertTrue( IsEmptyCondition( self.empty_str_field ) )

    def test_is_equal_condition( self ):

        self.assertTrue( IsEqualCondition( '4' ).getResult( self.integer_field ) )
        self.assertFalse( IsEqualCondition( 5 ).getResult( self.integer_field ) )

        self.assertTrue( IsEqualCondition( 4.32 ).getResult( self.float_field ) )
        self.assertFalse( IsEqualCondition( 4.325 ).getResult( self.float_field ) )

        self.assertTrue( IsEqualCondition( '2013-02-05 11:11' ).getResult( self.date_field ) )
        self.assertFalse( IsEqualCondition( '2013-02-04 11:11' ).getResult( self.date_field ) )

        self.assertTrue( IsEqualCondition( 'test' ).getResult( self.string_field ) )
        self.assertFalse( IsEqualCondition( 'tests' ).getResult( self.string_field ) )

        self.assertTrue( IsEqualCondition( None ).getResult( self.empty_str_field ) )

        self.assertFalse( IsEqualCondition( None ).getResult( self.integer_field ) )

    def test_is_greater_and_equal_condition( self ):

        self.assertFalse( IsGreaterAndEqualCondition( 6 ).getResult( self.integer_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( 4 ).getResult( self.integer_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( 3 ).getResult( self.integer_field ) )

        self.assertFalse( IsGreaterAndEqualCondition( 4.32001 ).getResult( self.float_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( 4.32 ).getResult( self.float_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( 4.31119 ).getResult( self.float_field ) )

        self.assertFalse( IsGreaterAndEqualCondition( '2013-02-06' ).getResult( self.date_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( '2013-02-05' ).getResult( self.date_field ) )
        self.assertTrue( IsGreaterAndEqualCondition( '2013-02-04' ).getResult( self.date_field ) )

        self.assertFalse( IsGreaterAndEqualCondition( 6 ).getResult( self.empty_int_field ) )

    def test_is_greater_condition( self ):

        self.assertFalse( IsGreaterCondition( 6 ).getResult( self.integer_field ) )
        self.assertFalse( IsGreaterCondition( 4 ).getResult( self.integer_field ) )
        self.assertTrue( IsGreaterCondition( 3 ).getResult( self.integer_field ) )

        self.assertFalse( IsGreaterCondition( 4.32001 ).getResult( self.float_field ) )
        self.assertFalse( IsGreaterCondition( 4.32 ).getResult( self.float_field ) )
        self.assertTrue( IsGreaterCondition( 4.31119 ).getResult( self.float_field ) )

        self.assertFalse( IsGreaterCondition( '2013-02-06' ).getResult( self.date_field ) )
        self.assertFalse( IsGreaterCondition( '2013-02-05' ).getResult( self.date_field ) )
        self.assertTrue( IsGreaterCondition( '2013-02-04' ).getResult( self.date_field ) )

        self.assertFalse( IsGreaterCondition( 6 ).getResult( self.empty_int_field ) )

    def test_is_in_condition( self ):

        self.assertFalse( IsInCondition([1,'2',3]).getResult( self.integer_field ) )
        self.assertTrue( IsInCondition([1,'2',3,'4',5]).getResult( self.integer_field ) )

        self.assertFalse( IsInCondition([ 4.2, 4.3, 4.4 ]).getResult( self.float_field ) )
        self.assertTrue( IsInCondition([ 4.31, '4.32', '4,33' ]).getResult( self.float_field ) )

        self.assertFalse( IsInCondition([ '2012-02-04', '2013-02-06' ]).getResult( self.date_field ) )
        self.assertTrue( IsInCondition([ '2013-02-04', '2013-02-05', '2013-02-06' ]).getResult( self.date_field ) )

        self.assertFalse( IsInCondition([1,'2',3]).getResult( self.empty_int_field ) )

    def test_is_in_source_condition( self ):

        false_source_records = [
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 1 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.2 ),
                Field( 'date', DateFieldType(), defaultValue = '2012-02-04' )
            ]),
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 2 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.3 ),
                Field( 'date', DateFieldType(), defaultValue = '2012-02-05' ) 
            ]),
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 3 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.4 ),
                Field( 'date', DateFieldType(), defaultValue = '2012-02-06' ) 
            ])
        ]
        self.assertFalse( IsInSourceCondition( false_source_records, 'integer' ).getResult( self.integer_field ) )
        self.assertFalse( IsInSourceCondition( false_source_records, 'float' ).getResult( self.float_field ) )
        self.assertFalse( IsInSourceCondition( false_source_records, 'date' ).getResult( self.date_field ) )

        true_source_records = [
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 3 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.31 ),
                Field( 'date', DateFieldType(), defaultValue = '2012-02-04' )
            ]),
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 4 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.32 ),
                Field( 'date', DateFieldType(), defaultValue = '2013-02-05' ) 
            ]),
            FieldSet([ 
                Field( 'integer', IntegerFieldType(), defaultValue = 5 ),
                Field( 'float', FloatFieldType(), defaultValue = 4.33 ),
                Field( 'date', DateFieldType(), defaultValue = '2012-02-06' ) 
            ])
        ]
        self.assertTrue( IsInSourceCondition( true_source_records, 'integer' ).getResult( self.integer_field ) )
        self.assertTrue( IsInSourceCondition( true_source_records, 'float' ).getResult( self.float_field ) )
        self.assertTrue( IsInSourceCondition( true_source_records, 'date' ).getResult( self.date_field ) )

    def test_is_less_and_equal_condition( self ):

        self.assertTrue( IsLessAndEqualCondition( 6 ).getResult( self.integer_field ) )
        self.assertTrue( IsLessAndEqualCondition( 4 ).getResult( self.integer_field ) )
        self.assertFalse( IsLessAndEqualCondition( 3 ).getResult( self.integer_field ) )

        self.assertTrue( IsLessAndEqualCondition( 4.32001 ).getResult( self.float_field ) )
        self.assertTrue( IsLessAndEqualCondition( 4.32 ).getResult( self.float_field ) )
        self.assertFalse( IsLessAndEqualCondition( 4.31119 ).getResult( self.float_field ) )

        self.assertTrue( IsLessAndEqualCondition( '2013-02-06' ).getResult( self.date_field ) )
        self.assertTrue( IsLessAndEqualCondition( '2013-02-05' ).getResult( self.date_field ) )
        self.assertFalse( IsLessAndEqualCondition( '2013-02-04' ).getResult( self.date_field ) )

        self.assertFalse( IsLessAndEqualCondition( 6 ).getResult( self.empty_int_field ) )

    def test_is_less_condition( self ):

        self.assertTrue( IsLessCondition( 6 ).getResult( self.integer_field ) )
        self.assertFalse( IsLessCondition( 4 ).getResult( self.integer_field ) )
        self.assertFalse( IsLessCondition( 3 ).getResult( self.integer_field ) )

        self.assertTrue( IsLessCondition( 4.32001 ).getResult( self.float_field ) )
        self.assertFalse( IsLessCondition( 4.32 ).getResult( self.float_field ) )
        self.assertFalse( IsLessCondition( 4.31119 ).getResult( self.float_field ) )

        self.assertTrue( IsLessCondition( '2013-02-06' ).getResult( self.date_field ) )
        self.assertFalse( IsLessCondition( '2013-02-05' ).getResult( self.date_field ) )
        self.assertFalse( IsLessCondition( '2013-02-04' ).getResult( self.date_field ) )

        self.assertFalse( IsLessCondition( 6 ).getResult( self.empty_int_field ) )

    def test_is_match_by_regexp_condition( self ):

        match_field = Field( 'string', StringFieldType(), defaultValue = 'It will match, because it has 1 or more number value.' )
        not_match_field = Field( 'string', StringFieldType(), defaultValue = 'It will match, because it has one or more number value.' )

        self.assertTrue( IsMatchByRegexpCondition( r'^.*(\d+).*$' ).getResult( match_field ) )
        self.assertFalse( IsMatchByRegexpCondition( r'^.*(\d+).*$' ).getResult( not_match_field ) )
        self.assertFalse( IsMatchByRegexpCondition( r'^.*(\d+).*$' ).getResult( self.empty_str_field ) )

if __name__ == '__main__':
    unittest.main()