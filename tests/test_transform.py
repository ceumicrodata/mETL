
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
from metl.transform.cleantransform import CleanTransform
from metl.transform.converttypetransform import ConvertTypeTransform
from metl.transform.homogenizetransform import HomogenizeTransform
from metl.transform.lowercasetransform import LowerCaseTransform
from metl.transform.uppercasetransform import UpperCaseTransform
from metl.transform.titletransform import TitleTransform
from metl.transform.striptransform import StripTransform
from metl.transform.maptransform import MapTransform
from metl.transform.stemtransform import StemTransform
from metl.transform.settransform import SetTransform
from metl.transform.replacebyregexptransform import ReplaceByRegexpTransform
from metl.transform.replacewordsbysourcetransform import ReplaceWordsBySourceTransform
from metl.transform.removewordsbysourcetransform import RemoveWordsBySourceTransform
from metl.transform.splittransform import SplitTransform
from metl.field import Field
from metl.fieldset import FieldSet
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.fieldtype.datefieldtype import DateFieldType
from metl.exception import *

class Test_Transform( unittest.TestCase ):

    def setUp( self ):

        self.field = Field( 'string', StringFieldType() )
 
    def test_clean_transform( self ):

        self.field.setValue('  That is a good sentence, which is contains many english word! ')
        self.field.setTransforms([ CleanTransform( replaces = { 'many': '1+' } ) ]).run()
        self.assertEqual( self.field.getValue(), 'That is a good sentence which is contains 1+ english word' )

    def test_converttype_transform_soft( self ):

        self.field.setValue('2012-02-12')
        self.field.setFinalType( DateFieldType() )
        self.field.setTransforms([ ConvertTypeTransform( 'Date' ) ]).run()
        self.assertEqual( self.field.getValue(), datetime.date( 2012, 2, 12 ) )

    def test_converttype_transform_soft_invalid( self ):

        self.field.setValue('four miles')
        self.field.setFinalType( DateFieldType() )

        with self.assertRaises( FieldTypeError ):
            self.field.setTransforms([ ConvertTypeTransform( 'Date' ) ]).run()

    def test_converttype_transform_hard( self ):

        self.field.setValue('four miles')
        self.field.setFinalType( DateFieldType() )
        self.field.setTransforms([ ConvertTypeTransform( 'Date', hard = True ) ]).run()
        self.assertIsNone( self.field.getValue() )

    def test_converttype_transform_hard_with_default( self ):

        self.field.setValue('four miles')
        self.field.setFinalType( DateFieldType() )
        self.field.setTransforms([ ConvertTypeTransform( 'Date', hard = True, defaultValue = datetime.date.today() ) ]).run()
        self.assertEqual( self.field.getValue(), datetime.date.today() )

    def test_homogenize_transform( self ):

        self.field.setValue(u'árvíztűrőtükörfúrógépÁRVÍZTŰRŐTÜKÖRFÚRÓGÉP')
        self.field.setTransforms([ HomogenizeTransform ]).run()
        self.assertEqual( self.field.getValue(), 'arvizturotukorfurogeparvizturotukorfurogep' )

    def test_lowercase_transform( self ):

        self.field.setValue(u'That is a good sentence, which is contains many english word!')
        self.field.setTransforms([ LowerCaseTransform ]).run()
        self.assertEqual( self.field.getValue(), 'that is a good sentence, which is contains many english word!' )

    def test_uppercase_transform( self ):

        self.field.setValue(u'That is a good sentence, which is contains many english word!')
        self.field.setTransforms([ UpperCaseTransform ]).run()
        self.assertEqual( self.field.getValue(), 'THAT IS A GOOD SENTENCE, WHICH IS CONTAINS MANY ENGLISH WORD!' )

    def test_strip_transform( self ):

        self.field.setValue(u'  That is a good sentence, which is contains many english word!   ')
        self.field.setTransforms([ StripTransform() ]).run()
        self.assertEqual( self.field.getValue(), 'That is a good sentence, which is contains many english word!' )

    def test_title_transform( self ):

        self.field.setValue(u'That is a good sentence, which is contains many english word!')
        self.field.setTransforms([ TitleTransform ]).run()
        self.assertEqual( self.field.getValue(), 'That Is A Good Sentence, Which Is Contains Many English Word!' )

    def test_stem_transform( self ):

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ StemTransform( 'English' ) ]).run()
        self.assertEqual( self.field.getValue(), 'contain hungarian member attract sad kill' )

    def test_set_transform( self ):

        self.field.setTransforms([ SetTransform( 'That will be the final value' ) ]).run()
        self.assertEqual( self.field.getValue(), 'That will be the final value' )

    def test_set_transform_srintfs( self ):

        self.field.setValue('Myself')
        self.field.setTransforms([ SetTransform( '%(self)s or the new string' ) ]).run()
        self.assertEqual( self.field.getValue(), 'Myself or the new string' )

    def test_map_transform( self ):

        self.field.setValue(u'Monkey')
        self.field.setTransforms([ MapTransform({ 'monkey': 'animal' }) ]).run()
        self.assertEqual( self.field.getValue(), 'Monkey' )

    def test_map_transform_ignore_case( self ):

        self.field.setValue(u'Monkey')
        self.field.setTransforms([ MapTransform({ 'monkey': 'animal' }, ignorecase = True ) ]).run()
        self.assertEqual( self.field.getValue(), 'animal' )

    def test_map_transform_not_used_else_value( self ):

        self.field.setValue(u'monkey')
        self.field.setTransforms([ MapTransform({ 'monkey': 'animal' }, elseValue = 'other' ) ]).run()
        self.assertEqual( self.field.getValue(), 'animal' )

    def test_map_transform_used_else_value( self ):

        self.field.setValue(u'Building')
        self.field.setTransforms([ MapTransform({ 'monkey': 'animal' }, elseValue = 'other' ) ]).run()
        self.assertEqual( self.field.getValue(), 'other' )

    def test_map_transform_used_else_clear( self ):

        self.field.setValue(u'Building')
        self.field.setTransforms([ MapTransform({ 'monkey': 'animal' }, elseClear = True ) ]).run()
        self.assertIsNone( self.field.getValue() )

    def test_replace_by_regexp( self ):

        self.field.setValue(u'2012-04-20')
        self.field.setTransforms([ ReplaceByRegexpTransform( regexp = '^([0-9]{4}-[0-9]{2})-[0-9]{2}$', to = '$1' ) ]).run()
        self.assertEqual( self.field.getValue(), '2012-04' )

    def test_remove_words_by_source( self ):

        sourceRecords = [
            FieldSet([ Field( 'name', StringFieldType(), defaultValue = 'killing' ) ]),
            FieldSet([ Field( 'name', StringFieldType(), defaultValue = 'sadness' ) ]),
            FieldSet([ Field( 'name', StringFieldType(), defaultValue = 'hungarian' ) ])
        ]

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ RemoveWordsBySourceTransform( sourceRecords ) ]).run()
        self.assertEqual( self.field.getValue(), 'contains members attractive' )

    def test_replace_words_by_source( self ):

        sourceRecords = [
            FieldSet([ 
                Field( 'from', StringFieldType(), defaultValue = 'killing' ),
                Field( 'to', StringFieldType(), defaultValue = 'born' ) 
            ]),
            FieldSet([ 
                Field( 'from', StringFieldType(), defaultValue = 'sadness' ),
                Field( 'to', StringFieldType(), defaultValue = 'happyness' ) 
            ]),
            FieldSet([ 
                Field( 'from', StringFieldType(), defaultValue = 'hungarian' ),
                Field( 'to', StringFieldType(), defaultValue = 'anywhere else' ) 
            ])
        ]

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ ReplaceWordsBySourceTransform( sourceRecords, join = 'from' ) ]).run()
        self.assertEqual( self.field.getValue(), 'contains anywhere else members attractive happyness born' )

    def test_split_transform_first( self ):

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ SplitTransform( '0' ) ]).run()
        self.assertEqual( self.field.getValue(), 'contains' )

    def test_split_transform_last( self ):

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ SplitTransform( '-1' ) ]).run()
        self.assertEqual( self.field.getValue(), 'killing' )

    def test_split_transform_interval( self ):

        self.field.setValue(u'contains hungarian members attractive sadness killing')
        self.field.setTransforms([ SplitTransform( '1:-1' ) ]).run()
        self.assertEqual( self.field.getValue(), 'hungarian members attractive sadness' )

if __name__ == '__main__':
    unittest.main()