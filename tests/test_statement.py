
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
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.statement.elifnotstatement import ELIFNotStatement
from metl.statement.elifstatement import ELIFStatement
from metl.statement.elsestatement import ELSEStatement
from metl.statement.endifstatement import ENDIFStatement
from metl.statement.ifstatement import IFStatement
from metl.statement.ifnotstatement import IFNotStatement
from metl.statement.returnfalsestatement import ReturnFalseStatement
from metl.statement.returntruestatement import ReturnTrueStatement
from metl.transform.settransform import SetTransform
from metl.condition.isequalcondition import IsEqualCondition
from metl.field import Field

class Test_Statement( unittest.TestCase ):

    def setUp( self ):

        self.field = Field( 'name', StringFieldType() )
        self.field.setTransforms([
            IFStatement( IsEqualCondition('if_test') ),
            SetTransform( 'if_test_valid' ),
            ELIFStatement( IsEqualCondition('elif_test') ),
            SetTransform( 'elif_test_valid' ),
            ELSEStatement,
            SetTransform( 'else_test_valid'),
            ENDIFStatement,
            ReturnTrueStatement
        ])

        self.other_field = Field( 'name', StringFieldType() )
        self.other_field.setTransforms([
            IFNotStatement( IsEqualCondition('if_not_test') ),
            IFNotStatement( IsEqualCondition('if_if_not_test') ),
            SetTransform('if_if_not_test_valid'),
            ELIFNotStatement( IsEqualCondition('if_elif_not_test') ),
            SetTransform('if_elif_not_test_valid'),
            ENDIFStatement,
            ENDIFStatement,
            ReturnTrueStatement
        ])

    def test_if( self ):

        self.field.setValue('if_test')
        self.field.run()
        self.assertEqual( self.field.getValue(), 'if_test_valid' )

    def test_elif( self ):

        self.field.setValue('elif_test')
        self.field.run()
        self.assertEqual( self.field.getValue(), 'elif_test_valid' )

    def test_else( self ):

        self.field.setValue('else_test')
        self.field.run()
        self.assertEqual( self.field.getValue(), 'else_test_valid' )

    def test_if_not( self ):

        self.other_field.setValue('if_if_not_test')
        self.other_field.run()
        self.assertEqual( self.other_field.getValue(), 'if_elif_not_test_valid' )

    def test_elif_not( self ):

        self.other_field.setValue('if_elif_not_test')
        self.other_field.run()
        self.assertEqual( self.other_field.getValue(), 'if_if_not_test_valid' )

if __name__ == '__main__':
    unittest.main()