
# -*- coding: utf-8 -*-

"""
mETL is a Python tool for do ETL processes with easy config.
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

import re, sqlalchemy, metl.fieldtype.base

from metl.fieldtype.booleanfieldtype import *
from metl.fieldtype.datefieldtype import *
from metl.fieldtype.datetimefieldtype import *
from metl.fieldtype.floatfieldtype import *
from metl.fieldtype.integerfieldtype import *
from metl.fieldtype.stringfieldtype import *
from metl.fieldtype.textfieldtype import *
from metl.fieldtype.listfieldtype import *
from metl.fieldtype.complexfieldtype import *
from metl.fieldtype.bigintegerfieldtype import *

class UnknownFieldType( metl.fieldtype.base.FieldType ):
              
    # bool
    def isConvertable( self, value ):

        return True

    # type
    def getValue( self, value ):
        
        if self.getPreConvertValue( value ):
            return None if self.isNullable() else u''

        return value

    # bool
    def isAcceptableStringType( self, value ):

        v = StringFieldType().getValue( value )
        return len( v or '' ) <= 255

    # bool
    def isAcceptableTextType( self, value ):

        v = TextFieldType().getValue( value )
        return True

    # bool
    def isAcceptableDateType( self, value ):

        v = DateFieldType().getValue( value )
        return True

    # bool
    def isAcceptableDateTimeType( self, value ):

        v = DateTimeFieldType().getValue( value )
        return True

    # bool
    def isAcceptableIntegerType( self, value ):

        iv = IntegerFieldType().getValue( value )
        fv = FloatFieldType().getValue( value )

        return ( ( fv is None ) or ( fv is not None and FloatFieldType().getValue( iv ) == fv ) ) and len(str(iv)) <= 9

    # bool
    def isAcceptableBigIntegerType( self, value ):

        iv = BigIntegerFieldType().getValue( value )
        fv = FloatFieldType().getValue( value )
        
        return ( fv is None ) or ( fv is not None and FloatFieldType().getValue( iv ) == fv )

    # bool
    def isAcceptableFloatType( self, value ):

        v = FloatFieldType().getValue( value )
        return True

    # bool
    def isAcceptableComplexType( self, value ):

        v = ComplexFieldType().getValue( value )
        return type( v ) in ( dict, list )

    # bool
    def isAcceptableListType( self, value ):

        v = ListFieldType().getValue( value )
        return map( unicode, v ) != [ unicode( value ) ]

    # bool
    def isAcceptableBooleanType( self, value ):

        return value is None or ( value is not None and type( value ) == bool )

    # bool
    def isAcceptableType( self, field_type, value ):

        try:
            return getattr( self, 'isAcceptable%sType' % ( field_type ) )( value )
        except:
            return False

    # list<unicode>
    def getAcceptableTypes( self, value ):

        acceptable_types = []

        if self.isAcceptableType( 'String', value ):
            acceptable_types.append( 'String' )

        if self.isAcceptableType( 'Text', value ):
            acceptable_types.append( 'Text' )

        if self.isAcceptableType( 'DateTime', value ):
            acceptable_types.append( 'DateTime' )

        if self.isAcceptableType( 'Date', value ):
            acceptable_types.append( 'Date' )

        if self.isAcceptableType( 'BigInteger', value ):
            acceptable_types.append( 'BigInteger' )

        if self.isAcceptableType( 'Integer', value ):
            acceptable_types.append( 'Integer' )

        if self.isAcceptableType( 'Float', value ):
            acceptable_types.append( 'Float' )

        if self.isAcceptableType( 'Complex', value ):
            acceptable_types.append( 'Complex' )

        if self.isAcceptableType( 'List', value ):
            acceptable_types.append( 'List' )

        if self.isAcceptableType( 'Boolean', value ):
            acceptable_types.append( 'Boolean' )

        return acceptable_types

    # FieldType
    @classmethod
    def offerType( cls, accepted_types ):

        if 'List' in accepted_types:
            return ListFieldType

        if 'Complex' in accepted_types:
            return ComplexFieldType

        if 'Float' in accepted_types and 'Integer' not in accepted_types and 'BigInteger' not in accepted_types:
            return FloatFieldType

        if 'Integer' in accepted_types:
            return IntegerFieldType

        if 'BigInteger' in accepted_types and 'Integer' not in accepted_types:
            return BigIntegerFieldType

        if 'DateTime' in accepted_types:
            return DateTimeFieldType

        if 'Date' in accepted_types:
            return DateFieldType

        if 'Boolean' in accepted_types:
            return BooleanFieldType

        if 'Text' in accepted_types and 'String' not in accepted_types:
            return TextFieldType

        return StringFieldType
