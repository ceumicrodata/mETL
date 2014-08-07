
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

from metl.exception import *
from metl.tarr.compiler import Program, RETURN_TRUE
from metl.tarr.data import Data

class Field( object ):

    # void
    def __init__( self, field_name, field_type, field_final_type = None, key = False, defaultValue = None, transforms = None, limit = None, nullable = True ):

        self.field_name       = field_name
        self.orig_type        = field_type
        self.field_type       = field_type
        self.field_final_type = field_final_type or field_type
        self.limit            = limit
        self.value            = None
        self.transforms       = transforms
        self.key              = key
        self.log              = False
        self.defaultValue     = defaultValue
        self.nullable         = nullable

        self.field_type.setNullable( self.nullable )
        self.field_final_type.setNullable( self.nullable )

        self.setValue( defaultValue )

    def clone( self, final = False ):

        return Field(
            self.getName(),
            self.getOriginalType().clone() if not final else self.getFinalType().clone(),
            self.getFinalType().clone(),
            key = self.isKey(),
            defaultValue = self.getDefaultValue(),
            transforms = self.getTransforms(),
            limit = self.limit,
            nullable = self.nullable
        )

    def getDefaultValue( self ):

        return self.defaultValue

    # void
    def setStdOutput( self ):

        self.log = True

    # boolean
    def isKey( self ):

        return self.key

    def getOriginalType( self ):

        return self.orig_type

    def getFinalType( self ):

        return self.field_final_type or self.orig_type

    # FieldType
    def getType( self ):

        return self.field_type

    def getLimit( self ):

        return self.limit

    # bool
    def isConvertable( self, field_type ):

        return field_type.isConvertable( self.value )

    # type
    def getValue( self, to_string = False, class_to_string = False, without_list = False ):

        if without_list and type( self.value ) == list:
            return unicode( self.value )

        if not to_string and not class_to_string:
            return self.value

        if to_string and self.value is not None:
            return unicode( self.value )

        if self.value is not None and class_to_string and self.value.__class__ in self.getType().getFieldClasses():
            return unicode( self.value )

        elif self.value is not None and class_to_string:
            return self.value

        elif self.value is None and class_to_string:
            return None

        return u''

    # unicode
    def getName( self ):

        return self.field_name

    # void
    def setValue( self, value ):

        self.value = self.field_type.getValue( value )
        if self.log:
            print '=>', repr( self.value )

    # void
    def setType( self, field_type, hard = False ):

        if not hard or self.isConvertable( field_type ):
            try:
                self.field_type = field_type
                self.value      = field_type.getValue( self.value )
                if self.log:
                    print '=>', repr( self.value )
            except:
                raise FieldTypeError('Selected Field type conversion is failed!')

        elif hard:
            self.field_type = field_type
            self.value      = None

    def setLimit( self, limit ):

      self.limit = limit

    # void
    def setName( self, field_name ):

        self.field_name = field_name

    # unicode
    def getPattern( self ):

        p = '(%s)-%d' % ( self.getName(), self.getType().getWidth() )
        return '%' + p + 's'

    # void
    def setTransforms( self, transforms ):

        self.transforms = transforms
        return self

    # list
    def getTransforms( self ):

        return self.transforms

    # void
    def setFinalType( self, field_final_type ):

        self.field_final_type = field_final_type

    # void
    def run( self ):

        if self.getTransforms():
            Program( self.getTransforms() + [ RETURN_TRUE ] ).run(
                Data( self.getName(), self )
            ).payload

        if self.getType() != self.field_final_type:
            self.setType( self.field_final_type, hard = self.getValue() is None )

