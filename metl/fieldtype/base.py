
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

class FieldType( object ):
    
    field_types, field_classes = [], []
    width = None
    nullable = True

    def clone( self ):

        return self.__class__()

    def setNullable( self, nullable = True ):

        self.nullable = nullable

    # void
    def setWidth( self, width = None ):

        self.width = width

    # str
    def getName( self ):

        return self.__class__.__name__

    # list<type>
    def getFieldTypes( self ):

        return self.field_types

    # list<object>
    def getFieldClasses( self ):

        return self.field_classes

    # int
    def getWidth( self ):

        return self.width

    # bool
    def isNullable( self ):

        return self.nullable

    # bool
    def isCorrect( self, value ):
        
        for t in self.getFieldTypes():
            if type( value ) == t:
                return True
                
        for c in self.getFieldClasses():
            if value.__class__ == c:
                return True
                
        return False
    
    # bool
    def isConvertable( self, value ):

        try:
            self.getConvertedValue( value )
            return True

        except:
            return False

    # type
    def getPreConvertValue( self, value ):

        return ( value is None or len( unicode( value ).strip() ) == 0 )

    # type
    def getConvertedValue( self, value ):

        return self.getFieldTypes()[0]( value )

    # type
    def getValue( self, value ):
        
        if self.getPreConvertValue( value ):
            return None if self.isNullable() else u''

        if self.isCorrect( value ):
            return value
        
        try:
            return self.getConvertedValue( value )
        except:
            raise FieldTypeError( 'Not valid FieldType value! type: %s, value: %s' % ( self.__class__.__name__, unicode( value ) ) )
