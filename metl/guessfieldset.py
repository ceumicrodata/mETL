
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

import metl.field, metl.fieldset, metl.fieldmap, metl.fieldtype.unknownfieldtype

class GuessFieldSet( object ):

    # void
    def __init__( self ):

        self.rules, self.fields, self.fieldlist = {}, {}, []
        self.setFieldMap()

    # void
    def clone( self, final = False ):

        return GuessFieldSet()

    # void
    def transform( self ):

        pass

    # bool
    def hasField( self, field_name ):

        return field_name in self.fields.keys()

    # void
    def setFields( self, fields, skip_check_exists = False ):

        self.addField( field, skip_check_exists = True )

    # void
    def setFieldMap( self, fieldmap_obj = None ):

        self.fieldmap = metl.fieldmap.FieldMap( self.rules )

    # void
    def setValues( self, values, base = None ):

        if type( values ) == list:
            for v in values:
                self.getField( values.index( v ) ).setValue( v )

        if type( values ) == dict:
            for k,v in values.items():
                self.getField( k ).setValue( v )

    # dict
    def getValues( self, without_none = False, to_string = False, class_to_string = False, without_list = False ):

        ret_dict = {}
        for field_name, field in self.getFields().items():
            if without_none and field.getValue() is None:
                continue

            ret_dict[ field_name ] = field.getValue( to_string, class_to_string, without_list )

        return ret_dict

    # list<unicode>
    def getFieldNames( self ):

        return self.fieldlist

    # list<unicode>
    def getValuesList( self, to_string = False, class_to_string = False ):

        return [ self.getField( field_name, skip_check_exists = True ).getValue( to_string, class_to_string ) for field_name in self.getFieldNames() ]

    # FieldMap
    def getFieldMap( self ):

        return self.fieldmap

    # Field
    def getField( self, field_name, skip_check_exists = False ):

        if not self.hasField( field_name ):
            self.addField( field_name )
            self.rules[ field_name ] = field_name
            self.setFieldMap()

        return self.fields[ field_name ]

    # dict
    def getFields( self ):

        return self.fields

    # unicode
    def getHash( self ):

        return hash( frozenset( self.getValues( without_none = True, without_list = True ).values() ) )

    # unicode
    def getID( self ):

        return '%s:%s' % ( self.getKey(), self.getHash() )

    # void
    def addField( self, field_name, skip_check_exists = False ):

        field_obj = metl.field.Field(
            field_name,
            metl.fieldtype.unknownfieldtype.UnknownFieldType()
        )
        self.fields[ field_obj.getName() ] = field_obj
        self.fieldlist.append( field_obj.getName() )

    # void
    def deleteField( self, field_name ):
        
        del self.fields[ field_name ]
        self.fieldlist.remove( field_name )

    # list
    def getKeyFieldList( self ):

        return []

    # unicode
    def getKey( self ):

        return None
