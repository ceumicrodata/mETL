
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

import metl.field, metl.fieldmap, copy
from metl.exception import *

class FieldSet( object ):

    # void
    def __init__( self, fields = None, fieldmap = None, skip_check_exists = False ):

        self.fields, self.fieldmap, self.fieldlist = {}, fieldmap, []
        self.setFields( fields or [], skip_check_exists = skip_check_exists )

    def clone( self, final = False ):

        return FieldSet(
            [ self.getField( field_name ).clone( final ) for field_name in self.getFieldNames() ],
            self.fieldmap,
            skip_check_exists = True
        )

    # void
    def transform( self ):

        for field in self.getFields().values():
            field.run()

    # bool
    def hasField( self, field_name ):

        return field_name in self.fields.keys()

    # void
    def setFields( self, fields, skip_check_exists = False ):

        for field in fields:
            self.addField( field, skip_check_exists = skip_check_exists )

    # void
    def setFieldMap( self, fieldmap_obj ):

        if not isinstance( fieldmap_obj, metl.fieldmap.FieldMap ):
            raise TypeError( 'FieldMap must be FieldMap object' )

        self.fieldmap = fieldmap_obj

    # void
    def setValues( self, values, base = None ):

        for field_name, value in self.getFieldMap().getValues( values, base ).items():
            try:
                self.getField( field_name, skip_check_exists = True ).setValue( value )
            except:
                pass

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

    # list
    def getValuesList( self, to_string = False, class_to_string = False ):

        return [ self.getField( field_name, skip_check_exists = True ).getValue( to_string, class_to_string ) for field_name in self.getFieldNames() ]

    # FieldMap
    def getFieldMap( self ):

        if self.fieldmap is not None:
            return self.fieldmap

        self.fieldmap = metl.fieldmap.FieldMap( dict( zip( self.getFieldNames(), self.getFieldNames() ) ) )
        return self.fieldmap

    # Field
    def getField( self, field_name, skip_check_exists = False ):

        if not skip_check_exists and not self.hasField( field_name ):
            raise FieldNotExistsError( 'Field %s does not exist' % ( field_name ) )

        return self.fields[ field_name ]

    # list<Field>
    def getFields( self ):

        return self.fields

    # int
    def getHash( self ):

        return hash( frozenset( self.getValues( without_none = True, without_list = True ).values() ) )

    # str
    def getID( self ):

        return '%s:%s' % ( self.getKey(), self.getHash() )

    # void
    def addField( self, field_obj, skip_check_exists = False ):

        if not skip_check_exists and not isinstance( field_obj, metl.field.Field ):
            raise TypeError( 'Field type must be Field' )

        if not skip_check_exists and field_obj.getName() in self.fields.keys():
            raise FieldAlreadyExistsError( 'Field already exists with name: %s' % ( field_obj.getName() ) )

        self.fields[ field_obj.getName() ] = field_obj
        self.fieldlist.append( field_obj.getName() )

    # void
    def deleteField( self, field_name ):
        
        if not self.hasField( field_name ):
            raise FieldNotExistsError( 'Field %s does not exist' % ( field_name ) )

        del self.fields[ field_name ]
        self.fieldlist.remove( field_name )

    # list<unicode>
    def getKeyFieldList( self ):

        return [ field_name for field_name in self.getFieldNames() if self.getField( field_name, skip_check_exists = True ).isKey() ]

    # unicode
    def getKey( self ):

        keys = self.getKeyFieldList()
        if len( keys ) == 0:
            return None

        return self.getKeyForFields( keys )

    # unicode
    def getKeyForFields( self, keys ):

        values = self.getValues( to_string = True )
        return u'-'.join([ values[field_name] for field_name in keys ])
