
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

import metl.guessfieldset, metl.fieldtype.unknownfieldtype

class Guess( object ):

    source = None

    def __init__( self, source_cls, init, resource_init, limit, base = None ):

        self.source_cls    = source_cls
        self.init          = init
        self.resource_init = resource_init
        self.limit         = limit
        self.base          = base

        self.fieldmap = None
        self.fields   = None

    def guess( self ):

        try:
            self.guessOpen()
        except Exception, e:
            raise RuntimeError('Open the current Source is failed! Error ~ %s' % e ) 

        self.determineMap()
        self.determineFields()
        self.determineFieldTypes()

        if self.fields is not None:
            self.fields = [ v for k, v in self.fields.items() ]

        return self

    def getFilePath( self ):

        return self.filepath

    def getSourceClass( self ):

        return self.source_cls

    def guessOpen( self ):

        self.source = self.getSourceClass()( metl.guessfieldset.GuessFieldSet(), **self.init )
        self.source.setResource( **self.resource_init )
        self.source.setLimitNumber( self.limit )
        self.source.initialize()
        self.records = [ r for r in self.source.getRecords() ]
        self.source.finalize()

    def determineMap( self ):

        self.fieldmap = None
        for record in self.records:
            if self.fieldmap is None:
                self.fieldmap = record.getFieldMap().getRules()

            if record.getFieldMap().getRules() != self.fieldmap:
                self.fieldmap.update( record.getFieldMap().getRules() )

        return self

    def determineFields( self ):

        self.fields = None
        for record in self.records:
            current = {}
            for name, field in record.getFields().items(): 
                current[ name ] = {
                    'name': field.getName(),
                    'type': field.getType().getName()[:-9],
                    'map': self.fieldmap.get( field.getName() )
                }

            if self.fields is None:
                self.fields = current

            if self.fields != current:
                self.fields.update( current )

        return self

    def determineFieldTypes( self ):

        if self.fields is None:
            return
        
        for field_name, cfg in self.fields.items():
            acceptable_types = None

            for field in ( record.getField( field_name ) for record in self.records ):
                types = set( field.getType().getAcceptableTypes( field.getValue() ) )
                
                if acceptable_types is None:
                    acceptable_types = types
                else:
                    acceptable_types = acceptable_types.intersection( types )

            self.fields[ field_name ]['type'] = metl.fieldtype.unknownfieldtype.UnknownFieldType.offerType( acceptable_types )().getName()[:-9]

    def getFieldMap( self ):

        return self.fieldmap

    def getFields( self ):

        return self.fields

    def getConfig( self ):

        source = {
            'source': self.source_cls.__name__[:-6],
            'fields': self.getFields()
        }
        source.update( self.init )
        source.update( self.resource_init )

        for k, v in self.base.get('source',{}).items():
            if k in source.keys() and source[k] == v:
                del source[k]

        d_ret = {
            'source': source,
            'target': {
                'type': 'Static',
                'silence': False
            }
        }

        if 'base' in self.base:
            d_ret['base'] = self.base['base']

        if 'target' in self.base and len( self.base['target'].keys() ):
            del d_ret['target']

        return d_ret

