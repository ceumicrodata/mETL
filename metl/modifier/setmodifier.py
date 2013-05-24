
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

import metl.modifier.base, metl.configparser, inspect

class SetModifier( metl.modifier.base.Modifier ):

    init = ['fieldNames','value','fn','source']
    
    # void
    def __init__( self, reader, fieldNames, value = None, fn = None, source = None, *args, **kwargs ):

        self.fields = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.value  = value
        self.source = source
        self.func = None

        if fn is not None:
            if inspect.ismethod( fn ) or inspect.isfunction( fn ):
                self.func = fn

            else:
                self.func = metl.configparser.lookupClass( fn )

        super( SetModifier, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        if self.source is not None:
            self.source.initialize()
            self.sourceRecords = [ r for r in self.source.getRecords() ]

        return super( SetModifier, self ).initialize()

    # void
    def finalize( self ):

        if self.source is not None:
            self.source.finalize()

        return super( SetModifier, self ).finalize()

    # type
    def getValue( self ):

        return self.value

    # list<FieldSet>
    def getSourceRecords( self ):

        return self.sourceRecords

    # FieldSet
    def modify( self, record ):

        for field in self.fields:
            if self.func is not None:
                record.getField( field ).setValue( self.func( record, field, self ) ) 

            elif self.value is not None and type( self.value ) in ( str, unicode ):
                record.getField( field ).setValue( self.value % record.getValues( to_string = True ) )

            else:
                record.getField( field ).setValue( self.value )

        return record
