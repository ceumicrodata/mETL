
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

class JoinByKeyModifier( metl.modifier.base.Modifier ):

    init = ['source','fieldNames']
    
    # void
    def __init__( self, reader, fieldNames, source, *args, **kwargs ):

        self.source        = source
        self.fields        = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.keys          = source.getFieldSetPrototypeCopy().getKeyFieldList()
        if len( self.keys ) == 0:
            raise AttributeError('JoinByKeyModifier: Inner source key fields is not exists!')

        self.sourceRecords = {}

        super( JoinByKeyModifier, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        self.source.initialize()
        for fs in self.source.getRecords():
            self.sourceRecords[ fs.getKey() ] = fs

        return super( JoinByKeyModifier, self ).initialize()

    # void
    def finalize( self ):

        self.source.finalize()
        return super( JoinByKeyModifier, self ).finalize()

    # list<unicode>
    def getKeys( self ):

        return self.keys

    # FieldSet
    def join( self, record ):

        return self.sourceRecords.get( record.getKeyForFields( self.getKeys() ) )

    # FieldSet
    def modify( self, record ):

        fs = self.join( record )
        if fs is None:
            return record

        for field in self.fields:
            record.getField( field ).setValue( fs.getField( field ).getValue() ) 

        return record
