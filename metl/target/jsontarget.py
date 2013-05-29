
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

import metl.target.base, demjson

class JSONTarget( metl.target.base.FileTarget ):

    init = ['rootIterator','flat','compact']

    def __init__( self, reader, rootIterator = None, flat = False, compact = True, *args, **kwargs ):
        
        self.rootIterator = rootIterator
        self.flat         = flat
        self.compact      = compact
        self.records      = None

        super( JSONTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        self.flatMode = self.getFieldSetPrototypeCopy().getFieldNames()[0] if self.flat and len( self.getFieldSetPrototypeCopy().getFieldNames() ) == 1 else None

        self.records = []
        return super( JSONTarget, self ).initialize()

    # void
    def finalize( self ):

        base = self.records if self.rootIterator is None else { self.rootIterator: self.records }
        if self.compact:
            self.file_pointer.write( 
                demjson.encode( base ) 
            )

        else:
            self.file_pointer.write(
                demjson.JSON( compactly = False ).encode( base )
            )

        return super( JSONTarget, self ).finalize()

    # void
    def writeRecord( self, record ):

        if self.flatMode is None:
            self.records.append( record.getValues( class_to_string = True ) )

        else:
            self.records.append( record.getField( self.flatMode ).getValue() )