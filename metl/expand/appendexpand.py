
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

import metl.expand.base, metl.fieldmap

class AppendExpand( metl.expand.base.Expand ):

    init = ['resource']

    # void
    def __init__( self, reader, resource, *args, **kwargs ):

        self.resource = resource
        super( AppendExpand, self ).__init__( reader, *args, **kwargs )
        self.source   = self.cloneFirstReader()

    def cloneFirstReader( self ):

        success = False
        current_reader = self.getReader()
        while not success:
            if hasattr( current_reader, 'getReader' ):
                current_reader = current_reader.getReader()
            else:
                success = True

        source = current_reader.__class__( current_reader.getFieldSetPrototypeCopy() )
        source.setResource( self.resource )
        return source

    # Source
    def getSource( self ):

        return self.source

    # void
    def initialize( self ):

        self.source.initialize()
        return super( AppendExpand, self ).initialize()

    # void
    def finalize( self ):

        self.source.finalize()
        return super( AppendExpand, self ).finalize()

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            yield record
        
        for record in self.getSource().getRecords():
            yield record

        