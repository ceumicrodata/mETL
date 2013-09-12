
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

import metl.expand.base, metl.fieldmap, demjson

class AppendExpand( metl.expand.base.Expand ):

    use_args = True

    # void
    def __init__( self, reader, skipIfFails = False, *args, **kwargs ):

        self.resource = kwargs
        self.skipIfFails = skipIfFails
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

        source = current_reader.clone()
        resource_filtered = current_reader.getResourceDict()
        resource_filtered.update( self.resource )
        source.updateResourceDict( resource_filtered )

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
    def getNewRecords( self ):

        if self.skipIfFails:
            try:
                records = [ r for r in self.getSource().getRecords() ]
                return records
            except Exception, e:
                rd = self.getSource().getResourceDict()
                rd['error'] = e
                self.log( rd )
                return []

        else:
            return self.getSource().getRecords()

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            yield record
        
        for record in self.getNewRecords():
            yield record

    # void
    def logActive( self, record ):

        self.log_file_pointer.write( '%s\n' % ( demjson.encode( record ) ) )

