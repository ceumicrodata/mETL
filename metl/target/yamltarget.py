
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

import metl.target.base, yaml

class YamlTarget( metl.target.base.FileTarget ):

    init = ['rootIterator', 'safe', 'flat']
    
    def __init__( self, reader, rootIterator, safe = False, flat = False, *args, **kwargs ):
        
        self.rootIterator = rootIterator
        self.flat         = flat
        self.safe         = safe
        self.records      = None

        super( YamlTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        self.flatMode = self.getFieldSetPrototypeCopy().getFieldNames()[0] if self.flat and len( self.getFieldSetPrototypeCopy().getFieldNames() ) == 1 else None
        self.records = []
        return super( YamlTarget, self ).initialize()

    # void
    def finalize( self ):

        if not self.safe:
            yaml.dump({ self.rootIterator: self.records }, self.file_pointer, default_flow_style = False )
        else:
            yaml.safe_dump({ self.rootIterator: self.records }, self.file_pointer, default_flow_style = False )

        return super( YamlTarget, self ).finalize()

    # void
    def writeRecord( self, record ):

        if self.flatMode is None:
            self.records.append( record.getValues() )

        else:
            self.records.append( record.getField( self.flatMode ).getValue() )

