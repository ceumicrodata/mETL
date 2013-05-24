
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

import metl.target.base

class FixedWidthTextTarget( metl.target.base.FileTarget ):

    init = ['addHeader']

    def __init__( self, reader, addHeader = True, *args, **kwargs ):
        
        self.addHeader = addHeader
        self.pattern   = None

        super( FixedWidthTextTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        super( FixedWidthTextTarget, self ).initialize()

        self.results = []
        if self.addHeader:
            self.results.append( dict( zip( 
                self.getFieldSetPrototypeCopy().getFieldNames(),
                self.getFieldSetPrototypeCopy().getFieldNames()
            ) ) )

        return self

    # void
    def writeRecord( self, record ):

        self.results.append( record.getValues( to_string = True ) )

    # void
    def finalize( self ):

        patterns = []
        fs = self.getFieldSetPrototypeCopy()
        field_names = fs.getFieldNames()
        for field_name in field_names:
            fs.getField( field_name ).getType().setWidth( max([ len(r[field_name]) for r in self.results ]) + 1 )
            patterns.append( fs.getField( field_name ).getPattern() )

        pattern = u''.join( patterns )
        for r in self.results:
            self.file_pointer.write( pattern % r + u'\n' )

        return super( FixedWidthTextTarget, self ).finalize()

