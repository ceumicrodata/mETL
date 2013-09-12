
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

import metl.reader

class Manipulation( metl.reader.Reader ):

    init = []
    use_args = False

    # void
    def __init__( self, reader, *args, **kwargs ):

        self.reader   = reader
        self.fieldset = {}

        super( Manipulation, self ).__init__( *args, **kwargs )
        
    # void
    def __iter__( self ):

        return self

    # Reader
    def getReader( self ):

        return self.reader

    # void
    def initialize( self ):

        self.getReader().initialize()
        return super( Manipulation, self ).initialize()

    # void
    def finalize( self ):

        self.getReader().finalize()
        return super( Manipulation, self ).finalize()

    # FieldSet
    def getFieldSetPrototypeCopy( self, final = True ):

        self.fieldset.setdefault( str(final), self.getReader().getFieldSetPrototypeCopy( final = final ) )
        return self.fieldset[ str(final) ]
