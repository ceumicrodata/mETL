
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

class AppendBySourceExpand( metl.expand.base.Expand ):

    init = ['source']

    # void
    def __init__( self, reader, source, *args, **kwargs ):

        self.source = source

        super( AppendBySourceExpand, self ).__init__( reader, *args, **kwargs )

    # Source
    def getSource( self ):

        return self.source

    # void
    def initialize( self ):

        self.source.initialize()
        return super( AppendBySourceExpand, self ).initialize()

    # void
    def finalize( self ):

        self.source.finalize()
        return super( AppendBySourceExpand, self ).finalize()

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            yield record
            
        for record in self.getSource().getRecords():
            fs = self.getFieldSetPrototypeCopy()
            fs.setFieldMap( metl.fieldmap.FieldMap( dict( zip( fs.getFieldNames(), fs.getFieldNames() ) ) ) )
            fs.setValues( record.getValues() )
            yield fs
