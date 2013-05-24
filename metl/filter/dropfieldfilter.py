
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

import metl.filter.base

class DropFieldFilter( metl.filter.base.Filter ):

    init = ['fieldNames']

    # void
    def __init__( self, reader, fieldNames, *args, **kwargs ):

        self.fields   = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.fieldset = {}

        super( DropFieldFilter, self ).__init__( reader, *args, **kwargs )

    # FieldSet
    def getFieldSetPrototypeCopy( self, final = True ):

        if str(final) not in self.fieldset:
            fs = self.getReader().getFieldSetPrototypeCopy( final = final ).clone()
            for field in self.fields:
                if fs.hasField( field ):
                    fs.deleteField( field )

            self.fieldset.setdefault( str(final), fs )
        
        return self.fieldset[ str(final) ]

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            for field in self.fields:
                if record.hasField( field ):
                    record.deleteField( field )

            yield record