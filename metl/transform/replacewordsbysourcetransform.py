
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

import metl.transform.base, re

class ReplaceWordsBySourceTransform( metl.transform.base.Transform ):

    init = ['sourceRecords','join']

    # void
    def __init__( self, sourceRecords, join, *args, **kwargs ):

        self.join   = join
        self.values = {}

        if len( sourceRecords ) > 0:
            all_field = set( sourceRecords[0].getFieldNames() )
            join_field = set([ self.join ])
            self.joinValue = list( all_field - join_field )[0]
            for record in sourceRecords:
                key = record.getField( self.join ).getValue()
                value = record.getField( self.joinValue ).getValue()

                if key is not None and value is not None:
                    self.values[ key ] = value

        super( ReplaceWordsBySourceTransform, self ).__init__( *args, **kwargs )

    # Field
    def transform( self, field ):

        if field.getValue() is None:
            return field

        words = []
        for word in field.getValue().split():
            if word not in self.values.keys():
                words.append( word ) 
            else:
                words.append( self.values[ word ] )

        field.setValue( u' '.join( words ) )
        return field