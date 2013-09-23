
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

import metl.transform.base

class SplitTransform( metl.transform.base.Transform ):

    init = ['idx','chars']

    # void
    def __init__( self, idx = None, chars = None, *args, **kwargs ):

        self.idx = idx
        self.chars = chars
        
        super( SplitTransform, self ).__init__( *args, **kwargs )

    def transform( self, field ):

        if field.getValue() is None:
            return field

        if self.idx is None:
            if type( field.getValue() ) == list and len( field.getValue() ) > 0:
                v = field.getValue()[0]
            else:
                v = field.getValue()

            field.setValue( v.split( self.chars ) )

        elif self.idx.count(u':') == 1:
            pts = self.idx.split(u':')
            pts0 = None if len( pts[0] ) == 0 else int( pts[0] )
            pts1 = None if len( pts[1] ) == 0 else int( pts[1] )
            field.setValue( u' '.join( field.getValue().split( self.chars )[ pts0 : pts1 ] ) )

        else:
            field.setValue( field.getValue().split( self.chars )[ int( self.idx ) ] )

        return field