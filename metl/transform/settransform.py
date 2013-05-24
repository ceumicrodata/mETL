
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

class SetTransform( metl.transform.base.Transform ):

    init = ['value']

    # void
    def __init__( self, value, *args, **kwargs ):

        self.value = value
        
        super( SetTransform, self ).__init__( *args, **kwargs )

    def transform( self, field ):

        try:
            v = self.value % { 'self': field.getValue() }
        except:
            v = self.value

        field.setValue( v )
        return field