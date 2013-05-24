
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

import metl.condition.base, re

class IsBetweenCondition( metl.condition.base.Condition ):

    init = ['fromValue','toValue']

    # void
    def __init__( self, fromValue, toValue, *args, **kwargs ):

    	self.fromValue = fromValue
    	self.toValue   = toValue
    	
        super( IsBetweenCondition, self ).__init__( *args, **kwargs )

    # bool
    def getResult( self, field ):

        return field.getValue() >= field.getType().getValue( self.fromValue ) and field.getValue() <= field.getType().getValue( self.toValue )
