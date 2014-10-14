
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

import copy
from dm import Mapper

class FieldMap( object ):

    # void
    def __init__( self, rules ):

        self.rules = rules
        self.base_rules = { k:unicode(v[1:]) for k,v in rules.items() if unicode(v).startswith(u'/') }
        self.normal_rules = { k:v for k,v in rules.items() if not unicode(v).startswith(u'/') }

    # dict
    def getRules( self ):

        return self.rules

    # dict
    def getValues( self, obj, base = None ):

        ret_dict = Mapper( obj, routes = self.normal_rules ).getRoutes()
        ret_dict.update( Mapper( base, routes = self.base_rules ).getRoutes() )

        return ret_dict
