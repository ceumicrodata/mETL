
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

class DropByConditionFilter( metl.filter.base.Filter ):

    init = ['condition','operation','fieldNames','fieldNames']

    # void
    def __init__( self, reader, condition, fieldNames, operation = 'AND', *args, **kwargs ):

        self.condition = condition
        self.fields    = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.operation = operation.upper()

        super( DropByConditionFilter, self ).__init__( reader, *args, **kwargs )

    # bool
    def isANDOperation( self ):

        return self.operation.upper() == 'AND'

    # bool
    def isOROperation( self ):

        return self.operation.upper() == 'OR'

    # bool
    def isFiltered( self, record ):

        for field_name, field in record.getFields().items():
            if field_name not in self.fields:
                continue

            try:
                result = self.condition.getResult( field )
            except:
                result = self.condition( field )

            if result and self.isOROperation():
                return True

            elif not result and self.isANDOperation():
                return False

        return self.isANDOperation()
