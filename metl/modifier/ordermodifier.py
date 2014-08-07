
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

import metl.modifier.base, operator

class OrderModifier( metl.modifier.base.Modifier ):

    init = ['fieldNamesAndOrder']

    # void
    def __init__( self, reader, fieldNamesAndOrder, *args, **kwargs ):

        self.fieldNamesAndOrder = fieldNamesAndOrder
        self.fieldNamesAndOrder.reverse()
        super( OrderModifier, self ).__init__( reader, *args, **kwargs )

    # dict
    def merge( self, record ):

        data = record.getValues()
        data['__record__'] = record

        return data

    # list<FieldSet>
    def getRecords( self ):

        records = [ self.merge( record ) for record in self.getReader().getRecords() ]

        for prefItem in self.fieldNamesAndOrder:
            fieldName, order = prefItem.popitem()

            records.sort(
                key = operator.itemgetter( fieldName ),
                reverse = order.upper() != 'ASC'
            )

        return ( record['__record__'] for record in records )
