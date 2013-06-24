
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

import metl.aggregator.base

class AvgAggregator( metl.aggregator.base.Aggregator ):

    init = ['fieldNames','targetFieldName','listFieldName','valueFieldName']

    def __init__( self, reader, fieldNames, targetFieldName, valueFieldName, listFieldName = None, *args, **kwargs ):

        self.valueFieldName = valueFieldName
        super( AvgAggregator, self ).__init__( reader, fieldNames, targetFieldName, listFieldName, *args, **kwargs )

    def aggregate( self, records ):

        return sum([ r.getField( self.valueFieldName ).getValue() or 0 for r in records ]) / float( len( records ) )