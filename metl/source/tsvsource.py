
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

import metl.source.csvsource

class TSVSource( metl.source.csvsource.CSVSource ):

    init = ['delimiter','quote','skipRows','headerRow']

    # void
    def __init__( self, fieldset, delimiter = '\t', quote = '"', skipRows = 0, headerRow = None, **kwargs ):

        super( TSVSource, self ).__init__( fieldset, delimiter, quote, skipRows, headerRow, **kwargs )
