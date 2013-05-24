
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

import metl.target.csvtarget

class TSVTarget( metl.target.csvtarget.CSVTarget ):

    def __init__( self, reader, delimiter = '\t', quote = '"', addHeader = True, appendFile = False, *args, **kwargs ):
        
        super( TSVTarget, self ).__init__( reader, delimiter, quote, addHeader, appendFile, *args, **kwargs )