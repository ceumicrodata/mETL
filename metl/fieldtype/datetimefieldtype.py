
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

import datetime, sqlalchemy, metl.fieldtype.base
from dateutil import parser
from metl.exception import *

class DateTimeFieldType( metl.fieldtype.base.FieldType ):
            
    field_classes = [ datetime.datetime ]

    # datetime.datetime
    def getConvertedValue( self, value ):

        if type( value ).__name__ in ( 'tuple', 'list' ):
            return datetime.datetime( value[0], value[1], value[2], value[3], value[4], value[5] )

        if type( value ).__name__ in ( 'unicode', 'str' ):
            return parser.parse( value )

        if isinstance( value, datetime.date ):
            return datetime.datetime( value.year, value.month, value.day )

        raise FieldTypeValueConversionError()
