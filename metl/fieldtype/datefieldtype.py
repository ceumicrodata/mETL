
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

class DateFieldType( metl.fieldtype.base.FieldType ):
            
    field_classes = [ datetime.date ]

    # datetime.date
    def getConvertedValue( self, value ):

        if type( value ).__name__ in ( 'tuple', 'list' ):
            return datetime.date( value[0], value[1], value[2] )

        if type( value ).__name__ in ( 'unicode', 'str' ):
            try:
                return parser.parse( value ).date()
            except:
                return parser.parse( value.split(' ')[0] ).date() # Containes not valid timestamp

        if value.__class__ == datetime.datetime:
        	return value.date()

        raise FieldTypeValueConversionError()