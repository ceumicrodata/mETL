
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

import sqlalchemy, metl.fieldtype.base, demjson
from sqlalchemy.types import TypeDecorator, VARCHAR

class JSONType(TypeDecorator):

    impl = VARCHAR

    def process_bind_param(self, value, dialect):

        if value is not None:
            value = demjson.encode(value)

        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            value = demjson.decode(value)

        return value

class ListFieldType( metl.fieldtype.base.FieldType ):
            
    field_types = [ list ]
    alchemy_map = JSONType
  
    def getPreConvertValue( self, value ):

        return value is None

    # int
    def getConvertedValue( self, value ):

        if type( value ) in ( str, unicode ):
            try:
                converted = demjson.decode( value )
                if type( converted ) == list:
                    return converted
                else:
                    return [ converted ]
            except:
                return [ value ]

        try:
            return list( value )

        except:
            return [ value ]