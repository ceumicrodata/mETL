
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

import metl.fieldtype.base, sqlalchemy

class BooleanFieldType( metl.fieldtype.base.FieldType ):
            
    field_types = [ bool ]

    # type
    def getConvertedValue( self, value ):

        if type( value ) in ( str, unicode ):
            if value.lower().strip() in ( 't', 'true' ):
                return True
            if value.lower().strip() in ( 'f,' 'false' ):
                return False

            try:
                return self.getFieldTypes()[0]( int( value ) )
            except:
                pass

        return self.getFieldTypes()[0]( value )