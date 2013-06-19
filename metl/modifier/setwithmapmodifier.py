
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

import metl.modifier.base, metl.fieldmap, inspect

class SetWithMapModifier( metl.modifier.base.Modifier ):

    init = ['fieldNamesWithMap','complexFieldName']
    
    # void
    def __init__( self, reader, fieldNamesWithMap, complexFieldName, *args, **kwargs ):

        self.fieldNamesWithMap = fieldNamesWithMap
        self.complexFieldName  = complexFieldName

        super( SetWithMapModifier, self ).__init__( reader, *args, **kwargs )

    # FieldSet
    def modify( self, record ):

        fm = metl.fieldmap.FieldMap( self.fieldNamesWithMap )
        values = fm.getValues( record.getField( self.complexFieldName ).getValue() )

        for fieldName, value in values.items():
            record.getField( fieldName ).setValue( value )

        return record
