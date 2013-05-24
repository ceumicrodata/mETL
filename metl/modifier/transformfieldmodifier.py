
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

import metl.modifier.base, tarr.compiler, tarr.data

class TransformFieldModifier( metl.modifier.base.Modifier ):

    init = ['fieldNames','transforms']
    
    # void
    def __init__( self, reader, fieldNames, transforms = None, *args, **kwargs ):

        self.fields      = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.transforms  = transforms
       
        super( TransformFieldModifier, self ).__init__( reader, *args, **kwargs )

    # list
    def getTransforms( self ):

        return self.transforms or []

    # FieldSet
    def modify( self, record ):

        for field_name in self.fields:
            tarr.compiler.Program( self.getTransforms() + [ tarr.compiler.RETURN_TRUE ] ).run( 
                tarr.data.Data( field_name, record.getField( field_name ) ) 
            ).payload

        return record
