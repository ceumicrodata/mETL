
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

import metl.transform.base, metl.configparser

class ConvertTypeTransform( metl.transform.base.Transform ):

    init = ['fieldType','hard','defaultValue']

    # void
    def __init__( self, fieldType, hard = False, defaultValue = None, *args, **kwargs ):

        self.fieldType     = fieldType
        self.hard          = hard
        self.defaultValue  = defaultValue

        self.field_type = metl.configparser.lookupClass(
            'metl.fieldtype.%(lowername)sfieldtype.%(name)sFieldType' % {
                'lowername': self.fieldType.lower(),
                'name': self.fieldType
            }
        )()

        super( ConvertTypeTransform, self ).__init__( *args, **kwargs )

    # Field
    def transform( self, field ):

        field.setType( self.field_type, hard = self.hard )
        if self.hard and self.defaultValue is not None:
            field.setValue( self.defaultValue )

        return field