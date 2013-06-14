
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

import metl.expand.baseexpanderexpand, metl.fieldmap

class FieldExpand( metl.expand.baseexpanderexpand.BaseExpanderExpand ):

    init = ['fieldNamesAndLabels','valueFieldName','labelFieldName']

    # void
    def __init__( self, reader, fieldNamesAndLabels, valueFieldName, labelFieldName, *args, **kwargs ):

        self.fieldNamesAndLabels = fieldNamesAndLabels
        self.valueFieldName = valueFieldName
        self.labelFieldName = labelFieldName
        super( FieldExpand, self ).__init__( reader, *args, **kwargs )

    def expand( self, record ):

        for fieldName, label in self.fieldNamesAndLabels.items():
            fs = self.getFieldSetPrototypeCopy().clone()
            fs.setFieldMap( metl.fieldmap.FieldMap( dict( zip( fs.getFieldNames(), fs.getFieldNames() ) ) ) )
            fs.setValues( record.getValues() )
            fs.getField( self.valueFieldName ).setValue( record.getField( fieldName ).getValue() )
            fs.getField( self.labelFieldName ).setValue( label )

            yield fs
