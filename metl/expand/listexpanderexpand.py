
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

class ListExpanderExpand( metl.expand.baseexpanderexpand.BaseExpanderExpand ):

    init = ['listFieldName','expandedFieldName','expanderMap']

    # void
    def __init__( self, reader, listFieldName, expandedFieldName = None, expanderMap = None, *args, **kwargs ):

        self.listFieldName = listFieldName
        self.expandedFieldName = expandedFieldName
        self.expanderMap = expanderMap
        super( ListExpanderExpand, self ).__init__( reader, *args, **kwargs )

    def expand( self, record ):

        for listValue in record.getField( self.listFieldName ).getValue() or []:
            fs = self.getFieldSetPrototypeCopy().clone()
            fs.setFieldMap( metl.fieldmap.FieldMap( dict( zip( fs.getFieldNames(), fs.getFieldNames() ) ) ) )
            fs.setValues( record.getValues() )

            if self.expandedFieldName is not None:
                fs.getField( self.expandedFieldName ).setValue( listValue )

            if self.expanderMap is not None:
                for k,v in self.expanderMap.items():
                    fs.getField( k ).setValue(
                        metl.fieldmap.FieldMap({ k: v }).getValues( listValue ).get( k )
                    )

            yield fs