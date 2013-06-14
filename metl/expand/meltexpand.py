
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

class MeltExpand( metl.expand.baseexpanderexpand.BaseExpanderExpand ):

    init = ['fieldNames','valueFieldName','labelFieldName']

    # void
    def __init__( self, reader, fieldNames, valueFieldName, labelFieldName, *args, **kwargs ):

        self.fields = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.valueFieldName = valueFieldName
        self.labelFieldName = labelFieldName
        super( MeltExpand, self ).__init__( reader, *args, **kwargs )

    def getFieldNames( self ):

        field_names = set( self.getReader().getFieldSetPrototypeCopy().getFieldNames() )
        stacked_fields = set( self.fields )

        return list( field_names - stacked_fields - set([ self.valueFieldName, self.labelFieldName ]) )

    def expand( self, record ):

        for fieldName in self.getFieldNames():
            fs = self.getReader().getFieldSetPrototypeCopy().clone()
            fs.setFieldMap( metl.fieldmap.FieldMap( dict( zip( fs.getFieldNames(), fs.getFieldNames() ) ) ) )
            fs.setValues( record.getValues() )
            fs.getField( self.valueFieldName ).setValue( record.getField( fieldName ).getValue() )
            fs.getField( self.labelFieldName ).setValue( fieldName )

            for field in self.getFieldNames():
                if fs.hasField( field ):
                    fs.deleteField( field )

            yield fs

    # FieldSet
    def getFieldSetPrototypeCopy( self, final = True ):

        if str(final) not in self.fieldset:
            fs = self.getReader().getFieldSetPrototypeCopy( final = final ).clone()
            for field in self.getFieldNames():
                if fs.hasField( field ):
                    fs.deleteField( field )

            self.fieldset.setdefault( str(final), fs )
        
        return self.fieldset[ str(final) ]
