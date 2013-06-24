
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

import metl.manipulation, metl.fieldmap

class Aggregator( metl.manipulation.Manipulation ):

    init = ['fieldNames','targetFieldName','listFieldName']

    # void
    def __init__( self, reader, fieldNames, targetFieldName, listFieldName = None, *args, **kwargs ):

        self.fields = fieldNames if type( fieldNames ) == list else [ fieldNames ]
        self.targetFieldName = targetFieldName
        self.listFieldName = listFieldName
        self.setStackedFields()

        super( Aggregator, self ).__init__( reader, *args, **kwargs )

    def setStackedFields( self ):

        self.stacked_fields = self.fields + [ self.targetFieldName ]
        if self.listFieldName is not None:
            self.stacked_fields += [ self.listFieldName ]

    def getFieldNames( self ):

        return list( set( self.getReader().getFieldSetPrototypeCopy().getFieldNames() ) - set( self.stacked_fields ) )

    def getFieldSetPrototypeCopy( self, final = True ):

        if str(final) not in self.fieldset:
            fs = self.getReader().getFieldSetPrototypeCopy( final = final ).clone()
            for field in self.getFieldNames():
                if fs.hasField( field ):
                    fs.deleteField( field )

            self.fieldset.setdefault( str(final), fs )
        
        return self.fieldset[ str(final) ]

    def aggregate( self, record, records ):

        raise RuntimeError('Aggregator.aggregate() function is not implemented yet.')

    def getRecords( self ):

        rdict = {}
        for record in self.getReader().getRecords():
            key = '|'.join([ record.getField(fN).getValue( to_string = True ) for fN in self.fields ])
            rdict.setdefault( key, [] )
            rdict[ key ].append( record )

        for key, records in rdict.items():
            fs = self.getReader().getFieldSetPrototypeCopy().clone()
            fs.setFieldMap( metl.fieldmap.FieldMap( dict( zip( fs.getFieldNames(), fs.getFieldNames() ) ) ) )
            fs.setValues( records[0].getValues() )

            for field in self.getFieldNames():
                if fs.hasField( field ):
                    fs.deleteField( field )
            
            fs.getField( self.targetFieldName ).setValue( self.aggregate( records ) )

            if self.listFieldName is not None:
                fs.getField( self.listFieldName ).setValue([ r.getKey() for r in records ])

            yield fs