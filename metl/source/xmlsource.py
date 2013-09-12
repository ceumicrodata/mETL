
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

import metl.source.base, codecs, re, xmlsquash

class XMLSource( metl.source.base.FileSource ):

    init = ['itemName']

    # void
    def __init__( self, fieldset, itemName = None, **kwargs ):

        self.itemName = self.setRootIterator( itemName )
        super( XMLSource, self ).__init__( fieldset, **kwargs )

    # XMLSource
    def clone( self ):

        return self.__class__(
            self.fieldset.clone(),
            itemName = self.itemName
        )

    def setRootIterator( self, rootIterator ):

        if rootIterator is not None:
            return rootIterator + '/!'
       
        return '!'

    def getEncoding( self ):

        return None

    # list
    def getRecordsList( self ):

        self.base = xmlsquash.XML2Dict().parseFile( self.file_pointer )
        return metl.fieldmap.FieldMap({ 'root': self.itemName }).getValues( self.base ).get('root') or []

    # FieldSet
    def getTransformedRecord( self, record ):

        return record
