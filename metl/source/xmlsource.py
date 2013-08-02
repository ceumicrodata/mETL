
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

import metl.source.base, codecs, re, xml2dict

class XMLSource( metl.source.base.FileSource ):

    init = ['itemName']

    # void
    def __init__( self, fieldset, itemName = None, **kwargs ):

        self.itemName = self.setRootIterator( itemName )
        super( XMLSource, self ).__init__( fieldset, **kwargs )

    def setRootIterator( self, rootIterator ):

        if rootIterator is not None:
            return rootIterator + '/!'
       
        return '!'

    def getEncoding( self ):

        return None

    # list
    def getRecordsList( self ):

        d = xml2dict.XML2Dict().parseFile( self.file_pointer )
        return metl.fieldmap.FieldMap({ 'root': self.itemName }).getValues( d ).get('root')

    # FieldSet
    def getTransformedRecord( self, record ):

        return record
