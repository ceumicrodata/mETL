
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

import metl.target.base
from xml.etree import ElementTree
from xml.dom import minidom

def prettify( element, encoding ):

    rough_string = ElementTree.tostring( element, encoding )
    reparsed = minidom.parseString( rough_string )
    return reparsed.toprettyxml( indent = 2*' ' )

class XMLTarget( metl.target.base.FileTarget ):

    init = ['rootIterator','itemName','flat']
    
    def __init__( self, reader, itemName, rootIterator = 'root', flat = False, *args, **kwargs ):
        
        self.rootIterator = rootIterator
        self.itemName     = itemName
        self.flat         = flat
        self.root         = None

        super( XMLTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        self.flatMode = self.getFieldSetPrototypeCopy().getFieldNames()[0] if self.flat and len( self.getFieldSetPrototypeCopy().getFieldNames() ) == 1 else None
        self.root = ElementTree.Element( self.rootIterator )
        return super( XMLTarget, self ).initialize()

    # void
    def finalize( self ):

        self.file_pointer.write( prettify( self.root, self.getEncoding() ) )
        return super( XMLTarget, self ).finalize()

    # void
    def writeRecord( self, record ):

        rec = ElementTree.SubElement( self.root, self.itemName )
        if self.flatMode is None:
            for field_name, field_value in record.getValues( to_string = True ).items():
                item = ElementTree.SubElement( rec, field_name )
                item.text = field_value

        else:
            rec.text = record.getField( self.flatMode ).getValue()
