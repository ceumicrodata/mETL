
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

import metl.source.base, codecs, re
import cElementTree as ElementTree

class XmlListConfig(list):
       
    # void
    def __init__( self, aList, tagname ):
                
        for element in aList:
            if element:
                if len( element ) == 1 or element[0].tag != element[1].tag:
                    self.append( XmlDictConfig( element ) )
                    
                elif element[0].tag == element[1].tag:
                    self.append( XmlListConfig( element, element[0].tag ) )
            
            elif element.items():
                aDict = dict( element.items() )
                if element.text:
                    text = element.text.strip()
                    if text:
                        aDict.update({ element.tag: text })
                
                if element.tag == tagname:
                    self.append( aDict )

            elif element.text:
                text = element.text.strip()
                if text:
                    if element.tag == tagname:
                        self.append( text )

class XmlListDictConfig(dict):

    def __init__( self, aList ):

        if len( aList ) == 0:
            return

        if len( aList ) == 1:
            self[tag] = XmlListConfig( aList, aList[0].tag )

        element_tags = set([])
        for idx in xrange( 1, len( aList ) ):
            if aList[ idx - 1 ].tag == aList[ idx ].tag:
                element_tags.add( aList[ idx ].tag ) 

        for tag in element_tags:
            self[ tag ] = XmlListConfig( aList, tag )

class XmlDictConfig(dict):

    # void
    def __init__( self, parent_element ):
        
        childrenNames = []
        for child in parent_element.getchildren():
            childrenNames.append( child.tag )

        if parent_element.items():
            self.update( dict( parent_element.items() ) )
            
        for element in parent_element:
            if element:
                if len( element ) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig( element )
                    
                else:
                    aDict = XmlListDictConfig( element )
                    
                if element.items():
                    aDict.update( dict( element.items() ) )

                if childrenNames.count(element.tag) > 1:
                    try:
                        currentValue = self[element.tag]
                        currentValue.append(aDict)
                        self.update({element.tag: currentValue})
                    except:
                        self.update({ element.tag: [ aDict ] })

                else:
                     self.update({element.tag: aDict})

            elif element.items():
                self.update({ element.tag: dict( element.items() ) })

            elif element.text:
                self.update({element.tag: element.text.strip()})

class XMLSource( metl.source.base.FileSource ):

    init = ['itemName']

    # void
    def __init__( self, fieldset, itemName = None, **kwargs ):

        self.itemName = itemName

        super( XMLSource, self ).__init__( fieldset, **kwargs )

    # list
    def getRecordsList( self ):

        tree = ElementTree.parse( self.getResource() )
        root = tree.getroot()
        ret  = XmlDictConfig(root)

        if self.itemName is not None:
            ret = ret[ self.itemName ]

        if type( ret ) != list:
            ret = [ ret ]
        
        return ret

    # FieldSet
    def getTransformedRecord( self, record ):

        return record
