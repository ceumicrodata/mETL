
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

import os, sys, codecs, yaml, metl.source.base

class Config( object ):

    # void
    def __init__( self, resource ):
        
        self.configurations, self.dict = [], {}
        self.loadYaml( resource )
        for cfg in self.configurations:
            self.mergeDict( cfg, self.dict )

    # void
    def loadYaml( self, resource ):

        file_pointer, file_closeable = metl.source.base.openResource( resource, 'r' )
        rdict = yaml.load( file_pointer )
        if file_closeable:
            file_pointer.close()

        self.configurations.insert( 0, rdict )

        if 'base' in rdict.keys():
            self.loadYaml( rdict['base'] )

    # void
    def mergeDict( self, cfg, base = None ):

        for k in cfg:
            if type( cfg[k] ) == dict:
                if k not in base:
                    base[ k ] = {}

                self.mergeDict( cfg[k], base[k] )

            elif ( isinstance( cfg[k], str ) or isinstance( cfg[k], unicode ) ) and cfg[k] == '__remove__' and k in base:
                del base[ k ]

            else:
                base[ k ] = cfg[ k ]

    # int
    def __len__( self ):
        
        return len( self.dict )

    # type
    def __getitem__( self, key ):

        return self.dict[ key ]

    # void
    def __setitem__( self, key, value ):

        self.dict[key] = value

    # void
    def __delitem__( self, key ):

        del self.dict[key]

    # iter
    def __iter__( self ):

        return self.dict.__iter__()

    # iter
    def iterkeys( self ):

        return self.dict.__iter__()

    # type
    def get( self, key, default_value = None ):

        return self.dict.get( key, default_value )

    # dict
    def getDict( self ):

        return self.dict