
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

class BaseDatabase( object ):

    TYPES = {}
    DEFAULT_LIMIT = {}

    def __init__( self, target ):

        self.target        = target
        self.session       = None
        self.connection    = None

    # void
    def open( self ):

        raise RuntimeError('BaseDatabaseHandler.open() function is not implemented!')

    # void
    def close( self ):

        raise RuntimeError('BaseDatabaseHandler.close() function is not implemented!')

    # void
    def iterateInsert( self, buffer ):

         raise RuntimeError('BaseDatabaseHandler.iterateInsert() function is not implemented!')

    # void
    def insert( self, buffer ):

        raise RuntimeError('BaseDatabaseHandler.insert() function is not implemented!')

    # void
    def update( self, buffer ):

        raise RuntimeError('BaseDatabaseHandler.update() function is not implemented!')

    # type
    def run( self, insert_buffer, update_buffer ):

        return self.target.getFunction()( self.connection, insert_buffer, update_buffer )

    # void
    def execute( self, insert_buffer, update_buffer ):

        if self.target.isTable():
            if len( insert_buffer ) != 0:
                if self.target.isContinueOnError():
                    try:
                        self.insert( insert_buffer )
                    except:
                        self.iterateInsert( insert_buffer )
                
                else:
                    self.insert( insert_buffer )

            if len( update_buffer ) != 0:
                self.update( update_buffer )

        if self.target.isFunction():
            self.run( insert_buffer, update_buffer )
