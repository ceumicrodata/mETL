
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

import pickle, metl.source.base, os, codecs
from metl.exception import *

class Migration( object ):

    MODE_SOURCE = 1
    MODE_TARGET = 2

    # void
    def __init__( self, filepath, mode = 1, migration_type = None ):

        self.migration_resource = os.path.abspath( filepath )
        self.migration_type     = migration_type
        self.migration_mode     = mode

    # void        
    def initialize( self ):

        if os.path.exists( self.migration_resource ) and self.migration_mode == self.MODE_SOURCE:
            migration_file_pointer, migration_file_closable = metl.source.base.openResource( self.migration_resource, 'r' )
            self.migration_data = pickle.load( migration_file_pointer )
            self.migration_type = self.getMigrationType()
            migration_file_pointer.close()

        elif self.migration_type is not None and self.migration_mode == self.MODE_TARGET:
            self.migration_data = set() if self.migration_type == list else {}

        else:
            raise MigrationNotCompatibleError( 'Migration initalization error!' )

    def finalize( self ):

        if self.migration_mode == self.MODE_TARGET:
            migration_file_pointer = codecs.open( self.migration_resource, 'w' )
            pickle.dump( self.getMigrationData( convert = True ), migration_file_pointer )
            migration_file_pointer.close()

    def getMigrationData( self, convert = False ):

        if not convert:
            return self.migration_data

        return self.migration_data if type( self.migration_data ) == dict else list( self.migration_data ) 

    def addRecord( self, record ):

        if self.getMigrationType() == list:
            self.migration_data.add( record.getHash() )

        else:
            self.migration_data[ record.getKey() ] = record.getHash()

    def getRecordStatus( self, record ):

        if self.getMigrationType() == list:
            return { 'exists': record.getHash() in self.getMigrationData() }

        else: 
            record_key = record.getKey()
            try:
                if self.getMigrationData()[ record_key ] != record.getHash():
                    return { 'exists': True, 'modified': True }
                else:
                    return { 'exists': True, 'modified': False }
            except:
                return { 'exists': False }

    # type
    def getMigrationType( self ):

        if self.migration_type is not None:
            return self.migration_type

        if type( self.migration_data ) in ( list, set ):
            return list

        return dict

    def getDeleted( self, migration ):

        if self.getMigrationType() != migration.getMigrationType():
            raise MigrationNotCompatibleError( 'Migration type is not compatible!' ) 

        if self.getMigrationType() == list:
            return list( migration.getMigrationData() - self.getMigrationData() )

        else:
            return list( set( migration.getMigrationData().keys() ) - set( self.getMigrationData().keys() ) )

    def getNews( self, migration ):

        if self.getMigrationType() != migration.getMigrationType():
            raise MigrationNotCompatibleError( 'Migration type is not compatible!' ) 

        if self.getMigrationType() == list:
            return list( self.getMigrationData() - migration.getMigrationData() )

        else:
            return list( set( self.getMigrationData().keys() ) - set( migration.getMigrationData().keys() ) )

    def getUpdated( self, migration ):

        if self.getMigrationType() != migration.getMigrationType():
            raise MigrationNotCompatibleError( 'Migration type is not compatible!' ) 

        if self.getMigrationType() == list:
            return []

        else:
            updated_keys = []
            for key in list( set( self.getMigrationData().keys() ) - set( list( set( migration.getMigrationData().keys() ) - set( self.getMigrationData().keys() ) ) + list( set( self.getMigrationData().keys() ) - set( migration.getMigrationData().keys() ) ) ) ):
                if self.getMigrationData()[ key ] != migration.getMigrationData()[ key ]:
                    updated_keys.append( key )

            return updated_keys

    def getUnchanged( self, migration ):

        if self.getMigrationType() != migration.getMigrationType():
            raise MigrationNotCompatibleError( 'Migration type is not compatible!' ) 

        if self.getMigrationType() == list:
            return list( set( self.getMigrationData() ) - set( list( migration.getMigrationData() - self.getMigrationData() ) + list( self.getMigrationData() - migration.getMigrationData() ) ) )

        else:
            unchanged_keys = []
            for key in list( set( self.getMigrationData().keys() ) - set( list( set( migration.getMigrationData().keys() ) - set( self.getMigrationData().keys() ) ) + list( set( self.getMigrationData().keys() ) - set( migration.getMigrationData().keys() ) ) ) ):
                if self.getMigrationData()[ key ] == migration.getMigrationData()[ key ]:
                    unchanged_keys.append( key )

            return unchanged_keys

