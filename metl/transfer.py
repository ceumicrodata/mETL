
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

import time
from metl.exception import TableNotExistsError
from metl.target.databasetarget import DatabaseTarget
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.types import *
from sqlalchemy.ext.declarative import declarative_base

class Transfer( object ):

    # void
    def __init__( self, source_uri, target_uri, tables = None, truncate = None, run_before = None, run_after = None ):

        self.run_before = run_before
        self.run_after = run_after
        self.source_uri = source_uri
        self.target_uri = target_uri

        self.reConnectToSource()
        self.source_tables = self.getTableList( self.source_meta )

        self.target, self.target_engine = self.connect( self.target_uri )
        self.target_meta = MetaData( bind = self.target_engine )
        self.target_meta.reflect( self.target_engine )
        self.target_tables = self.getTableList( self.target_meta )
        self.target_connection = self.target_engine.connect()

        self.tables = tables or self.target_tables
        self.tables_before = [ ( t[0] if type(t) in ( tuple, list ) and len(t) == 2 else t ) for t in self.tables ]
        self.tables_after = [ ( t[1] if type(t) in ( tuple, list ) and len(t) == 2 else t ) for t in self.tables ]
        self.tables_rename = [ t for t in self.tables if type(t) in ( tuple, list ) and len(t) == 2 ]

        self.truncate = truncate or []

        self.missing_source_tables = set( self.tables_before ) - set( self.source_tables )
        self.missing_target_tables = set( self.tables_after ) - set( self.target_tables )
        self.common_tables  = set( self.tables_before ) - self.missing_source_tables - self.getSourceTableBySet( self.missing_target_tables )
        self.common_target_tables = self.getTargetTableBySet( self.common_tables )

        self.common_sorted_tables = [ self.getSourceTableName( table.name ) \
            for table in self.target_meta.sorted_tables \
            if table.name in self.common_target_tables ]

        self.target_database = DatabaseTarget.DISPATCH.get( target_uri[:target_uri.find(':')].lower(), DatabaseTarget.DISPATCH['default'] )( self )
        self.counter = 1

    # void
    def reConnectToSource( self ):

        self.source, self.source_engine = self.connect( self.source_uri )
        self.source_meta = MetaData( bind = self.source_engine )
        self.source_meta.reflect( self.source_engine )

    # set
    def getSourceTableBySet( self, tables ):

        rdict = set( self.tables_before ) & set( self.tables_after ) & tables
        
        for before, after in self.tables_rename:
            if after in tables:
                rdict.add( before )

        return rdict

    # set
    def getTargetTableBySet( self, tables ):

        rdict = set( self.tables_before ) & set( self.tables_after ) & tables
        
        for before, after in self.tables_rename:
            if before in tables:
                rdict.add( after )

        return rdict

    # unicode
    def getSourceTableName( self, table_name ):

        for before, after in self.tables_rename:
            if after == table_name:
                return before

        return table_name

    # unicode
    def getTargetTableName( self, table_name ):

        for before, after in self.tables_rename:
            if before == table_name:
                return after

        return table_name

    # bool
    def isContinueOnError( self ):

        return False

    # bool
    def isTable( self ):

        return True

    # bool
    def isFunction( self ):

        return False

    # tuple
    def connect( self, connection_uri ):

        engine = create_engine( connection_uri, echo = False, convert_unicode = True )
        session = sessionmaker( bind = engine )

        return Session(), engine

    # list<unicode>
    def getTableList( self, metadata ):

        return metadata.tables.keys()

    # void
    def initialize( self ):

        print '%d. Initialize' % ( self.counter )

        if len( self.missing_source_tables ) != 0:
            print
            print '  Missing source tables found (%s)' % ( ', '.join( self.missing_source_tables ) )

        if len( self.missing_target_tables ) != 0:
            print
            print '  Missing target tables found (%s)' % ( ', '.join( self.missing_target_tables ) )

        print '  Done'
        self.counter += 1

        return self.runBefore()

    # void
    def runBefore( self ):

        if self.run_before is None:
            return self

        print '%d. Run script on source' % ( self.counter )

        self.source_engine.execute( text( self.run_before ) )
        self.source_engine.execute('COMMIT')

        print '  Done'
        self.counter += 1

        return self

    # void
    def finalize( self ):

        return self.runAfter()

    def runAfter( self ):

        if self.run_after is None:
            return self

        print '%d. Run script on target' % ( self.counter )

        self.target_engine.execute( text( self.run_after ) )
        self.target_engine.execute('COMMIT')

        print '  Done'
        self.counter += 1

        return self

    # bool
    def isExistsTargetTable( self, table_name ):

        return table_name in self.target_tables

    # list<unicode>
    def getColumns( self, table ):

        return [ column.name for column in table.columns ]

    # void
    def migrateTable( self, source_table_name, target_table_name ):

        source_table = Table( source_table_name, self.source_meta, autoload = True )
        source_columns = self.getColumns( source_table )

        target_table = Table( target_table_name, self.target_meta, autoload = True )
        target_columns = self.getColumns( target_table )

        if target_table_name in self.truncate:
            self.target_engine.execute( target_table.delete() )

        common_columns = set( source_columns ) & set( target_columns )

        self.target_database.load( self.target_connection, self.target_meta, target_table )

        target_buffer = []
        for source_record in self.source.query( source_table ).all():
            target_data = {}

            for column_name in common_columns:
                source_column_value = getattr( source_record, column_name )

                if isinstance( target_table.columns[ column_name ].type, Boolean ):
                    source_column_value = bool( source_column_value )

                target_data[ column_name ] = source_column_value

            target_buffer.append( target_data )
            if len( target_buffer ) >= 5000:
                self.target_database.execute( target_buffer, [] )
                target_buffer = []

        if len( target_buffer ) != 0:
            self.target_database.execute( target_buffer, [] )

    # class
    def getMapperForTable( self, table ):

        Base = declarative_base()

        class Mapper( Base ):

            __table__ = table

        return Mapper

    # unicode
    def getTableName( self ):

        return self.table_name

    # void
    def migrate( self ):

        print '%d. Migrate:' % ( self.counter )

        for source_table_name in self.common_sorted_tables:

            target_table_name = self.getTargetTableName( source_table_name )

            started = time.time()
            if source_table_name == target_table_name:
                print '  Table (%s) ...' % ( source_table_name ),

            else:
                print '  Table (%s->%s) ...' % ( source_table_name, target_table_name ),
            
            try:
                self.table_name = target_table_name
                self.migrateTable( source_table_name, target_table_name )
                print 'OK (%02dm:%02ds)' % ( divmod( time.time() - started, 60 ) )

            except Exception, e:
                print 'FAIL', e

            self.reConnectToSource()

        print '  Done'
        self.counter += 1

        return self

    # void
    def sequences( self ):

        print '%d. Fix sequences:' % ( self.counter )

        for source_table_name in self.common_sorted_tables:
            started = time.time()
            target_table_name = self.getTargetTableName( source_table_name )
            table = Table( target_table_name, self.target_meta, autoload = True )
            columns = table.primary_key.columns.keys()

            if len( columns ) != 1:
                continue

            primary_key_column = columns[0]

            if not isinstance( table.columns[primary_key_column].type, Integer ):
                continue

            try:
                print '  Table (%s) ...' % ( target_table_name ),
                self.target_engine.execute(
                    'SELECT setval( \'%(table_name)s_id_seq\', MAX( "%(primary_key)s" ) ) FROM "%(table_name)s"' % {
                        'table_name': target_table_name, 
                        'primary_key': columns[0]
                    }
                )
                print 'OK (%02dm:%02ds)' % ( divmod( time.time() - started, 60 ) )

            except Exception, e:
               print 'FAIL', e

        print '  Done'
        self.counter += 1

        return self

