
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

THIS FILE IS BASED ON BREWERY PYTHON DS CLASS!
https://pypi.python.org/pypi/brewery/0.8.0
"""

import metl.target.base, sqlalchemy, sqlalchemy.sql.expression, random
from metl.exception import *

class DatabaseTarget( metl.target.base.Target ):

    init = ['createTable','replaceTable','truncateTable','addIDKey','idKeyName','bufferSize']
    resource_init = ['url','schema','table']

    def __init__( self, reader, createTable = False, replaceTable = False, truncateTable = False, addIDKey = True, idKeyName = 'id', bufferSize = None, *args, **kwargs ):
        
        self.connection           = None
        self.url                  = None
        self.table                = None
        self.schema               = None
        self.createTable          = createTable
        self.replaceTable         = replaceTable
        self.truncateTable        = truncateTable
        self.addIDKey             = addIDKey
        self.idKeyName            = idKeyName
        self.bufferSize           = bufferSize or int( random.random()*10000+5000 )
        self.db_connection        = None
        self.db_closable          = None
        self.db_table             = None
        self.db_insert_command    = None
        self.db_insert_buffer     = None
 
        super( DatabaseTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def setResource( self, url, table, schema = None ):

        self.url        = url
        self.table      = table
        self.schema     = schema

        return self

    # void
    def initialize( self ):

        engine, now_created = sqlalchemy.create_engine( self.url ), False

        self.db_connection = engine.connect()
        self.db_metadata = sqlalchemy.MetaData()
        self.db_metadata.bind = self.db_connection.engine

        if not self.isExistsTable() and self.createTable:
            self.db_table = self.getCreatedTable()
            now_created   = True
        
        if not self.isExistsTable() and self.db_table is None:
            raise ResourceNotExistsError( 'Table %s does not exist!' % ( self.table ) )

        if self.isExistsTable() and self.replaceTable and not now_created:
            self.db_table = self.getTable( autoload = False )
            self.db_table.drop( checkfirst = False )
            self.db_table = self.getCreatedTable()

        if self.isExistsTable() and not self.replaceTable and self.truncateTable and not now_created:
            self.db_table = self.getTable( autoload = False )
            self.db_table.delete().execute()
            self.db_table = self.getTable( autoload = True, extend_existing = True )

        if self.db_table is None:
            self.db_table = self.getTable( autoload = True, extend_existing = True )

        self.db_insert_command = self.db_table.insert()
        self.db_insert_buffer  = []
        self.db_update_buffer  = []

        if self.getMigrationType() == dict:
            cols = [ getattr( self.db_table.c, field_name ) for field_name in self.getFieldSetPrototypeCopy().getKeyFieldList() ]
            col  = cols[0]
            for c in cols[1:]:
                col = col.concat( '-' )
                col = col.concat( c )

            self.db_update_command = self.db_table.update().where( col == sqlalchemy.sql.expression.bindparam('metlkey') )

        return super( DatabaseTarget, self ).initialize()

    # bool
    def isExistsTable( self ):

        return self.getTable( autoload = False ).exists()

    # sqlalchemy.Table
    def getTable( self, autoload, extend_existing = False ):
        
        return sqlalchemy.Table( self.table, self.db_metadata, autoload = autoload, schema = self.schema, extend_existing = extend_existing )

    # sqlalchemy.Table
    def getCreatedTable( self ):

        db_table = sqlalchemy.Table( self.table, self.db_metadata, schema = self.schema )

        if self.addIDKey:
            db_sequence = sqlalchemy.schema.Sequence( 'seq_%s_%s' % ( self.table, self.idKeyName ), optional = True )
            db_column = sqlalchemy.schema.Column( self.idKeyName, sqlalchemy.types.Integer, db_sequence, primary_key = True )
            db_table.append_column( db_column )

        fs = self.getFieldSetPrototypeCopy()
        for field_name in fs.getFieldNames():
            field = fs.getField( field_name )

            sqlalchemy_type = field.getType().getAlchemyType()

            # force limit on mysql varchar
            limit = field.getLimit()
            if self.url.startswith( 'mysql' ) and not limit and (sqlalchemy_type == sqlalchemy.types.Unicode):
              limit = 255

            # apply eventual limit to sqlalchemy_type
            if limit:
              sqlalchemy_type = sqlalchemy_type( limit )

            db_column = sqlalchemy.schema.Column( field_name, sqlalchemy_type )
            db_table.append_column( db_column )

        db_table.create()
        
        return db_table

    # void
    def execute( self ):

        if len( self.db_insert_buffer ) > 0:
            self.db_connection.execute( self.db_insert_command, self.db_insert_buffer )
            self.db_insert_buffer = []

        if len( self.db_update_buffer ) > 0:
            self.db_connection.execute( self.db_update_command, self.db_update_buffer )
            self.db_update_buffer = []

    # void
    def finalize( self ):

        self.execute()
        self.db_connection.close()
        return super( DatabaseTarget, self ).finalize()

    # void
    def updateRecord( self, record, record_key ):

        data            = record.getValues()
        data['metlkey'] = record_key

        self.db_update_buffer.append( data )
        if len( self.db_insert_buffer ) + len( self.db_update_buffer ) >= self.bufferSize:
            self.execute()

    # void
    def writeRecord( self, record ):

        self.db_insert_buffer.append( record.getValues() )
        if len( self.db_insert_buffer ) + len( self.db_update_buffer ) >= self.bufferSize:
            self.execute()
