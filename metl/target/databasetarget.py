
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

import metl.target.base, sqlalchemy, sqlalchemy.sql.expression, random, inspect, metl.configparser
from metl.database.alchemydatabase import AlchemyDatabase
from metl.database.postgresqldatabase import PostgresqlDatabase
from metl.exception import *

class DatabaseTarget( metl.target.base.Target ):

    init = ['createTable','replaceTable','truncateTable','addIDKey','idKeyName','bufferSize','continueOnError']
    resource_init = ['url','schema','table', 'fn']

    DISPATCH = {
        'default': AlchemyDatabase,
        'postgresql': PostgresqlDatabase
    }

    def __init__( self, reader, createTable = False, replaceTable = False, truncateTable = False, addIDKey = True, idKeyName = None, bufferSize = None, continueOnError = False, *args, **kwargs ):
        
        self.url                  = None
        self.table                = None
        self.schema               = None
        self.func                 = None
        self.database             = None
        self.createTable          = createTable
        self.replaceTable         = replaceTable
        self.truncateTable        = truncateTable
        self.addIDKey             = addIDKey
        self.idKeyName            = idKeyName
        self.continueOnError      = continueOnError
        self.bufferSize           = bufferSize or int( random.random()*10000+5000 )
        self.db_insert_buffer     = []
        self.db_update_buffer     = []
 
        super( DatabaseTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def setResource( self, url, table = None, schema = None, fn = None ):

        if table is None and fn is None:
            raise ParameterError('DatabaseTarget table or fn attribute is required!')

        self.url        = url
        self.table      = table
        self.schema     = schema

        if fn is not None:
            if inspect.ismethod( fn ) or inspect.isfunction( fn ):
                self.func = fn

            else:
                self.func = metl.configparser.lookupClass( fn )

        self.database = self.DISPATCH.get( url[:url.find(':')].lower(), self.DISPATCH['default'] )( self )

        return self

    def isFunction( self ):

        return self.func is not None

    def isTable( self ):

        return self.table is not None

    def getConnectionURL( self ):

        return self.url

    def getTableName( self ):

        return self.table

    def getSchemaName( self ):

        return self.schema

    def getIDKeyName( self ):

        return self.idKeyName

    def getAddIDKey( self ):

        return self.addIDKey

    def getFunction( self ):

        return self.func

    def isCreateTable( self ):

        return self.createTable

    def isReplaceTable( self ):

        return self.replaceTable

    def isTruncateTable( self ):

        return self.truncateTable

    def isContinueOnError( self ):

        return self.continueOnError

    # void
    def initialize( self ):

        self.database.open()
        return super( DatabaseTarget, self ).initialize()

    # void
    def execute( self ):

        self.database.execute(
            self.db_insert_buffer,
            self.db_update_buffer
        )
        self.db_insert_buffer = []
        self.db_update_buffer = []

    # void
    def finalize( self ):

        self.execute()
        self.database.close()
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
