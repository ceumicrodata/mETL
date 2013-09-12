
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

import metl.source.base, sqlalchemy, os, re

class DatabaseSource( metl.source.base.Source ):

    resource_init = ['url','schema','table','statement','resource','params']

   # void
    def __init__( self, fieldset, **kwargs ):

        self.connection    = None
        self.url           = None
        self.table         = None
        self.schema        = None
        self.statement     = None
        self.db_connection = None
        self.db_closable   = None

        super( DatabaseSource, self ).__init__( fieldset, **kwargs )

    # void
    def setResource( self, url, resource = None, schema = None, table = None, statement = None, params = None ):

        res_dict = {
            'url': url,
            'resource': resource,
            'schema': schema,
            'table': table,
            'statement': statement,
            'params': params,
        }

        self.updateResourceDict( res_dict )

        return self

    def updateResourceDict( self, res_dict ):

        super( DatabaseSource, self ).updateResourceDict( res_dict )

        if self.res_dict['resource'] is None and self.res_dict['statement'] is None and self.res_dict['table'] is None:
            raise AttributeError( 'Resource, table or statement is required!' )

        statement = None
        if self.res_dict['resource'] is not None and self.res_dict['statement'] is None and self.res_dict['table'] is None:
            fp, closable = metl.source.base.openResource( os.path.abspath( self.res_dict['resource'] ), 'r' )
            statement = fp.read()
            if closable:
                fp.close()

        self.url        = self.res_dict['url']
        self.table      = self.res_dict['table']
        self.schema     = self.res_dict['schema']
        self.statement  = statement or self.res_dict['statement']
        if self.res_dict['params'] is not None and type( self.res_dict['params'] ) == dict:
            for k,v in self.res_dict['params'].items():
                self.statement = re.sub( ':' + k, str(v), self.statement )

    # void
    def initialize( self ):

        engine = sqlalchemy.create_engine( self.url )

        self.db_connection = engine.connect()
        self.db_metadata = sqlalchemy.MetaData()
        self.db_metadata.bind = self.db_connection.engine

        if self.statement is None:
            self.db_table = self.getTable()

        return super( DatabaseSource, self ).initialize()

    # void
    def finalize( self ):

        self.db_connection.close()
        return super( DatabaseSource, self ).finalize()

    # sqlalchemy.Table
    def getTable( self ):
        
        return sqlalchemy.Table( self.table, self.db_metadata, autoload = True, schema = self.schema )

    # dict
    def execute( self ):

        if self.statement is not None:
            return self.db_connection.execute( sqlalchemy.text( self.statement ) )

        return self.db_table.select().execute()

    # list
    def getRecordsList( self ):

        return self.execute()

    # FieldSet
    def getTransformedRecord( self, record ):

        return dict( record )

