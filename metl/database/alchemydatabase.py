
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

import sqlalchemy, sqlalchemy.sql.expression, sqlalchemy.types, random, \
    metl.database.basedatabase, json, demjson, pickle
from sqlalchemy.types import TypeDecorator, VARCHAR, LargeBinary
from metl.exception import *

class JSONType( TypeDecorator ):

    impl = VARCHAR

    def process_bind_param( self, value, dialect ):

        if value is not None:
            value = demjson.encode(value)

        return value

    def process_result_value( self, value, dialect ):
        if value is not None:
            value = json.loads(value)

        return value

class PickleType( TypeDecorator ):

    impl = LargeBinary

    def process_bind_param( self, value, dialect ):

        if value is not None:
            value = pickle.dumps(value)

        return value

    def process_result_value( self, value, dialect ):
        if value is not None:
            value = pickle.loads(value)

        return value

class AlchemyDatabase( metl.database.basedatabase.BaseDatabase ):

    TYPES = {
        'BooleanFieldType': sqlalchemy.types.Boolean,
        'ComplexFieldType': JSONType,
        'DateFieldType': sqlalchemy.types.Date,
        'DateTimeFieldType': sqlalchemy.types.DateTime,
        'FloatFieldType': sqlalchemy.types.Numeric,
        'IntegerFieldType': sqlalchemy.types.Integer,
        'BigIntegerFieldType': sqlalchemy.types.BigInteger,
        'ListFieldType': JSONType,
        'StringFieldType': sqlalchemy.types.Unicode,
        'TextFieldType': sqlalchemy.types.UnicodeText,
        'PickleFieldType': PickleType # sqlalchemy.types.PickleType works only with ORM :(
    }

    DEFAULT_LIMIT = {
        'StringFieldType': 255
    }

    # bool
    def isExistsTable( self ):

        return self.getTable( autoload = False ).exists()

    # sqlalchemy.Table
    def getTable( self, autoload, extend_existing = False ):
        
        return sqlalchemy.Table( 
            self.target.getTableName(), 
            self.metadata, 
            autoload = autoload, 
            schema = self.target.getSchemaName(), 
            extend_existing = extend_existing 
        )

    def getEmptyTable( self ):

        return sqlalchemy.Table( 
            self.target.getTableName(), 
            self.metadata, 
            schema = self.target.getSchemaName(), 
        )

    # sqlalchemy.Sequence
    def getSequence( self ):

        return sqlalchemy.schema.Sequence( 
            'seq_%s_%s' % ( 
                self.target.getTableName(), 
                self.target.getIDKeyName() 
            ), optional = True 
        )

    # sqlalchemy.Column
    def getPrimaryKeyColumn( self, sequence = None ):

        return sqlalchemy.schema.Column( 
            self.target.getIDKeyName() or 'id', 
            sqlalchemy.types.Integer, 
            sequence,
            primary_key = True
        )

    # bool
    def isPrimaryKey( self, field ):

        return self.target.getIDKeyName() is not None and not self.target.getAddIDKey() and field.getName() == self.target.getIDKeyName()

    # sqlalchemy.types.Column
    def getColumnForField( self, field ):

        return sqlalchemy.schema.Column( 
            field.getName(), 
            self.getColumnType( field.getType().getName(), field.getLimit() ),
            primary_key = self.isPrimaryKey( field )
        )

    # sqlalchemy.types
    def getColumnType( self, field_type, field_limit ):

        limit = field_limit or self.DEFAULT_LIMIT.get( field_type )
        if limit is None:
            return self.TYPES[ field_type ]()

        return self.TYPES[ field_type ]( limit )

    # void
    def addColumn( self, table, column ):

        table.append_column( column )

    # void
    def createTable( self, table ):

        table.create()

    # sqlalchemy.Table
    def getCreatedTable( self ):

        table = self.getEmptyTable()
        if self.target.getAddIDKey():
            self.addColumn(
                table,
                self.getPrimaryKeyColumn( self.getSequence() )
            )

        fs = self.target.getFieldSetPrototypeCopy()
        for field_name in fs.getFieldNames():
            self.addColumn(
                table,
                self.getColumnForField( fs.getField( field_name ) )
            )

        self.createTable( table )
        return table

    # sqlalchemy.insert
    def getInsertCommand( self ):

        return self.table.insert()

    # sqlalchemy.update
    def getUpdateCommand( self ):

        cols = [ getattr( self.table.c, field_name ) for field_name in self.target.getFieldSetPrototypeCopy().getKeyFieldList() ]
        if len( cols ) == 0:
            return
            
        col  = cols[0]
        for c in cols[1:]:
            col = col.concat( '-' )
            col = col.concat( c )

        return self.table.update().where( col == sqlalchemy.sql.expression.bindparam('metlkey') )

    # void
    def open( self ):

        engine, now_created = sqlalchemy.create_engine( self.target.getConnectionURL() ), False

        self.connection = engine.connect()
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.connection.engine

        self.table = None
        self.cursor = self.connection.connection.cursor()

        if self.target.isTable():
            if not self.isExistsTable() and self.target.isCreateTable():
                self.table = self.getCreatedTable()
                now_created   = True
            
            if not self.isExistsTable() and self.table is None:
                raise ResourceNotExistsError( 'Table %s does not exist!' % ( self.table ) )

            if self.isExistsTable() and self.target.isReplaceTable() and not now_created:
                self.table = self.getTable( autoload = False )
                self.table.drop( checkfirst = False )
                self.table = self.getCreatedTable()

            if self.isExistsTable() and not self.target.isReplaceTable() and self.target.isTruncateTable() and not now_created:
                self.table = self.getTable( autoload = False )
                self.table.delete().execute()
                self.table = self.getTable( autoload = True, extend_existing = True )

            if self.table is None:
                self.table = self.getTable( autoload = True, extend_existing = True )

            self.db_insert_command = self.getInsertCommand()
            self.db_update_command = self.getUpdateCommand()

            self.columns = self.target.getFieldSetPrototypeCopy().getFieldNames()
        
        self.afterOpen()

    # void
    def load( self, connection, metadata, table ):

        self.connection = connection
        self.metadata = metadata
        self.table = table
        self.cursor = self.connection.connection.cursor()
        self.columns = [ str( column.name ) for column in table.columns ]

        self.db_insert_command = self.getInsertCommand()
        self.db_update_command = None

        self.afterOpen()

    # void
    def afterOpen( self ):

        pass

    # list<unicode>
    def getColumnNames( self ):

        return self.columns

    # void
    def iterateInsert( self, buffer ):

        for b in buffer:
            try:
                self.connection.execute( self.db_insert_command, [b] )
            except:
                print 'Error in buffer element', b
                pass

    # void
    def close( self ):

        self.connection.close()


    # void
    def _insert( self, buffer ):

        self.connection.execute( self.db_insert_command, buffer )

    # void
    def insert( self, buffer ):

        self._insert( buffer )

    # void
    def _update( self, buffer ):

        self.connection.execute( self.db_update_command, buffer )

    # void
    def update( self, buffer ):

        self._update( buffer )

