
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

import metl.database.alchemydatabase, sqlalchemy.schema
from sqlalchemy.types import *
from metl.target.csvtarget import *
from StringIO import StringIO

class PostgresqlDatabase( metl.database.alchemydatabase.AlchemyDatabase ):

    # void
    def afterOpen( self ):

        if not self.target.isTable():
            return

        naming_cond = [ col.name for col in self.table.c if col.name in map( str.lower, self.getColumnNames() ) ]

        if hasattr( self.target, 'getFieldSetPrototypeCopy' ):
            type_cond = [ name \
                for name, field in self.target.getFieldSetPrototypeCopy().getFields().items() \
                if field.getType().getName() in [ 'ListFieldType', 'ComplexFieldType', 'PickleFieldType' ] ]

        else:
            type_cond = type_cond = [ column.name \
                for column in self.table.columns \
                if isinstance( column.type, LargeBinary ) ]

        if len( naming_cond ) == len( self.getColumnNames() ) and len( type_cond ) == 0:
            self.insert = self.alternateInsert
        else:
            self.insert = self._insert

    # sqlalchemy.types.Column
    def getColumnForField( self, field ):

        return sqlalchemy.schema.Column( 
            field.getName().lower(), 
            self.getColumnType( field.getType().getName(), field.getLimit() ),
            primary_key = self.isPrimaryKey( field )
        )

    # list
    def getListRecord( self, buffer_item ):

        return ( unicode( buffer_item.get( column_name ) ) for column_name in self.getColumnNames() )

    # StringIO
    def getBufferContent( self, buffer ):

        pointer = StringIO()
        writer = UnicodeWriter( 
            pointer, 
            delimiter = ',', 
            quotechar = '`',
            encoding = 'utf-8'
        )

        for b in buffer:
            writer.writerow( self.getListRecord( b ) )

        pointer.seek(0)
        return pointer

    # void
    def alternateInsert( self, buffer ):

        self.cursor.copy_expert(
            "COPY \"%(table)s\" (%(columns)s) FROM STDIN WITH CSV NULL 'None' QUOTE '`' ESCAPE '`' DELIMITER ','" % {
                'table': self.target.getTableName(),
                'columns': u', '.join([ '"%s"' % ( c ) for c in self.getColumnNames() ])
            },
            self.getBufferContent( buffer )
        )

        self.connection.connection.commit()

