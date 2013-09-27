
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
from StringIO import StringIO

class PostgresqlDatabase( metl.database.alchemydatabase.AlchemyDatabase ):

    # void
    def afterOpen( self ):

        naming_cond = [ col.name for col in self.table.c if col.name in map( str.lower, self.getColumnNames() ) ]
        type_cond   = [ name for name, field in self.target.getFieldSetPrototypeCopy().getFields().items() if field.getType().getName() in [ 'ListFieldType', 'ComplexFieldType', 'PickleFieldType' ] ]
        if len( naming_cond ) == len( self.getColumnNames() ) and len( type_cond ) == 0:
            self.insert = self.alternateInsert

    # sqlalchemy.types.Column
    def getColumnForField( self, field ):

        return sqlalchemy.schema.Column( 
            field.getName().lower(), 
            self.getColumnType( field.getType().getName(), field.getLimit() ),
            primary_key = self.isPrimaryKey( field )
        )

    # unicode
    def getTabSeparatedRecord( self, buffer_item ):

        v = []
        for column_name in self.getColumnNames():
            v.append( unicode( buffer_item.get( column_name ) or '\N' ) )

        return u'\t'.join(v)

    # StringIO
    def getBufferContent( self, buffer ):

        return StringIO( u'\n'.join( self.getTabSeparatedRecord( b ) for b in buffer ) + u'\n' )

    # void
    def alternateInsert( self, buffer ):

        self.cursor.copy_from( 
            self.getBufferContent( buffer ), 
            self.target.getTableName(), 
            columns = map( str.lower, self.getColumnNames() )
        )

        self.connection.connection.commit()

