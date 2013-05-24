
# -*- coding: utf-8 -*-

"""
mETLapp is a Python tool for do ETL processes with easy config.
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

import metl.filter.base

class DropBySourceFilter( metl.filter.base.Filter ):

    init = ['source','condition','join','operation']

    # void
    def __init__( self, reader, source, condition, join, operation = 'AND', *args, **kwargs ):

        self.condition = condition
        self.source    = source
        self.operation = operation
        self.join      = join
        self.records   = {}
        self.source_fieldset = source.getFieldSetPrototypeCopy( final = True )
        self.source_fields = list( set( self.source_fieldset.getFieldNames() ) - set([ self.join ]) )
        self.mapping   = {}

        for key in self.condition.init:
            self.mapping[ key ] = getattr( self.condition, key )

        super( DropBySourceFilter, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        self.source.initialize()
        for record in self.source.getRecords():
            self.records[ self.getJoinValue( record ) ] = record

        return super( DropBySourceFilter, self ).initialize()

    # type
    def getJoinValue( self, record ):

        return record.getField( self.join ).getValue()

    # void
    def finalize( self ):

        self.source.finalize()
        return super( DropBySourceFilter, self ).finalize()

    # bool
    def isANDOperation( self ):

        return self.operation.upper() == 'AND'

    # bool
    def isOROperation( self ):

        return self.operation.upper() == 'OR'

    # void
    def refreshCondition( self, record ):

        for setter, field_name in self.mapping.items():
            if self.getJoinValue( record ) not in self.records:
                return False

            setattr(
                self.condition,
                setter,
                self.records[ self.getJoinValue( record ) ].getField( field_name ).getValue()
            )

        return True

    # bool
    def isFiltered( self, record ):

        for field_name, field in record.getFields().items():
            if field_name not in self.source_fields:
                continue

            if not self.refreshCondition( record ):
                return True

            result = self.condition.getResult( field )
            if result and self.isOROperation():
                return True

            elif not result and self.isANDOperation():
                return False

        return self.isANDOperation()
