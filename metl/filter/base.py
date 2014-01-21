
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

import metl.manipulation, demjson

class Filter( metl.manipulation.Manipulation ):

    # bool
    def isFiltered( self, record ):

    	raise RuntimeError( 'Filter.isFiltered() function is not implemented yet.' ) 

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            filtered = self.isFiltered( record )
            if not filtered:
                yield record
            
            self.log( self, record, filtered )

    # void
    def logActive( self, ftr, record, is_filtered ):

        if not is_filtered:
            return

        self.log_file_pointer.write( '%s: %s\n' % ( record.getID(), demjson.encode( record.getValues( class_to_string = True ) ) ) )
