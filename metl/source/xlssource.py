
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

import metl.source.base, codecs, xlrd, datetime

class XLSSource( metl.source.base.FileSource ):

    init = ['skipRows']
    resource_init = ['resource','sheetName','encoding','username','password','realm','host']

    # void
    def __init__( self, fieldset, skipRows = 0, **kwargs ):

        self.workbook   = None
        self.sheet      = None
        self.rowCount   = None
        self.currentRow = None

        super( XLSSource, self ).__init__( fieldset, **kwargs )
        self.setOffsetNumber( skipRows )

    # void
    def setResource( self, resource, sheetName = None, encoding = 'utf-8' ):

        self.sheetName = sheetName or 0

        return super( XLSSource, self ).setResource( resource, encoding )

    # void
    def initialize( self ):

        self.file_pointer, self.file_closable = metl.source.base.openResource( 
            self.getResource(), 
            'r',
            username = self.htaccess_username,
            password = self.htaccess_password,
            realm = self.htaccess_realm,
            host = self.htaccess_host
        )
        self.workbook = xlrd.open_workbook(
            file_contents = self.file_pointer.read(),
            encoding_override = self.getEncoding()
        )

        self.sheet = self.workbook.sheet_by_index( self.sheetName ) \
            if type( self.sheetName ) == int \
            else self.workbook.sheet_by_name( self.sheetName )

        self.rowCount = self.sheet.nrows

        return self.base_initialize()

    # list
    def getRecordsList( self ):

        return xrange( self.rowCount )

    # FieldSet
    def getTransformedRecord( self, record ):

        return [ self.convertCellValue( cell ) for cell in self.sheet.row( self.current ) ]

    # type
    def convertCellValue( self, cell ):

        if cell.ctype == xlrd.XL_CELL_NUMBER:
            return float( cell.value )

        elif cell.ctype == xlrd.XL_CELL_DATE:
            args = xlrd.xldate_as_tuple( cell.value, self.workbook.datemode )

            try:
                return datetime.date( args[0], args[1], args[2] )
            except Exception, inst:
                return None

        elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
            return bool( cell.value )

        else:
            return cell.value
