
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
        self.res_dict   = None

        super( XLSSource, self ).__init__( fieldset, **kwargs )
        self.setOffsetNumber( skipRows )

    # XLSSource
    def clone( self ):

        return self.__class__(
            self.fieldset.clone(),
            skipRows = self.offset
        )

    # void
    def setResource( self, resource, sheetName = None, encoding = 'utf-8' ):

        res_dict = {
            'resource': resource,
            'sheetName': sheetName,
            'encoding': encoding
        }

        self.updateResourceDict( res_dict )

        return super( XLSSource, self ).setResource( resource, encoding )

    def updateResourceDict( self, res_dict ):

        super( XLSSource, self ).updateResourceDict( res_dict )

        self.resource = self.res_dict['resource']
        self.sheetName = self.res_dict['sheetName'] or 0

    # void
    def initialize( self ):

        super( XLSSource, self ).initialize()

        self.workbook = xlrd.open_workbook(
            file_contents = self.file_pointer.read(),
            encoding_override = self.getEncoding()
        )

        self.sheet = self.workbook.sheet_by_index( self.sheetName ) \
            if type( self.sheetName ) == int \
            else self.workbook.sheet_by_name( self.sheetName )

        self.rowCount = self.sheet.nrows

        return self.base_initialize()

    def getEncoding( self ):

        return None

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
