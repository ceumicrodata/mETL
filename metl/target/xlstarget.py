
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

import metl.target.base, xlwt, xlrd, metl.source.base, xlutils.copy, os

class XLSTarget( metl.target.base.FileTarget ):

    init = ['addHeader']
    resource_init = ['resource','sheetName','encoding','replaceFile','truncateSheet']
    
    # void
    def __init__( self, reader, addHeader = True, *args, **kwargs ):
        
        self.addHeader    = addHeader
        self.sheetName    = None
        self.workbook     = None
        self.old_workbook = None
        self.worksheet    = None
        self.colNumber    = None
        self.replace      = None
        self.truncate     = None

        super( XLSTarget, self ).__init__( reader, *args, **kwargs )

    # bool
    def isExistsSheet( self ):

        if self.old_workbook is None:
            return False

        try:
            return self.old_workbook.sheet_by_name( self.sheetName ) is not None
        except:
            return False

    # void
    def setResource( self, resource, sheetName, replaceFile = True, truncateSheet = True, encoding = 'utf-8' ):

        self.replaceFile    = replaceFile
        self.truncateSheet  = truncateSheet
        self.sheetName      = sheetName

        return super( XLSTarget, self ).setResource( resource, encoding )
        
    # void
    def initialize( self ):

        self._initialize()

        if not os.path.exists( self.getResource() ) or self.replaceFile:
            self.workbook     = xlwt.Workbook()
            self.old_workbook = None

        else:
            self.file_pointer, self.file_closable = metl.source.base.openResource( self.getResource(), 'r' )
            self.old_workbook = xlrd.open_workbook(
                file_contents = self.file_pointer.read(),
                encoding_override = self.getEncoding()
            )
            self.workbook = xlutils.copy.copy( self.old_workbook )

        self.rowIndex = self.old_workbook.sheet_by_name( self.sheetName ).nrows \
            if self.isExistsSheet() and not self.truncateSheet \
            else 0

        self.worksheet = self.workbook.add_sheet( self.sheetName ) \
            if not self.isExistsSheet() \
            else self.workbook.get_sheet( self.old_workbook.sheet_by_name( self.sheetName ).sheet_selected - 1 )

        fieldNames     = self.getFieldSetPrototypeCopy().getFieldNames()
        self.colNumber = len( fieldNames )

        if self.isExistsSheet() and self.truncateSheet:
            for colIndex in range( self.colNumber ):
                for rowIndex in range( self.old_workbook.sheet_by_name( self.sheetName ).nrows ):
                    self.worksheet.write( rowIndex, colIndex, None )

        if self.addHeader and self.rowIndex == 0:
            for colIndex in range( self.colNumber ):
                self.worksheet.write( self.rowIndex, colIndex, fieldNames[ colIndex ] )

            self.rowIndex += 1

        return self

    # void
    def finalize( self ):

        self._finalize()
        if self.old_workbook is not None and self.file_closable:
            self.file_pointer.close()

        self.workbook.save( self.getResource() )

    # void
    def writeRecord( self, record ):

        value = record.getValuesList()

        for colIndex in range( self.colNumber ):
            self.worksheet.write( self.rowIndex, colIndex, value[ colIndex ] )

        self.rowIndex += 1
