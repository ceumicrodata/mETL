
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

import metl.target.base, gspread

class GoogleSpreadsheetTarget( metl.target.base.Target ):

    init = [ 'addHeader' ]
    resource_init = [ 'username', 'password', 'spreadsheetName', 'spreadsheetKey', 'worksheetName', 'truncateSheet' ]

    # void
    def __init__( self, reader, addHeader = True, *args, **kwargs ):
        
        self.addHeader = addHeader

        super( GoogleSpreadsheetTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def setResource( self, username, password, worksheetName, spreadsheetName = None, spreadsheetKey = None, truncateSheet = False):

        self.username = username
        self.password = password
        self.spreadsheetKey = spreadsheetKey
        self.spreadsheetName = spreadsheetName
        self.worksheetName = worksheetName
        self.truncateSheet = truncateSheet

        if self.spreadsheetKey is None and self.spreadsheetName is None:
            raise AttributeError('Missing spreadsheetKey or spreadsheetName attribute!') 

        return self

    # void
    def initialize( self ):

        self.gc = gspread.login(
            self.username, 
            self.password
        )

        if self.spreadsheetKey is not None:
            self.sheet = self.gc.open_by_key(
                self.spreadsheetKey
            )

        elif self.spreadsheetName is not None:
            self.sheet = self.open(
                self.spreadsheetName
            )

        if self.worksheetName not in [ w.title for w in self.sheet.worksheets() ]:
            self.worksheet = self.sheet.add_worksheet( 
                title = self.worksheetName, 
                rows = "1000", 
                cols = "20"
            )
        elif self.truncateSheet:
            self.sheet.del_worksheet( self.sheet.worksheet( self.worksheetName ) )
            self.worksheet = self.sheet.add_worksheet( 
                title = self.worksheetName, 
                rows = "1000", 
                cols = "20"
            )

        else:
            self.worksheet = self.sheet.worksheet( self.worksheetName )

        try:
            self.rowIndex = len( self.worksheet.get_all_values() )
        except:
            self.rowIndex = 0

        self.fieldNames = self.getFieldSetPrototypeCopy().getFieldNames()
        self.colNumber = len( self.fieldNames )

        if self.rowIndex == 0 and self.addHeader:
            for colIndex in xrange( self.colNumber ):
                self.worksheet.update_cell( 
                    self.rowIndex + 1,
                    colIndex + 1, 
                    self.fieldNames[ colIndex ] 
                )

            self.rowIndex += 1

        return super( GoogleSpreadsheetTarget, self ).initialize()

    # void
    def finalize( self ):

        return super( GoogleSpreadsheetTarget, self ).finalize()

    # void
    def writeRecord( self, record ):

        value = record.getValuesList()

        for colIndex in xrange( self.colNumber ):
            self.worksheet.update_cell( 
                self.rowIndex + 1, 
                colIndex + 1, 
                value[ colIndex ] 
            )
        
        self.rowIndex += 1

