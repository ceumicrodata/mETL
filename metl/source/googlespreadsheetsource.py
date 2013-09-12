
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

import metl.source.base, gdata.spreadsheet.text_db, gdata.spreadsheet.service

class GoogleSpreadsheetSource( metl.source.base.Source ):

    resource_init = ['username','password','spreadsheetKey','spreadsheetName','worksheetId','worksheetName']

    # void
    def __init__( self, fieldset, **kwargs ):

        self.spreadsheetKey  = None
        self.spreadsheetName = None
        self.worksheetId     = None
        self.worksheetName   = None
        self.username        = None
        self.password        = None
        self.client          = None
        self.worksheet       = None
        self.public_auth     = None
        self.res_dict        = None

        super( GoogleSpreadsheetSource, self ).__init__( fieldset, **kwargs )

    # void
    def setResource( self, username = None, password = None, spreadsheetKey = None, spreadsheetName = None, worksheetId = None, worksheetName = None ):

        self.spreadsheetKey  = spreadsheetKey
        self.spreadsheetName = spreadsheetName
        self.worksheetId     = worksheetId
        self.worksheetName   = worksheetName
        self.username        = username
        self.password        = password
        self.client          = None
        self.public_auth     = username is None or password is None

        res_dict = {
            'username': username,
            'password': password,
            'spreadsheetKey': spreadsheetKey,
            'spreadsheetName': spreadsheetName,
            'worksheetId': worksheetId,
            'worksheetName': worksheetName
        }

        self.updateResourceDict( res_dict )

        return self

    def updateResourceDict( self, res_dict ):

        super( GoogleSpreadsheetSource, self ).updateResourceDict( res_dict )

        self.spreadsheetKey  = self.res_dict['spreadsheetKey']
        self.spreadsheetName = self.res_dict['spreadsheetName']
        self.worksheetId     = self.res_dict['worksheetId']
        self.worksheetName   = self.res_dict['worksheetName']
        self.username        = self.res_dict['username']
        self.password        = self.res_dict['password']
        self.client          = None
        self.public_auth     = self.res_dict['username'] is None or self.res_dict['password'] is None

    # void
    def initialize( self ):

        if not self.public_auth:
            self.client = gdata.spreadsheet.text_db.DatabaseClient(
                username = self.username, 
                password = self.password
            )

            databases = self.client.GetDatabases(
                spreadsheet_key = self.spreadsheetKey,
                name = self.spreadsheetName
            )

            if len( databases ) == 0:
                raise ValueError( "Spreadsheet does not exist with key '%s' or name '%s'" % ( self.spreadsheetKey, self.spreadsheetKey ) )

            database = databases[0]

            worksheets = database.GetTables(
                worksheet_id = self.worksheetId,
                name = self.worksheetName
            )

            self.worksheet = worksheets[0]
            self.worksheet.LookupFields()

        else:
            self.client = gdata.spreadsheet.service.SpreadsheetsService()

            try:
                feed = self.client.GetWorksheetsFeed( self.spreadsheetKey, visibility = 'public', projection = 'basic' )
            except:
                raise ValueError( "Spreadsheet requires Authorization or does not exist with key '%s'!" % ( self.spreadsheetKey ) )

            for sheet in feed.entry:
                if self.worksheetName is None:
                    self.worksheet = sheet
                    break

                elif sheet.title.text == self.worksheetName:
                    self.worksheet = sheet
                    break

            if self.worksheet is None:
                raise ValueError( "Worksheet is not found!" )

        return super( GoogleSpreadsheetSource, self ).initialize()

    # list
    def getRecordsList( self ):

        if not self.public_auth:
            return self.worksheet.FindRecords("").__iter__()

        else:
            feed = self.client.GetListFeed( self.spreadsheetKey, visibility = 'public', projection = 'basic' )
            fs = self.getFieldSetPrototypeCopy()
            first_field_name = fs.getFieldNames()[0]
            title_field_name = fs.getFieldMap().getRules()[ first_field_name ]

            records = []
            for row_entry in feed.entry:
                try:
                    entries = row_entry.content.text.decode('utf-8').split(u', ')
                    field_names  = [ e.split(u': ')[0].strip() for e in entries ]
                    field_values = [ e.split(u': ')[1].strip() for e in entries ]
                    content = dict( zip( field_names, field_values ) )
                    content[ title_field_name ] = row_entry.title.text

                    record = gdata.spreadsheet.text_db.Record( content = content )
                except:
                    record = gdata.spreadsheet.text_db.Record( row_entry = row_entry )

                records.append( record )

            return records

    # FieldSet
    def getTransformedRecord( self, record ):

        return dict( record.content )
