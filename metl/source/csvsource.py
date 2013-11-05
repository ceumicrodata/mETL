
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

import metl.source.base, csv, codecs, cStringIO

class UTF8Recoder:

    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeReader:

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.reader(f, dialect=dialect, **kwds)

    def next(self):
        row = self.reader.next()
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

class CSVSource( metl.source.base.FileSource ):

    init = ['delimiter','quote','skipRows','headerRow']

    # void
    def __init__( self, fieldset, delimiter = ',', quote = '"', skipRows = 0, headerRow = None, **kwargs ):

        self.delimiter  = delimiter
        self.quote      = quote
        self.headerRow  = headerRow
        self.headers    = {}

        super( CSVSource, self ).__init__( fieldset, **kwargs )
        self.setOffsetNumber( skipRows )

    # CSVSource
    def clone( self ):

        return self.__class__(
            self.fieldset.clone(),
            delimiter = self.delimiter,
            quote = self.quote,
            skipRows = self.offset,
            headerRow = self.headerRow
        )

    # void
    def initialize( self ):

        self.file_pointer, self.file_closable = metl.source.base.openResource( 
            self.getResource(), 
            'rb',
            username = self.htaccess_username,
            password = self.htaccess_password,
            realm = self.htaccess_realm,
            host = self.htaccess_host
        )
        csv_params = {
            'delimiter': self.delimiter,
            'encoding': self.getEncoding()
        }
        if self.quote is not None and len( self.quote ):
            csv_params['quotechar'] = self.quote
        else:
            csv_params['quoting'] = csv.QUOTE_NONE

        self.file_reader  = UnicodeReader( 
            self.file_pointer, 
            ** csv_params
        )
        return self.base_initialize()

    # void
    def invalidOffsetRecord( self, record ):

        if self.headerRow is None:
            return

        if self.current == int( self.headerRow ):
            self.headers = record

    # list
    def getRecordsList( self ):

        return self.file_reader

    # FieldSet
    def getTransformedRecord( self, record ):

        if self.headerRow is None:
            return record

        else:
            return dict( zip( self.headers, record ) )
