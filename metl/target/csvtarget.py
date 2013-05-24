
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

import metl.target.base, csv, cStringIO, codecs

class UnicodeWriter:

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):

        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        data = self.encoder.encode(data)
        self.stream.write(data)
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

class CSVTarget( metl.target.base.FileTarget ):

    init = ['delimiter','quote','addHeader','appendFile']

    def __init__( self, reader, delimiter = ',', quote = '"', addHeader = True, appendFile = False, *args, **kwargs ):
        
        self.delimiter  = delimiter
        self.quote      = quote
        self.addHeader  = addHeader
        self.appendFile = appendFile

        super( CSVTarget, self ).__init__( reader, *args, **kwargs )

    # bool
    def isAppendMode( self ):

        return self.appendFile

    # void
    def initialize( self ):

        self._initialize()

        try:
            file_pointer, file_closable = metl.source.base.openResource( self.getResource(), 'r' )
            exists = True
        except:
            exists = False

        self.file_pointer, self.file_closable = metl.source.base.openResource( 
            self.getResource(), 
            'w' if not self.isAppendMode() else 'a' 
        )

        self.file_writer  = UnicodeWriter( 
            self.file_pointer, 
            delimiter = self.delimiter, 
            quotechar = self.quote,
            encoding = self.getEncoding()
        )

        if self.addHeader and not ( exists and self.isAppendMode() ):
            self.file_writer.writerow( self.getFieldSetPrototypeCopy().getFieldNames() )

        return self

    # void
    def writeRecord( self, record ):

        self.file_writer.writerow( record.getValuesList( to_string = True ) )
