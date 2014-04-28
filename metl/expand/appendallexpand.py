
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

import metl.expand.base, metl.expand.appendexpand, os

class AppendAllExpand( metl.expand.base.Expand ):

    init = ['folder', 'extension', 'skipIfFails', 'skipSubfolders']

    # void
    def __init__( self, reader, folder, extension = None, skipIfFails = False, skipSubfolders = False, *args, **kwargs ):

        self.folder = folder
        self.skipIfFails = skipIfFails
        self.skipSubfolders = skipSubfolders
        self.extension = extension.lower() \
            if extension is not None \
            else None

        super( AppendAllExpand, self ).__init__( reader, *args, **kwargs )

    # unicode
    def getFirstResource( self ):

        success = False
        current_reader = self.getReader()
        while not success:
            if hasattr( current_reader, 'getReader' ):
                current_reader = current_reader.getReader()
            else:
                success = True

        return current_reader.resource

    # void
    def initialize( self ):

        if not os.path.exists( self.folder ):
            raise AttributeError('Not existins directory')

        bfolder = os.path.abspath( self.folder )
        self.files = []
        for directory, dirnames, filenames in os.walk( self.folder ):
            for filename in filenames:
                if self.extension is None or ( self.extension is not None and filename.lower().endswith( self.extension ) ):
                    full_path = os.path.abspath( os.path.join( directory, filename ) )
                    if full_path == self.getFirstResource():
                        continue

                    if self.skipSubfolders and bfolder != os.path.abspath( directory ):
                        continue

                    self.files.append( full_path )

        return super( AppendAllExpand, self ).initialize()

    # list<unicode>
    def getResources( self ):

        return self.files

    # AppendExpand
    def getAppendExpander( self, resource ):

        return metl.expand.appendexpand.AppendExpand(
            reader = self.reader,
            resource = resource,
            skipIfFails = self.skipIfFails
        )

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getReader().getRecords():
            yield record

        for resource in self.getResources():
            e = self.getAppendExpander( resource )
            e.initialize()
            for record in e.getNewRecords():
                yield record

