
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

import metl.writer, metl.source.base, pickle, os, codecs, demjson, metl.migration
from metl.exception import *

class Target( metl.writer.Writer ):

    init = []
    resource_init = []
    
    # void
    def __init__( self, reader, *args, **kwargs ):

        self.migration  = None
        self.tmigration = None
        self.fieldset   = {}

        super( Target, self ).__init__( reader, *args, **kwargs )

    # unicode
    def getMigrationType( self ):
        if len( self.getFieldSetPrototypeCopy().getKeyFieldList() ) == 0:
            return list

        return dict

    # FieldSet
    def getFieldSetPrototypeCopy( self, final = True ):

        self.fieldset.setdefault( str(final), self.reader.getFieldSetPrototypeCopy( final = final ) )
        return self.fieldset[ str(final) ]

    # void
    def setTargetMigrationResource( self, filepath ):

        self.tmigration = metl.migration.Migration( 
            filepath, 
            metl.migration.Migration.MODE_TARGET, 
            self.getMigrationType() 
        )
        return self

    # bool
    def hasTargetMigration( self ):

        return self.tmigration is not None

    # void
    def setMigrationResource( self, filepath ):

        self.migration = metl.migration.Migration( 
            filepath
        )
        return self

    # bool
    def hasMigration( self ):

        return self.migration is not None

    # list<unicode>
    def getMigrationData( self ):

        return self.migration.getMigrationData()

    # void
    def _initialize( self ):

        self.reader.initialize()

        if self.hasMigration():
            self.migration.initialize()

        if self.hasTargetMigration():
            self.tmigration.initialize()

        return super( Target, self ).initialize()

    # void
    def _finalize( self ):

        if self.hasMigration():
            self.migration.finalize()

        if self.hasTargetMigration():
            self.tmigration.finalize()

        self.reader.finalize()

        return super( Target, self ).finalize()

    # void
    def initialize( self ):

        return self._initialize()

    # void
    def finalize( self ):

        return self._finalize()

    # void
    def setResource( self ):

        raise RuntimeError( 'Target.setResource() is not implemented yet!' )

    # void
    def updateRecord( self, record, record_key ):

        self.writeRecord( record )

    # void
    def writeRecord( self, record, record_hash ):

        raise RuntimeError( 'Target.writeRecord() is not implemented yet!' )

    # list<FieldSet>
    def write( self ):

        for record in self.getRecords():

            if self.hasMigration():
                status = self.migration.getRecordStatus( record )
                if not status['exists']:
                    self.writeRecord( record )
                    self.log( record, 'write' )

                elif status['exists'] and status['modified']:
                    self.updateRecord( record, record.getKey() )
                    self.log( record, 'update' )

                elif status['exists'] and not status['modified']:
                    pass

            else:
                self.writeRecord( record )
                self.log( record, 'write' )

            if self.hasTargetMigration():
                self.tmigration.addRecord( record )

    # void
    def logActive( self, record, msgtype ):

        self.log_file_pointer.write( '%s (%s): %s\n' % ( record.getID(), msgtype.upper(), demjson.encode( record.getValues( class_to_string = True ) ) ) )


class FileTarget( Target ):

    resource_init = ['resource','encoding']

    def __init__( self, reader, *args, **kwargs ):

        self.resource      = None
        self.encoding      = None
        self.file_pointer  = None
        self.file_closable = None

        super( FileTarget, self ).__init__( reader, *args, **kwargs )

    # void
    def initialize( self ):

        super( FileTarget, self ).initialize()
        self.file_pointer, self.file_closable = metl.source.base.openResource( self.getResource(), 'w', self.getEncoding() )
        return self

    # void
    def finalize( self ):

        super( FileTarget, self ).finalize()
        if self.file_closable:
            self.file_pointer.close()
        return self

    # void
    def setResource( self, resource, encoding = 'utf-8' ):

        self.resource = os.path.abspath( resource ) \
            if os.path.exists( os.path.abspath( resource ) ) \
            else resource

        self.encoding = encoding
        return self

    # unicode
    def getResource( self ):

        return self.resource

    # unicode
    def getEncoding( self ):

        return self.encoding
