
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

import codecs, os, metl.configparser

class Base( object ):

    logFile = None
    appendLog = False
    log = None
    logger = None

    # StreamIO
    def getLogFilePointer( self ):

        return self.log_file_pointer

    # void
    def base_initialize( self ):

        if self.logFile is not None and not self.appendLog:
            self.log_file_pointer = codecs.open( self.logFile, 'w', 'utf-8' )
            self.log = self.logger or self.logActive

        elif self.logFile is not None and self.appendLog:
            self.log_file_pointer = codecs.open( self.logFile, 'a', 'utf-8' )
            self.log = self.logger or self.logActive

        else:
            self.log = self.logInactive

        return self

    # void
    def base_finalize( self ):

        if self.logFile is not None:
            self.log_file_pointer.close()

        return self

    # void
    def initialize( self ):

        return self.base_initialize()

    # void
    def finalize( self ):

        return self.base_finalize()

    # void
    def setLogFile( self, logFile = None, appendLog = False, logger = None ):

        self.logFile = os.path.abspath( logFile ) if logFile is not None else None 
        self.appendLog = appendLog
        self.logger = metl.configparser.lookupClass( logger ) \
            if logger is not None \
            else None

    # void
    def logInactive( self, msg, *args, **kwargs ):

        pass

    # void
    def logActive( self, msg, *args, **kwargs ):

        self.log_file_pointer.write( '%s\n' % ( msg ) )


