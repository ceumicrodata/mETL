
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

import codecs, optparse, yaml, metl.guess, metl.config
import metl.source.csvsource
import metl.source.databasesource 
import metl.source.googlespreadsheetsource 
import metl.source.jsonsource 
import metl.source.tsvsource
import metl.source.xlssource
import metl.source.xmlsource
import metl.source.yamlsource

class GuessManager( object ):

    types = {
        'CSV': metl.source.csvsource.CSVSource,
        'TSV': metl.source.tsvsource.TSVSource,
        'JSON': metl.source.jsonsource.JSONSource,
        'DATABASE': metl.source.databasesource.DatabaseSource,
        'XML': metl.source.xmlsource.XMLSource,
        'XLS': metl.source.xlssource.XLSSource,
        'YAML': metl.source.yamlsource.YamlSource,
        'GOOGLESPREADSHEET': metl.source.googlespreadsheetsource.GoogleSpreadsheetSource
    }

    def __init__( self, source_type ):

        if source_type not in self.types.keys():
            raise TypeError( 'Unknown or not supported Source Type!')

        self.source_class  = self.types[ source_type ]
        self.guess = None

    def getGuess( self, options ):

        init, resource_init, base = {}, {}, {}
        if options.get('base') is not None:
            cfg = metl.config.Config( options.get('base') )
            for v in self.getSourceClass().init:
                if cfg.get('source',{}).get( v ) is not None:
                    init[ v ] = cfg.get('source',{}).get( v )
            for v in self.getSourceClass().resource_init:
                if cfg.get('source',{}).get( v ) is not None:
                    resource_init[ v ] = cfg.get('source',{}).get( v )

            base = {
                'source': cfg.get('source',{}),
                'target': cfg.get('target',{}),
                'manipulations': cfg.get('manipulations',{}),
                'base': options.get('base')
            } 

        for v in self.getSourceClass().init:
            if options.get( v ) is not None:
                init[ v ] = options.get( v ) 
        for v in self.getSourceClass().resource_init:
            if options.get( v ) is not None:
                resource_init[ v ] = options.get( v ) 

        return metl.guess.Guess(
            self.source_class,
            init,
            resource_init,
            int(options.get('limit',1000)),
            base
        )

    def startGuess( self, option ):

        self.guess = self.getGuess( option )
        self.guess.guess()

        if self.guess.getFieldMap() is None and self.guess.getFields() is None:
            raise RuntimeError( 'Guess process is not successfull!')

    def saveConfig( self, filepath ):

        config = self.guess.getConfig()
        fp = codecs.open( filepath, 'w', 'utf-8' )
        fp.write( yaml.safe_dump( config, default_flow_style = False ) )
        fp.close()

    def getSourceClass( self ):

        return self.source_class

    def getParser( self ):

        parser = optparse.OptionParser(
            usage = 'Usage: %prog [options] CONFIG_FILE SOURCE_TYPE'
        )

        parser.add_option(
            '-b',
            '--base',
            dest = "base",
            default = None,
            help = 'Base configuration file'
        )

        parser.add_option(
            '-l',
            '--limit',
            default = 1000,
            help = 'Create the configuration file with examining LIMIT number of records.'
        )

        for v in self.getSourceClass().init + self.getSourceClass().resource_init:
            parser.add_option(
                "--" + v,
                dest = v,
                default = None
            )

        return parser
