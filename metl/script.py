
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

import optparse, metl.configparser, metl.manager, metl.config, sys, os, time, \
    metl.target.statictarget, metl.migration, metl.source.staticsource, \
    metl.fieldmap, metl.fieldset, metl.fieldtype.stringfieldtype, metl.guessmanager
from multiprocessing import Process

def main( argv = sys.argv ):

    parser = optparse.OptionParser(
        usage = 'Usage: %prog [options] CONFIG.YML'
    )
    
    parser.add_option(
        "-t", 
        "--targetMigration", 
        dest = "target_migration_file", 
        default = None,
        help = "During running, it prepares a migration file from the state of the present data."
    )

    parser.add_option(
        "-m",
        "--migration",
        dest = "migration_file",
        default = None,
        help = 'Conveyance of previous migration file that was part of the previously run version.'
    )

    parser.add_option(
        "-p",
        "--path",
        dest = "path",
        default = None,
        help = "Conveyance of a folder, which is added to the PATH variable in order that the link in the YAML configuration could be run on an outside python file."
    )

    parser.add_option(
        "-d",
        "--debug",
        action = 'store_true',
        default = False,
        help = "Debug mode, writes everything out as stdout."
    )

    parser.add_option(
        "-l",
        "--limit",
        help = "Determines the number of elements for processing."
    )

    parser.add_option(
        "-o",
        "--offset",
        default = 0,
        help = 'Determines from which element should the processing start.'
    )

    parser.add_option(
        "-s",
        "--source",
        help = "If the configuration does not contain the route of the resource, it could be given here as well."
    )

    (options, args) = parser.parse_args( argv[1:] )

    if len( args ) != 1:
        parser.print_help()
        sys.exit()

    if options.path is not None:
        absdirectory = os.path.abspath( options.path )
        sys.path.append( absdirectory )

    configparser = metl.configparser.ConfigParser( 
        metl.config.Config( args[0] ), 
        debug = options.debug, 
        limit = options.limit,
        offset = options.offset, 
        source_resource = options.source 
    )

    metl.manager.Manager( 
        configparser.getTarget(), 
        migration_resource = options.migration_file,
        target_migration_resource = options.target_migration_file
    ).run()

def metl_walk( argv = sys.argv ):

    def run( config, filename, debug, limit, offset, resource ):

        sys.stdout.write( filename + ' ... ' )
        sys.stdout.flush()

        configparser = metl.configparser.ConfigParser( 
            metl.config.Config( config ),
            debug = debug, 
            limit = limit,
            offset = offset, 
            source_resource = resource
        )

        metl.manager.Manager( 
            configparser.getTarget()
        ).run()

        sys.stdout.write( 'OK\n' )
        sys.stdout.flush()

    parser = optparse.OptionParser(
        usage = 'Usage: %prog [options] BASECONFIG.YML FOLDER'
    )
    
    parser.add_option(
        "-p",
        "--path",
        dest = "path",
        default = None,
        help = "Conveyance of a folder, which is added to the PATH variable in order that the link in the YAML configuration could be run on an outside python file."
    )

    parser.add_option(
        "-d",
        "--debug",
        action = 'store_true',
        default = False,
        help = "Debug mode, writes everything out as stdout."
    )

    parser.add_option(
        "-l",
        "--limit",
        help = "Determines the number of elements for processing."
    )

    parser.add_option(
        "-o",
        "--offset",
        default = 0,
        help = 'Determines from which element should the processing start.'
    )

    parser.add_option(
        "-m",
        "--multiprocessing",
        action = 'store_true',
        default = False,
        help = 'Walk the files with multiple process (use only for database target)'
    )

    (options, args) = parser.parse_args( argv[1:] )

    if len( args ) != 2:
        parser.print_help()
        sys.exit()

    if options.path is not None:
        absdirectory = os.path.abspath( options.path )
        sys.path.append( absdirectory )

    if not os.path.exists( args[1] ):
        parser.print_help()
        sys.exit()

    for ( path, dirs, files ) in os.walk( os.path.abspath( args[1] ) ):
        n = 0
        for f in files:
            if f.startswith( '.' ):
                continue

            if options.multiprocessing:
                p = Process( target = run, args=( args[0], f, options.debug, options.limit, options.offset, os.path.join( path, f ), ) )
                p.start()

                if n == 0:
                    time.sleep( 15 )

            else:
                run(
                    config = args[0],
                    filename = f,
                    debug = options.debug,
                    limit = options.limit,
                    offset = options.offset,
                    resource = os.path.join( path, f ) 
                )

            n += 1

def metl_transform( argv = sys.argv ):

    parser = optparse.OptionParser(
        usage = 'Usage: %prog [options] CONFIG.YML FIELD VALUE'
    )
    
    parser.add_option(
        "-p",
        "--path",
        dest = "path",
        default = None,
        help = "Conveyance of a folder, which is added to the PATH variable in order that the link in the YAML configuration could be run on an outside python file."
    )

    parser.add_option(
        "-d",
        "--debug",
        action = 'store_true',
        default = False,
        help = "Debug mode, writes everything out as stdout"
    )

    (options, args) = parser.parse_args( argv[1:] )

    if len( args ) != 3:
        parser.print_help()
        sys.exit()

    if options.path is not None:
        absdirectory = os.path.abspath( options.path )
        sys.path.append( absdirectory )

    configparser = metl.configparser.ConfigParser( metl.config.Config( args[0] ), options.debug )

    field = configparser.getReaders()[0].getFieldSetPrototypeCopy().getField( args[1] )
    field.setStdOutput()
    field.setValue( args[2] )
    field.run()
    print repr( field.getValue() )

def metl_differences( argv = sys.argv ):
    
    def write( filepath, records ):

        source = metl.source.staticsource.StaticSource(
            metl.fieldset.FieldSet(
                fields = [
                    metl.field.Field( 'key', metl.fieldtype.stringfieldtype.StringFieldType() )
                ],
                fieldmap = metl.fieldmap.FieldMap({
                    'key': 0
                })
            )
        )
        source.setResource( sourceRecords = [ [r] for r in records ] )

        configparser = metl.configparser.ConfigParser( metl.config.Config( filepath ), init_on_start = False )
        configparser.readers = [ source ]
        configparser.loadTarget()

        metl.manager.Manager( 
            configparser.getTarget()
        ).run()

    parser = optparse.OptionParser(
        usage = 'Usage: %prog [options] CURRENT_MIGRATION LAST_MIGRATION'
    )

    parser.add_option(
        "-p",
        "--path",
        dest = "path",
        default = None,
        help = "Conveyance of a folder, which is added to the PATH variable in order that the link in the YAML configuration could be run on an outside python file."
    )
    
    parser.add_option(
        '-d',
        '--deleted',
        dest = 'deleted',
        default = None,
        help = 'Save the deleted key list by this YAML configuration.'
    )
    parser.add_option(
        '-n',
        '--news',
        dest = 'news',
        default = None,
        help = 'Save the new key list by this YAML configuration.'
    )
    parser.add_option(
        '-m',
        '--modified',
        dest = 'modified',
        default = None,
        help = 'Save the modified key list by this YAML configuration.'
    )
    parser.add_option(
        '-u',
        '--unchanged',
        dest = 'unchanged',
        default = None,
        help = 'Save the unchanged key list by this YAML configuration.'
    )

    (options, args) = parser.parse_args( argv[1:] )

    if len( args ) != 2:
        parser.print_help()
        sys.exit()

    if options.path is not None:
        absdirectory = os.path.abspath( options.path )
        sys.path.append( absdirectory )

    new_migration = metl.migration.Migration( args[0] )
    new_migration.initialize()

    old_migration = metl.migration.Migration( args[1] )
    old_migration.initialize()

    news = new_migration.getNews( old_migration )
    updated = new_migration.getUpdated( old_migration )
    deleted = new_migration.getDeleted( old_migration )
    unchanged = new_migration.getUnchanged( old_migration )

    if options.deleted is not None:
        write( options.deleted, deleted )

    if options.news is not None:
        write( options.news, news )

    if options.modified is not None:
        write( options.modified, updated )

    if options.unchanged is not None:
        write( options.unchanged, unchanged )

    print 'New:\t\t%d' % ( len( news ) )
    print 'Updated:\t%d' % ( len( updated ) )
    print 'Unchanged:\t%d' % ( len( unchanged ) )
    print 'Deleted:\t%d' % ( len( deleted ) )

def metl_generate( argv = sys.argv ):

    try:
        mgr = metl.guessmanager.GuessManager( argv[-2].upper() )
    except:
        print 'Usage: metl-generate [options] SOURCE_TYPE CONFIG_FILE'
        print 'Supported SOURCE_TYPEs:'
        print '\n'.join(( ' - %s' % ( t.title() ) for t in metl.guessmanager.GuessManager.types.keys() ))
        sys.exit()

    parser = mgr.getParser()
    (options, args) = parser.parse_args( argv[1:] )

    if len([ v for v in vars( options ).values() if v is not None ]) == 1:
        parser.print_help()
        sys.exit()

    mgr.startGuess( vars( options ) )
    mgr.saveConfig( sys.argv[-1] )

def metl_aggregate( argv = sys.argv ):
    
    parser = optparse.OptionParser(
        usage = 'Usage: %prog [options] CONFIG.YML FIELD'
    )
    
    parser.add_option(
        "-p",
        "--path",
        dest = "path",
        default = None,
        help = "Conveyance of a folder, which is added to the PATH variable in order that the link in the YAML configuration could be run on an outside python file."
    )

    parser.add_option(
        "-d",
        "--debug",
        action = 'store_true',
        default = False,
        help = "Debug mode, writes everything out as stdout."
    )

    parser.add_option(
        "-l",
        "--limit",
        help = "Determines the number of elements for processing. An excellent option to test large files within small records until everything works as we would like to."
    )

    parser.add_option(
        "-o",
        "--offset",
        default = 0,
        help = 'Determines from which element should the processing start.'
    )

    parser.add_option(
        "-s",
        "--source",
        help = "If the configuration does not contain the route of the resource, it could be given here as well."
    )

    (options, args) = parser.parse_args( argv[1:] )

    if len( args ) != 2:
        parser.print_help()
        sys.exit()

    if options.path is not None:
        absdirectory = os.path.abspath( options.path )
        sys.path.append( absdirectory )

    configparser = metl.configparser.ConfigParser( 
        metl.config.Config( args[0] ), 
        debug = options.debug, 
        limit = options.limit,
        offset = options.offset, 
        source_resource = options.source 
    )

    target = metl.target.statictarget.StaticTarget( configparser.getTarget().getReader() )
    target.setResource( silence = True )
    target.initialize()
    target.write()
    target.finalize()

    values = set([])
    for record in target.getResults():
        values.add( record.getField( args[1] ).getValue() )

    for value in values:
        print value
