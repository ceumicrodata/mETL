
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

import traceback, sys, metl.field, metl.fieldset, metl.fieldmap, copy, \
    tarr.compiler_base, metl.transform.base, inspect
from metl.exception import *

# class
def lookupClass( name ):

    dotposition = name.rfind('.')
    moduleName  = name[:dotposition]
    className   = name[dotposition+1:]
    mod         = __import__(moduleName, {}, {}, [''])

    return getattr( mod, className )

# dict
def getMore( d, keys, only = False ):

    nd, rd = copy.deepcopy( d ), {}
    for k,v in nd.items():
        if only and k in keys:
            rd[ k ] = v

        elif k not in keys and not only:
            rd[ k ] = v

    return rd

class ConfigParser( object ):

    def __init__( self, config, debug = False, offset = None, limit = None, source_resource = None, init_on_start = True ):

        self.config          = config
        self.target          = None
        self.debug           = debug
        self.offset          = offset or 0
        self.limit           = limit
        self.source_resource = source_resource

        if init_on_start:
            self.init()

    def init( self ):

        self.loadSource()
        self.loadManipulations()
        self.loadTarget()

    # cls
    def lookupClass( self, name, nametype ):

        inner_path = 'metl.%(nametypelower)s.%(namelower)s%(nametypelower)s.%(name)s%(nametype)s' % {
            'nametype': nametype,
            'nametypelower': nametype.lower(),
            'name': name,
            'namelower': name.lower()
        }

        try:
            return lookupClass( inner_path )
        except:
            try:
                return lookupClass( name )
            except Exception as inst:

                if self.debug:
                    traceback.print_exc()

                raise ClassNotFoundException(
                    'Not found class: %s or %s' % ( inner_path, name )
                )

    # str
    def getParameterErrorMessage( self, m, entry, params ):

        s = 'Usage of %s\n' % ( m.__name__ ) 
        s += 2*' ' + self.getParameterList( m, entry ) + '\n\n'
        s += 'Given:\n' + '\n'.join( [ ( 4*' ' + '%s: %s' % ( str(k), str(v) ) ) for k,v in params.items() ] )

        return s

    # str
    def getParameterList( self, m, entry ):
        
        function  = getattr( m, entry )
        inspected = inspect.getargspec( function )
        defaults  = list( inspected.defaults ) if inspected.defaults is not None else []
        arguments = inspected.args[1:]
        params, i = [], 0
        
        if len( arguments ) == 0:
            return ''
            
        arguments.reverse()
        
        for argument in arguments:
            if len( defaults ) != 0:
                params.append( '<%s = %s>' % ( argument, str( defaults[0] ) ) )
                del defaults[0]
                continue
                
            params.append( '<%s>' % ( argument ) )

        params.reverse()

        if inspected.varargs != None:
            params.append( '*args' )

        if inspected.keywords != None:
            params.append( '**kwargs' )

        return ', '.join( params )

    # list<Reader>
    def getReaders( self ):

        return self.readers

    # Source
    def getLastReader( self ):

        return self.getReaders()[-1]

    # Target
    def getTarget( self ):

        return self.target

    # void
    def loadSource( self ):

        if 'source' not in self.config:
            raise ConfigError( 'Config must have source attribute' )

        self.readers = [ self.loadSourceObject( self.config.get('source'), base = True ) ]
        if self.offset is not None:
            self.readers[0].setOffsetNumber( int( self.offset ), True )

        if self.limit is not None:
            self.readers[0].setLimitNumber( int( self.limit ) )

    # Source
    def loadSourceObject( self, cfg, base = False ):

        fs, m, defaultValues = metl.fieldset.FieldSet(), cfg.get( 'map', {} ), cfg.get( 'defaultValues', {} )
        for fcfg in cfg.get( 'fields', [] ):
            
            field_params = getMore( fcfg, ['name','type','map','finalType'] )
            if fcfg['name'] not in defaultValues.keys() and 'defaultValue' in field_params:
                defaultValues[ fcfg['name'] ] = field_params['defaultValue']

            if 'map' in fcfg:
                m.setdefault( fcfg['name'], fcfg['map'] )

            if fcfg['name'] not in defaultValues.keys() and m.get( fcfg['name'] ) is None:
                m[ fcfg['name'] ] = fcfg['name']

            if fcfg['name'] in defaultValues.keys():
                field_params['defaultValue'] = defaultValues[ fcfg['name'] ]

            finalType = self.lookupClass( fcfg['finalType'], 'FieldType' )() \
                if fcfg.get('finalType') is not None \
                else None

            f = metl.field.Field( 
                fcfg['name'], 
                self.lookupClass( fcfg.get('type','String'), 'FieldType' )(),
                field_final_type = finalType,
                **field_params
            )
            transforms = self.loadTransforms( fcfg.get( 'transforms', [] ) )
            f.setTransforms( transforms )
            fs.addField( f )

        fs.setFieldMap( metl.fieldmap.FieldMap( m ) )
        source_cls = self.lookupClass( cfg['source'], 'Source' )
        source = source_cls(
            fs,
            **getMore( cfg, source_cls.init, only = True )
        )

        resource_params = getMore( cfg, source_cls.resource_init, only = True )
        if self.source_resource is not None and base:
            resource_params['resource'] = self.source_resource

        source.setResource( **resource_params )
        source.setLogFile( **getMore( cfg, ['logFile','appendLog','logger'], only = True ) )

        return source

    # void
    def getObject( self, transformType, transformConfig ):

        obj = self.lookupClass( transformConfig[transformType.lower()], transformType )
        if transformType in ( 'Condition', 'Transform' ) and inspect.isclass( obj ):
            params = getMore( transformConfig, obj.init, only = True )
            if 'source' in transformConfig:
                source_obj = self.loadSourceObject( transformConfig )
                source_obj.initialize()
                params['sourceRecords'] = [ r for r in source_obj.getRecords() ]
                source_obj.finalize()
            try:
                obj = obj( **params )
            except Exception as inst:
                if self.debug:
                    traceback.print_exc()

                raise ParameterError( self.getParameterErrorMessage( obj, '__init__', params ) )

        if transformType == 'Statement' and 'condition' in transformConfig:
            condition_obj = self.getObject( 
                'Condition', 
                transformConfig 
            )

            try:
                obj = obj( branch_instruction = condition_obj )
            except Exception as inst:
                if self.debug:
                    traceback.print_exc()

                raise ParameterError( self.getParameterErrorMessage( obj, '__init__', { 
                    'branch_instruction': condition_obj 
                } ) )

        if transformType in ( 'Filter', 'Expand', 'Modifier', 'Aggregator' ):
            if not obj.use_args:
                params = getMore( transformConfig, obj.init, only = True )
            else:
                params = transformConfig
                
            if 'condition' in transformConfig:
                params['condition'] = self.getObject( 
                    'Condition', 
                    transformConfig 
                )
            if 'source' in transformConfig:
                params['source'] = self.loadSourceObject( transformConfig )

            if 'transforms' in transformConfig:
                params['transforms'] = self.loadTransforms( params['transforms'] )

            try:
                obj = obj( 
                    self.getLastReader(),
                    **params 
                )
                obj.setLogFile( **getMore( transformConfig, ['logFile','appendLog','logger'], only = True ) )
            except Exception as inst:
                if self.debug:
                    traceback.print_exc()
                    
                raise ParameterError( self.getParameterErrorMessage( obj, '__init__', params ) )

        return obj

    # void
    def loadManipulations( self ):

        if 'manipulations' not in self.config:
            return

        for cfg in self.config['manipulations']:
            if 'filter' in cfg:
                self.readers.append( self.getObject( 'Filter', cfg ) )
            if 'expand' in cfg:
                self.readers.append( self.getObject( 'Expand', cfg ) )
            if 'modifier' in cfg:
                self.readers.append( self.getObject( 'Modifier', cfg ) )
            if 'aggregator' in cfg:
                self.readers.append( self.getObject( 'Aggregator', cfg ) )

    # list<dict>
    def getFlatTransformConfig( self, cfg ):

        fcfg = []
        for tcfg in cfg:
            fcfg.append( tcfg )
            if 'then' in tcfg and len( tcfg['then'] ) > 0:
                fcfg += self.getFlatTransformConfig( tcfg['then'] )

        return fcfg

    # void
    def loadTransforms( self, transforms_config ):

        transforms = []
        flats = self.getFlatTransformConfig( transforms_config )
        for transform_config in flats:
            tType = None
            if 'transform' in transform_config:
                tType = 'Transform'
            if 'statement' in transform_config:
                tType = 'Statement'

            if tType is None:
                raise ConfigError( 'Transform must have transform or statement attribute' )

            transforms.append( self.getObject( tType, transform_config ) )

        return transforms

    # void
    def loadTarget( self ):

        if 'target' not in self.config:
            raise ConfigError( 'Config must have target attribute' )

        target_config = self.config['target']

        if 'type' not in target_config:
            raise ConfigError( 'Target must have type attribute' )

        target_cls = self.lookupClass( target_config['type'], 'Target' )
        self.target = target_cls( 
            self.getLastReader(), 
            **getMore( target_config, target_cls.init, only = True ) 
        )
        self.target.setResource( **getMore( target_config, target_cls.resource_init, only = True ) )
        self.target.setLogFile( **getMore( target_config, ['logFile','logger'], only = True ) )

