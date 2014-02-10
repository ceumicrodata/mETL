
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

import metl.target.base, json, re, random
from metl.exception import *
from py2neo import cypher

class Neo4jTarget( metl.target.base.Target ):

    init = ['bufferSize']
    resource_init = ['url','label','resourceType','truncateLabel','fieldNameLeft','fieldNameRight','keyNameLeft','keyNameRight','labelLeft','labelRight']

    # void
    def __init__( self, reader, bufferSize = None, *argw, **kwargs ):

        self.session = None
        self.bufferSize = bufferSize or int( random.random()*10000+5000 )
        self.db_insert_buffer = []
        self.db_update_buffer = []

        super( Neo4jTarget, self ).__init__( reader, *argw, **kwargs )

    # void
    def setResource( self, url, label, resourceType, truncateLabel = False, \
            fieldNameLeft = None, fieldNameRight = None, labelLeft = None, labelRight = None, keyNameLeft = None, keyNameRight = None ):

        if resourceType.lower() not in ('node','relation'):
            raise ParameterError('resourceType must be node or relation')

        self.url = url
        self.label = label
        self.resourceType = resourceType.lower()
        self.truncateLabel = truncateLabel
        self.fieldNameLeft = fieldNameLeft
        self.fieldNameRight = fieldNameRight
        self.labelLeft = labelLeft
        self.labelRight = labelRight
        self.keyNameLeft = keyNameLeft
        self.keyNameRight = keyNameRight

        if resourceType.lower() == 'relation' and ( self.fieldNameLeft is None or self.fieldNameRight is None or self.keyNameLeft is None or self.keyNameRight is None ):
            raise ParameterError('fieldNameLeft, fieldNameRight, keyNameLeft, keyNameRight are required when using Relation resource type')

        self.execute = self.executeNode if resourceType.lower() == 'node' else self.executeRelation

        return self

    # void
    def initialize( self ):

        self.session = cypher.Session( self.url )
        self.regexp  = re.compile( ur'\"([^"]*?)\"\:' )

        if self.truncateLabel and self.resourceType == 'node':
            tx = self.session.create_transaction()
            cmd1 = 'MATCH (n:`%s`)-[r]-() DELETE n, r' % ( self.label )
            tx.append( cmd1 )
            cmd2 = 'MATCH (c:`%s`) DELETE c' % ( self.label )
            tx.append( cmd2 )
            tx.commit()

        elif self.truncateLabel and self.resourceType == 'relation':
            tx = self.session.create_transaction()
            cmd = 'MATCH (a)-[r:`%s`]-(b) DELETE r' % ( self.label )
            tx.append( cmd )
            tx.commit()

        return super( Neo4jTarget, self ).initialize()

    # void
    def finalize( self ):

        self.execute()

        if self.resourceType == 'node':
            keys = self.getFieldSetPrototypeCopy().getKeyFieldList()
            if len( keys ) != 0:
                tx = self.session.create_transaction()
                for fieldName in keys:
                    tx.append(
                        """
                        CREATE INDEX ON :`%(label)s`(`%(field)s`)
                        """ % {
                            'label': self.label,
                            'field': fieldName
                        }
                    )
                tx.commit()

        return super( Neo4jTarget, self ).finalize()

    # unicode
    def dictToString( self, d, remove = None ):

        if remove is not None:
            for r in remove:
                try:
                    del d[ r ]
                except:
                    pass

        if len( d ) == 0:
            return u''

        json_str = json.dumps( d )
        json_str = self.regexp.sub( u'`\\1`:', json_str )
        return json_str

    # unicode
    def getCreateNodeCommand( self, b, key ):

        return 'CREATE ( `%(key)s`:`%(label)s` %(data)s )' % {
            'key': key,
            'label': self.label,
            'data': self.dictToString(b)
        }

    # unicode
    def getCreateRelationCommand( self, b, key ):

        return u'MATCH (a%(label_l)s), (b%(label_r)s) WHERE a.`%(key_l)s` = "%(val_l)s" AND b.`%(key_l)s` = "%(val_r)s" CREATE (a)-[:`%(label)s` %(data)s]->(b)' % {
            'label_l': u':`%s`' % ( self.labelLeft ) if self.labelLeft is not None else u'',
            'label_r': u':`%s`' % ( self.labelRight ) if self.labelRight is not None else u'',
            'label': self.label,
            'key_l': self.keyNameLeft,
            'key_r': self.keyNameRight,
            'val_l': b[ self.fieldNameLeft ],
            'val_r': b[ self.fieldNameRight ],
            'data': self.dictToString( b, [ self.fieldNameLeft, self.fieldNameRight ] )
        }

    # void
    def execute( self ):

        pass

    # void
    def executeNode( self ):

        tx = self.session.create_transaction()

        for b, key in self.db_insert_buffer:
            cmd = self.getCreateNodeCommand( b, key )
            tx.append( cmd )

        tx.commit()

        self.db_insert_buffer = []
        self.db_update_buffer = []

    # void
    def executeRelation( self ):

        tx = self.session.create_transaction()

        for b, key in self.db_insert_buffer:
            cmd = self.getCreateRelationCommand( b, key )
            tx.append( cmd )

        tx.commit()

        self.db_insert_buffer = []
        self.db_update_buffer = []

    # void
    def writeRecord( self, record ):

        self.db_insert_buffer.append( ( record.getValues( without_none = True, class_to_string = True ), record.getKey() ) )
        if len( self.db_insert_buffer ) + len( self.db_update_buffer ) >= self.bufferSize:
            self.execute()
