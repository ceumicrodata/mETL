
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

import metl.reader, os, copy, urlparse, codecs, urllib2, demjson, httplib

def patch_http_response_read( func ):
    def inner( *args ):
        try:
            return func(*args)

        except httplib.IncompleteRead, e:
            return e.partial

    return inner

httplib.HTTPResponse.read = patch_http_response_read( httplib.HTTPResponse.read )

def openResource( resource, mode, encoding = None, realm = None, host = None, username = None, password = None ):

    if type( resource ) in ( str, unicode ):
        file_closable = True

        parts = urlparse.urlparse( resource )

        if parts.scheme in ( '', 'file' ) or len( parts.scheme ) == 1:
            if mode and encoding:
                file_pointer = codecs.open( resource, mode, encoding )

            else:
                file_pointer = codecs.open( resource, mode )

        elif username is None or password is None or realm is None or host is None:
            file_pointer = urllib2.urlopen( resource )

        else:
            authinfo = urllib2.HTTPBasicAuthHandler() 
            authinfo.add_password( realm, host, username, password ) 
            opener = urllib2.build_opener(authinfo) 
            file_pointer = opener.open( resource )

    else:
        file_closable = False
        file_pointer  = resource

    return ( file_pointer, file_closable )

class Source( metl.reader.Reader ):

    init = []
    resource_init = []

    # void
    def __init__( self, fieldset, *args, **kwargs ):

        self.fieldset = fieldset
        self.limit    = None
        self.offset   = 0
        self.kwargs   = kwargs
        self.base     = None
        self.current  = 0
        self.res_dict = None

    def clone( self ):

        return self.__class__(
            self.fieldset.clone()
        )

    # void
    def setLimitNumber( self, number ):

        self.limit = number

    # void
    def setOffsetNumber( self, number, init = False ):

        if init:
            self.offset += int( number )
        else:
            self.offset = int( number or 0 )

    # FieldSet
    def getFieldSetWithValue( self, value ):

        fs = self.getFieldSetPrototypeCopy()
        fs.setValues( value, self.base )
        fs.transform()

        self.log( fs, value )
        
        return fs

    # void
    def invalidOffsetRecord( self, record ):

        pass

    # void
    def setResource( self ):

        raise RuntimeError( 'Source.setResource() is not implemented yet!' )

    # list
    def getRecordsList( self ):

        raise RuntimeError( 'Source.getRecordsList() is not implemented yet!' )

    # FieldSet
    def getTransformedRecord( self, record ):

        raise RuntimeError( 'Source.getTransformedRecord() is not implemented yet!' )

    # list<FieldSet>
    def getRecords( self ):

        for record in self.getRecordsList():
            if self.current >= int( self.offset ):
                yield self.getFieldSetWithValue( self.getTransformedRecord( record ) )

            else:
                self.invalidOffsetRecord( record )

            self.current += 1
            if self.limit is not None and self.current >= int( self.limit ) + int( self.offset ):
                raise StopIteration()

    # FieldSet
    def getFieldSetPrototypeCopy( self, final = False ):

        return self.fieldset.clone( final )

    # void
    def logActive( self, record, msgvalue ):

        msgdict = {}
        for k, v in msgvalue.items():
            msgdict[ k ] = unicode(v) if type( v ) not in ( str, unicode, float, bool, int ) else v

        self.log_file_pointer.write( '%s: %s => %s\n' % ( 
            record.getID(), 
            demjson.encode( msgdict ), 
            demjson.encode( record.getValues( class_to_string = True ) ) ) 
        )

    # dict
    def getResourceDict( self ):

        return self.res_dict

    def updateResourceDict( self, res_dict ):

        if self.res_dict is None:
            self.res_dict = {}

        for k,v in res_dict.items():
            if k in self.resource_init:
                self.res_dict[k] = v


class FileSource( Source ):

    resource_init = ['resource','encoding','username','password','realm','host']

    # void
    def __init__( self, fieldset, *args, **kwargs ):

        self.resource      = None
        self.encoding      = None
        self.file_pointer  = None
        self.file_closable = None

        super( FileSource, self ).__init__( fieldset, *args, **kwargs )

    # void
    def initialize( self ):

        self.file_pointer, self.file_closable = openResource( 
            self.getResource(), 
            'r', 
            self.getEncoding(),
            username = self.htaccess_username,
            password = self.htaccess_password,
            realm = self.htaccess_realm,
            host = self.htaccess_host
        )
        return super( FileSource, self ).initialize()

    # void
    def finalize( self ):

        if self.file_closable:
            self.file_pointer.close()

        return super( FileSource, self ).finalize()

    # void
    def setResource( self, resource, encoding = 'utf-8', username = None, password = None, realm = None, host = None ):

        res_dict = {
            'resource': resource,
            'encoding': encoding,
            'username': username,
            'password': password,
            'realm': realm,
            'host': host
        }

        self.updateResourceDict( res_dict )

        return self

    def updateResourceDict( self, res_dict ):

        super( FileSource, self ).updateResourceDict( res_dict )

        self.resource = os.path.abspath( self.res_dict['resource'] ) \
            if os.path.exists( os.path.abspath( self.res_dict['resource'] ) ) \
            else self.res_dict['resource']

        self.encoding = self.res_dict.get('encoding')
        self.htaccess_username = self.res_dict.get('username')
        self.htaccess_password = self.res_dict.get('password')
        self.htaccess_realm = self.res_dict.get('realm')
        self.htaccess_host = self.res_dict.get('host')

    # unicode
    def getResource( self ):

        return self.resource

    # unicode
    def getEncoding( self ):

        return self.encoding

