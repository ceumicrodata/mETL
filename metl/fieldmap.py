
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

import copy

class FieldMap( object ):

    def __init__( self, rules ):

        self.rules = rules

    # dict
    def getRules( self ):

        return self.rules

    def getFieldValue( self, obj, rule ):

        current_object = obj
        try:
            for rule_part in unicode( rule ).split('/'):

                if rule_part == '!':
                    if type( current_object ) not in ( list, tuple ) and not isinstance( current_object, list ) and not isinstance( current_object, tuple ):
                        current_object = [ current_object ]

                    continue

                if type( current_object ) == dict or isinstance( current_object, dict ): 
                    if rule_part in current_object:
                        current_object = current_object[ rule_part ]

                    elif rule_part[0] == '~':
                        current_object = current_object

                    elif rule_part == '!':
                        current_object = [ current_object ]

                    else:
                        current_object = current_object[ int( rule_part ) ]

                elif type( current_object ) in ( list, tuple ) or isinstance( current_object, list ) or isinstance( current_object, tuple ):
                    if rule_part[0] == '~':
                        rule_part = rule_part[1:]

                    if rule_part.count('=') == 1:
                        object_name, object_value = rule_part.split('=')
                        for item in current_object:
                            if item[object_name] == object_value:
                                current_object = item

                    elif rule_part == '*':
                        new_object = []
                        for item in current_object:
                            new_object.append( self.getFieldValue( 
                                item, 
                                rule[ rule.index( rule_part ) + len( rule_part ) + 1: ] 
                            ))
                        return new_object

                    elif rule_part.count(':') == 1:
                        pts = rule_part.split(':')
                        new_object = []
                        for item in current_object[ int( pts[0] ) : int( pts[1] ) ]:
                            new_object.append( self.getFieldValue( 
                                item, 
                                rule[ rule.index( rule_part ) + len( rule_part ) + 1: ] 
                            ))
                        return new_object

                    else:
                        current_object = current_object[ int( rule_part ) ]

                elif isinstance( current_object, object ) and type( current_object ) not in ( str, unicode, int, float, bool ) :
                    current_object = getattr( current_object, rule_part )

                elif type( current_object ) in ( str, unicode ):
                    pts = rule_part.split(':')
                    if len( pts ) == 2:
                        current_object = current_object[ int( pts[0] ) : int( pts[1] ) ].strip()

                    else:
                        current_object = current_object[ int( rule_part ) ].strip()

            return current_object
            
        except:
            return None

    # dict
    def getValues( self, obj ):

        ret_dict = {}
        for field_name, rule in self.getRules().items():
            ret_dict[ field_name ] = self.getFieldValue( obj, rule )

        return ret_dict