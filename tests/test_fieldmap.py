
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

import unittest, metl.fieldmap

class Test_FieldMap( unittest.TestCase ):

    def setUp( self ):

        self.python_dict = {
            'first': {
                'of': {
                    'all': 'dictionary',
                    'with': [ 'many', 'list', 'item' ]
                },
                'and': [ (0, 1), (1, 2), (2, 3), (3, 4) ]
            },
            'filtered': [ { 
                'name': 'first', 
                'value': 'good'
            }, {
                'name': 'second',
                'value': 'normal'
            }, {
                'name': 'third',
                'value': 'bad'
            } ],
            'emptylist': {
                'item': 'itemname'
            },
            'notemptylist': [
                { 'item': 'itemname' },
                { 'item': 'seconditemname' }
            ],
            'strvalue': 'many',
            'strlist': [ 'many', 'list', 'item' ],
            'root': 'R'
        }

        self.python_list = [
            'many',
            'list',
            'item'
        ]

    def test_python_dict_map( self ):

        values = metl.fieldmap.FieldMap({
            'list_first': 'first/of/with/0',
            'list_last': 'first/of/with/-1',
            'tuple_last_first': 'first/and/-1/0',
            'not_existing': 'first/of/here',
            'root': 'root',
            'dict': 'first/of/all',
            'filtered': 'filtered/name=second/value',
            'list': 'filtered/*/value',
            'listendingwstar': 'filtered/*',
            'listendingwostar': 'filtered',
            'listpart': 'filtered/0:-1/value',
            'emptylistref': 'emptylist/~0/item',
            'notemptylistref': 'notemptylist/~0/item',
            'strvalue': 'strvalue',
            'strvalue1': 'strvalue/!/0',
            'strvalue2': 'strvalue/!/1',
            'strlist1': 'strlist/!/0',
            'strlist2': 'strlist/!/1'
        }).getValues( self.python_dict )

        self.assertEqual( values['list_first'], 'many' )
        self.assertEqual( values['list_last'], 'item' )
        self.assertEqual( values['tuple_last_first'], 3 )
        self.assertEqual( values['not_existing'], None )
        self.assertEqual( values['root'], 'R' )
        self.assertEqual( values['dict'], 'dictionary' )
        self.assertEqual( values['filtered'], 'normal' )
        self.assertEqual( values['listpart'], ['good','normal'] )
        self.assertEqual( values['list'], ['good','normal','bad'] )
        self.assertEqual( values['emptylistref'], 'itemname' )
        self.assertEqual( values['notemptylistref'], 'itemname' )
        self.assertEqual( values['strvalue'], 'many' )
        self.assertEqual( values['strvalue1'], 'many' )
        self.assertIsNone( values['strvalue2'] )
        self.assertEqual( values['strlist1'], 'many' )
        self.assertEqual( values['strlist2'], 'list' )
        self.assertEqual( values['listendingwstar'], values['listendingwostar'] )

    def test_python_list( self ):

        values = metl.fieldmap.FieldMap({
            'first': 0,
            'last': '-1',
            'not_existing': 4
        }).getValues( self.python_list )

        self.assertEqual( values['first'], 'many' )
        self.assertEqual( values['last'], 'item' )
        self.assertEqual( values['not_existing'], None )


if __name__ == '__main__':
    unittest.main()