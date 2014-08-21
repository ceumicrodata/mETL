
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

import unittest, metl.filter.base, hashlib, os, sys
from metl.manager import Manager
from metl.configparser import ConfigParser
from metl.config import Config
from metl.exception import *
from metl.utils import *

class Test_Target( unittest.TestCase ):

    def setUp( self ):

        if os.path.exists('tests/target'):
            os.unlink('tests/target')

    def getHashForFile( self, filename, delete = True, migration_resource = None, target_migration_resource = None ):

        configparser = ConfigParser( Config( filename ) )
        target = configparser.getTarget()
        manager = Manager( target, migration_resource = migration_resource, target_migration_resource = target_migration_resource ).run()
        hashcode = hashlib.md5( open( 'tests/target', 'rb' ).read() ).hexdigest()
        if delete:
            os.unlink( 'tests/target' )

        return hashcode

    def test_csv_target( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_csv_target_normal.yml'), 'e3211f67fa9529ed6fb6b2a9445b5ff0' )

    def test_csv_target_special( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_csv_target_special.yml'), '72123ab835df62303918ba7c17c896ca' )

    def test_csv_target_append( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_csv_target_append.yml', delete = False ), '9cffbc6f7693a5847bff770e1622539f' )
        self.assertEqual( self.getHashForFile('tests/config/test_csv_target_append.yml' ), 'cf947195ad7e68113c7e56f7ec043db2' )

    def test_tsv_target( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_tsv_target.yml'), '25536bd3ac8588467f9afa64ec284fcf' )

    def test_fwt_target( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_fwt_target.yml'), 'aba939cfe36f5b23be3a0cb187e99f13' )

    def test_json_target( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_json_target.yml', delete = False), 'fd5ae206495b4e9dc624e0999eaf5082' )

    def test_yaml_target( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_yaml_target.yml'), 'ed16fc6b87c551bdecf864b81cb078e4' )

    def test_xml_target( self ):

        if tuple(sys.version_info)[:3] <= (2, 7, 2):
            self.assertEqual( self.getHashForFile('tests/config/test_xml_target.yml'), '02e4bf1d0f6c0fc8672b5f5dabb9c284' )
        else:
            self.assertEqual( self.getHashForFile('tests/config/test_xml_target.yml'), '26c26dd40b05a59b2db2d9562912d1ed' )

    def test_xls_target_empty( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_empty.yml'), '7670fcfa0a0b0f1ccfd420f0bd7dbe66' )

    def test_xls_target_replace_sheet( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_empty.yml', delete = False ), '7670fcfa0a0b0f1ccfd420f0bd7dbe66' )
        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_replace_sheet.yml'), '73033cb58b4a9b4896af512dbc8ae353' )

    def test_xls_target_continue_sheet( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_continue_sheet.yml', delete = False ), '7670fcfa0a0b0f1ccfd420f0bd7dbe66' )
        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_continue_sheet.yml' ), '4d80b0c97abcaea892504a094451f45e' )

    def test_xls_target_dinamic_sheets( self ):

        self.assertEqual( self.getHashForFile('tests/config/test_xls_target_dynamic.yml'), 'c5d2bc718817370dd83813ee5a16536c' )

    def test_db_target_not_exist_table( self ):

        with self.assertRaises( ResourceNotExistsError ):
            self.getHashForFile('tests/config/test_db_target_not_exist_table.yml')

    def test_db_target_without_table_and_fn( self ):

        with self.assertRaises( ParameterError ):
            self.getHashForFile('tests/config/test_db_target_without_table_and_fn.yml')

    def test_db_target_with_fn( self ):

        self.getHashForFile('tests/config/test_db_target_with_fn.yml', delete = False )

        s = DatabaseSource( FieldSet([
            Field( 'lat', FloatFieldType() )
        ]))
        s.setResource( url = 'sqlite:///tests/target', table = 'result' )
        s.initialize()
        records = [ r.getField('lat').getValue() for r in s.getRecords() ]
        s.finalize()

        self.assertEqual( len(records), 187 )
        self.assertEqual( records[:5], [ 47.5487066254, 47.5268950165, 47.5997870304, 47.5906976355, 47.5713306295 ] )
        self.assertEqual( records[-5:], [ 47.4839168553, 47.4743399473, 47.4634649166, 47.4703229377, 47.47670395 ] )

        os.unlink( 'tests/target' )

    def test_db_target_table( self ):

        self.getHashForFile('tests/config/test_db_target_table.yml', delete = False )
        self.getHashForFile('tests/config/test_db_target_replace_table.yml', delete = False )
        self.getHashForFile('tests/config/test_db_target_truncate_table.yml', delete = False )
        self.getHashForFile('tests/config/test_db_target_update.yml', delete = False )

        s = DatabaseSource( FieldSet([
            Field( 'lat', FloatFieldType() )
        ]))
        s.setResource( url = 'sqlite:///tests/target', table = 'result' )
        s.initialize()
        records = [ r.getField('lat').getValue() for r in s.getRecords() ]
        s.finalize()

        self.assertEqual( len(records), 374 )
        self.assertEqual( records[:5], [ 47.5487066254, 47.5268950165, 47.5997870304, 47.5906976355, 47.5713306295 ] )
        self.assertEqual( records[-5:], [ 47.4839168553, 47.4743399473, 47.4634649166, 47.4703229377, 47.47670395 ] )

        os.unlink( 'tests/target' )

    def test_db_target_update( self ):

        self.getHashForFile('tests/config/test_db_target_table.yml', delete = False )
        self.getHashForFile('tests/config/test_db_target_update.yml', migration_resource = 'tests/test_migration/test_migration.pickle', delete = False )

        s = DatabaseSource( FieldSet([
            Field( 'lat', FloatFieldType() )
        ]))
        s.setResource( url = 'sqlite:///tests/target', table = 'result' )
        s.initialize()
        records = [ r.getField('lat').getValue() for r in s.getRecords() ]
        s.finalize()

        self.assertEqual( len(records), 187 )
        self.assertEqual( records[:5], [ 50.5487066254, 50.5268950165, 50.5997870304, 50.5906976355, 47.5713306295 ] )
        self.assertEqual( records[-5:], [ 47.4839168553, 47.4743399473, 47.4634649166, 47.4703229377, 47.47670395 ] )

        os.unlink( 'tests/target' )

    def test_db_target_complex( self ):

        self.getHashForFile('tests/config/test_db_target_complex.yml', delete = False )
        s = DatabaseSource( FieldSet([
            Field( 'SERIAL_NUMBER', IntegerFieldType() ),
            Field( 'VALUE', ComplexFieldType() ),
            Field( 'LIST', ListFieldType() )
        ]))
        s.setResource( url = 'sqlite:///tests/target', table = 'result' )
        s.initialize()
        records = [ r.getValues() for r in s.getRecords() ]
        s.finalize()

        self.assertEqual(
            [ r.getValues() for r in s.getRecords() ],
            [
                {'SERIAL_NUMBER': 1, 'LIST': [u'1st', u'2nd', u'3rd'], 'VALUE': [1, 2, 3, 4, 5, [6, 7]]},
                {'SERIAL_NUMBER': 2, 'LIST': [u'3rd', u'2nd', u'1st'], 'VALUE': {u'second': 2, u'third': [1, 2, 3], u'first': 1}}
            ]
        )

        os.unlink( 'tests/target' )

    def test_db_target_limit( self ):

        configparser = ConfigParser( Config( 'tests/config/test_db_target_limit.yml' ) )
        target = configparser.getTarget()
        target.initialize()

        self.assertEquals( 255, target.database.table.c.name.type.length )
        self.assertEquals( 130, target.database.table.c.name_limited.type.length )

def RunFunctionQuery( connection, insert_buffer, update_buffer ):

    connection.execute(
        """
        CREATE TABLE result (
            lat numeric,
            lng numeric
        );
        """
    )

    for item in insert_buffer:
        connection.execute(
            """
            INSERT INTO result ( lat, lng ) VALUES ( :lat, :lng );
            """,
            item
        )

if __name__ == '__main__':
    unittest.main()
