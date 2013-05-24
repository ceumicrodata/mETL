
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

import unittest, os, subprocess, hashlib

class Test_Script( unittest.TestCase ):

    def test_metl_transform( self ):

        SCRIPT = 'metl-transform tests/config/test_config.yml time "29 Dec, 2012 12:12:41"'
        v = subprocess.check_output( [SCRIPT], shell = True ).split('\n')[-2]
        self.assertEqual( v, "u'2012-12'" )

    def test_metl( self ):

        SCRIPT = 'metl tests/config/test_config.yml'
        v = hashlib.md5( subprocess.check_output( [SCRIPT], shell = True ) ).hexdigest()
        self.assertEqual( v, '19ecb92fd8c3d96cc451f00a3c71b4e0' )

if __name__ == '__main__':
    unittest.main()