
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

import unittest
from metl.configparser import ConfigParser
from metl.config import Config

class Test_ConfigParser( unittest.TestCase ):

    def test_diff_structure_configs( self ):

        cp1 = ConfigParser( Config('tests/config/test_config.yml') )
        cp2 = ConfigParser( Config('tests/config/test_config_diff.yml') )

        self.assertEqual( cp1.getLastReader().getFieldSetPrototypeCopy().getFieldMap().getRules(), cp2.getLastReader().getFieldSetPrototypeCopy().getFieldMap().getRules() )
        self.assertEqual( cp1.getLastReader().getFieldSetPrototypeCopy().getField('name').getValue(), cp2.getLastReader().getFieldSetPrototypeCopy().getField('name').getValue() )

if __name__ == '__main__':
    unittest.main()