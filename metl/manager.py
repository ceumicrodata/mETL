
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

class Manager( object ):

    # void
    def __init__( self, target, migration_resource = None, target_migration_resource = None ):

        self.target = target

        if migration_resource is not None:
            self.target.setMigrationResource( migration_resource )

        if target_migration_resource is not None:
            self.target.setTargetMigrationResource( target_migration_resource )

    # void
    def run( self ):

        self.target.initialize()
        self.target.write()
        self.target.finalize()