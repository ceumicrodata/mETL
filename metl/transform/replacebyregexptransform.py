
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

import metl.transform.base, re

class ReplaceByRegexpTransform( metl.transform.base.Transform ):

    init = ['regexp','to','ignorecase']

    # void
    def __init__( self, regexp, to, ignorecase = False, *args, **kwargs ):

        self.regexp     = regexp
        self.to         = re.sub( r'\$', '\\\\', to )
        self.ignorecase = ignorecase

        super( ReplaceByRegexpTransform, self ).__init__( *args, **kwargs )

    # Field
    def transform( self, field ):

        if field.getValue() is None:
            return field

        if not self.ignorecase:
            field.setValue( re.sub( self.regexp, self.to, field.getValue() ) )
        else:
            field.setValue( re.sub( self.regexp, self.to, field.getValue(), re.I ) )

        return field
