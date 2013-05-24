
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
from nltk import SnowballStemmer

class StemTransform( metl.transform.base.Transform ):

    init = ['language']

    # void
    def __init__( self, language, *args, **kwargs ):

        self.language  = language.lower()
        self.stemmer   = SnowballStemmer( self.language )

        super( StemTransform, self ).__init__( *args, **kwargs )

    # Field
    def transform( self, field ):

        if field.getValue() is None:
            return field

        field.setValue( u' '.join( [ self.stemmer.stem( word ) for word in field.getValue().split() if word != u'' ] ) )
        return field

