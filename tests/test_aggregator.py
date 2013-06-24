
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
from metl.config import Config
from metl.field import Field
from metl.fieldset import FieldSet
from metl.fieldmap import FieldMap
from metl.fieldtype.stringfieldtype import StringFieldType
from metl.fieldtype.integerfieldtype import IntegerFieldType
from metl.aggregator.countaggregator import CountAggregator
from metl.aggregator.sumaggregator import SumAggregator
from metl.aggregator.avgaggregator import AvgAggregator
from metl.source.staticsource import StaticSource

class Test_Aggregator( unittest.TestCase ):

    def setUp( self ):

        self.reader = StaticSource(
            FieldSet([
                Field( 'name', StringFieldType() ),
                Field( 'email', StringFieldType() ),
                Field( 'year', IntegerFieldType() ),
                Field( 'after_year', IntegerFieldType() ),
                Field( 'number', IntegerFieldType() )
            ],
            FieldMap({
                'name': 0,
                'email': 1,
                'year': 2,
                'after_year': 3
            }))
        )
        self.reader.setResource([
            [ 'El Agent', 'El Agent@metl-test-data.com', 2008, 2008 ],
            [ 'Serious Electron', 'Serious Electron@metl-test-data.com', 2008, 2013 ],
            [ 'Brave Wizard', 'Brave Wizard@metl-test-data.com', 2008, 2008 ],
            [ 'Forgotten Itchy Emperor', 'Forgotten Itchy Emperor@metl-test-data.com', 2008, 2013 ],
            [ 'The Moving Monkey', 'The Moving Monkey@metl-test-data.com', 2008, 2008 ],
            [ 'Evil Ghostly Brigadier', 'Evil Ghostly Brigadier@metl-test-data.com', 2008, 2013 ],
            [ 'Strangely Oyster', 'Strangely Oyster@metl-test-data.com', 2008, 2008 ],
            [ 'Anaconda Silver', 'Anaconda Silver@metl-test-data.com', 2006, 2008 ],
            [ 'Hawk Tough', 'Hawk Tough@metl-test-data.com', 2004, 2008 ],
            [ 'The Disappointed Craw', 'The Disappointed Craw@metl-test-data.com', 2008, 2013 ],
            [ 'The Raven', 'The Raven@metl-test-data.com', 1999, 2008 ],
            [ 'Ruby Boomerang', 'Ruby Boomerang@metl-test-data.com', 2008, 2008 ],
            [ 'Skunk Tough', 'Skunk Tough@metl-test-data.com', 2010, 2008 ],
            [ 'The Nervous Forgotten Major', 'The Nervous Forgotten Major@metl-test-data.com', 2008, 2013 ],
            [ 'Bursting Furious Puppet', 'Bursting Furious Puppet@metl-test-data.com', 2011, 2008 ],
            [ 'Neptune Eagle', 'Neptune Eagle@metl-test-data.com', 2011, 2013 ],
            [ 'The Skunk', 'The Skunk@metl-test-data.com', 2008, 2013 ],
            [ 'Lone Demon', 'Lone Demon@metl-test-data.com', 2008, 2008 ],
            [ 'The Skunk', 'The Skunk@metl-test-data.com', 1999, 2008 ],
            [ 'Gamma Serious Spear', 'Gamma Serious Spear@metl-test-data.com', 2008, 2008 ],
            [ 'Sleepy Dirty Sergeant', 'Sleepy Dirty Sergeant@metl-test-data.com', 2008, 2008 ],
            [ 'Red Monkey', 'Red Monkey@metl-test-data.com', 2008, 2008 ],
            [ 'Striking Tiger', 'Striking Tiger@metl-test-data.com', 2005, 2008 ],
            [ 'Sliding Demon', 'Sliding Demon@metl-test-data.com', 2011, 2008 ],
            [ 'Lone Commander', 'Lone Commander@metl-test-data.com', 2008, 2013 ],
            [ 'Dragon Insane', 'Dragon Insane@metl-test-data.com', 2013, 2013 ],
            [ 'Demon Skilled', 'Demon Skilled@metl-test-data.com', 2011, 2004 ],
            [ 'Vulture Lucky', 'Vulture Lucky@metl-test-data.com', 2003, 2008 ],
            [ 'The Ranger', 'The Ranger@metl-test-data.com', 2013, 2008 ],
            [ 'Morbid Snake', 'Morbid Snake@metl-test-data.com', 2011, 2008 ],
            [ 'Dancing Skeleton', 'Dancing Skeleton@metl-test-data.com', 2001, 2004 ],
            [ 'The Psycho', 'The Psycho@metl-test-data.com', 2005, 2008 ],
            [ 'Jupiter Rider', 'Jupiter Rider@metl-test-data.com', 2011, 2008 ],
            [ 'Green Dog', 'Green Dog@metl-test-data.com', 2011, 2008 ],
            [ 'Brutal Wild Colonel', 'Brutal Wild Colonel@metl-test-data.com', 2004, 2008 ],
            [ 'Random Leader', 'Random Leader@metl-test-data.com', 2008, 2008 ],
            [ 'Pluto Brigadier', 'Pluto Brigadier@metl-test-data.com', 2008, 2004 ],
            [ 'Southern Kangaroo', 'Southern Kangaroo@metl-test-data.com', 2008, 2008 ],
            [ 'Serious Flea', 'Serious Flea@metl-test-data.com', 2001, 2005 ],
            [ 'Nocturnal Raven', 'Nocturnal Raven@metl-test-data.com', 2008, 2004 ],
            [ 'Risky Flea', 'Risky Flea@metl-test-data.com', 2005, 2005 ],
            [ 'The Corporal', 'The Corporal@metl-test-data.com', 2013, 2008 ],
            [ 'The Lucky Barbarian', 'The Lucky Barbarian@metl-test-data.com', 2008, 2008 ],
            [ 'Rocky Serious Dog', 'Rocky Serious Dog@metl-test-data.com', 2008, 2008 ],
            [ 'The Frozen Guardian', 'The Frozen Guardian@metl-test-data.com', 2008, 2008 ],
            [ 'Freaky Frostbite', 'Freaky Frostbite@metl-test-data.com', 2008, 2004 ],
            [ 'The Tired Raven', 'The Tired Raven@metl-test-data.com', 2008, 2008 ],
            [ 'Disappointed Frostbite', 'Disappointed Frostbite@metl-test-data.com', 2008, 2008 ],
            [ 'The Craw', 'The Craw@metl-test-data.com', 2003, 2008 ],
            [ 'Gutsy Strangely Chief', 'Gutsy Strangely Chief@metl-test-data.com', 2008, 2008 ],
            [ 'Queen Angry', 'Queen Angry@metl-test-data.com', 2008, 2008 ],
            [ 'Pluto Albatross', 'Pluto Albatross@metl-test-data.com', 2003, 2008 ],
            [ 'Endless Invader', 'Endless Invader@metl-test-data.com', 2003, 2004 ],
            [ 'Beta Young Sergeant', 'Beta Young Sergeant@metl-test-data.com', 2008, 2011 ],
            [ 'The Demon', 'The Demon@metl-test-data.com', 2003, 2008 ],
            [ 'Lone Monkey', 'Lone Monkey@metl-test-data.com', 2011, 2008 ],
            [ 'Bursting Electron', 'Bursting Electron@metl-test-data.com', 2003, 2010 ],
            [ 'Gangster Solid', 'Gangster Solid@metl-test-data.com', 2005, 2009 ],
            [ 'The Gladiator', 'The Gladiator@metl-test-data.com', 2001, 2002 ],
            [ 'Flash Frostbite', 'Flash Frostbite@metl-test-data.com', 2005, 2004 ],
            [ 'The Rainbow Pluto Demon', 'The Rainbow Pluto Demon@metl-test-data.com', 2011, 2013 ],
            [ 'Poseidon Rider', 'Poseidon Rider@metl-test-data.com', 2008, 2006 ],
            [ 'The Old Alpha Brigadier', 'The Old Alpha Brigadier@metl-test-data.com', 2008, 2008 ],
            [ 'Rough Anaconda', 'Rough Anaconda@metl-test-data.com', 2001, 2011 ],
            [ 'Tough Dinosaur', 'Tough Dinosaur@metl-test-data.com', 2011, 2010 ],
            [ 'The Lost Dinosaur', 'The Lost Dinosaur@metl-test-data.com', 2008, 2008 ],
            [ 'The Raven', 'The Raven@metl-test-data.com', 2005, 2009 ],
            [ 'The Agent', 'The Agent@metl-test-data.com', 2011, 2008 ],
            [ 'Brave Scarecrow', 'Brave Scarecrow@metl-test-data.com', 2008, 2007 ],
            [ 'Flash Skeleton', 'Flash Skeleton@metl-test-data.com', 2008, 2006 ],
            [ 'The Admiral', 'The Admiral@metl-test-data.com', 1998, 2005 ],
            [ 'The Tombstone', 'The Tombstone@metl-test-data.com', 2013, 2008 ],
            [ 'Golden Arrow', 'Golden Arrow@metl-test-data.com', 2008, 2005 ],
            [ 'White Guardian', 'White Guardian@metl-test-data.com', 2011, 2004 ],
            [ 'The Black Eastern Power', 'The Black Eastern Power@metl-test-data.com', 2008, 2008 ],
            [ 'Ruthless Soldier', 'Ruthless Soldier@metl-test-data.com', 2008, 2008 ],
            [ 'Dirty Clown', 'Dirty Clown@metl-test-data.com', 2008, 2008 ],
            [ 'Alpha Admiral', 'Alpha Admiral@metl-test-data.com', 2008, 2008 ],
            [ 'Lightning Major', 'Lightning Major@metl-test-data.com', 2008, 2008 ],
            [ 'The Rock Demon', 'The Rock Demon@metl-test-data.com', 2008, 2001 ],
            [ 'Wild Tiger', 'Wild Tiger@metl-test-data.com', 2008, 2001 ],
            [ 'The Pointless Bandit', 'The Pointless Bandit@metl-test-data.com', 2008, 2008 ],
            [ 'The Sergeant', 'The Sergeant@metl-test-data.com', 1998, 2002 ],
            [ 'Western Ogre', 'Western Ogre@metl-test-data.com', 1998, 2004 ],
            [ 'Sergeant Strawberry', 'Sergeant Strawberry@metl-test-data.com', 2006, 2008 ]
        ])

    def test_count_aggregator( self ):

        records = [ r for r in CountAggregator(
            self.reader,
            fieldNames = 'year',
            targetFieldName = 'number'
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 11 )
        self.assertEqual( records[0].getFieldNames(), ['year','number'] )
        self.assertEqual( records[0].getValues(), {'number': 2, 'year': 1999} )
        self.assertEqual( records[-1].getValues(), {'number': 4, 'year': 2013} )

    def test_sum_aggregator( self ):

        records = [ r for r in SumAggregator(
            self.reader,
            fieldNames = 'year',
            targetFieldName = 'number',
            valueFieldName = 'after_year'
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 11 )
        self.assertEqual( records[0].getFieldNames(), ['year','number'] )
        self.assertEqual( records[0].getValues(), {'number': 4016, 'year': 1999} )
        self.assertEqual( records[-1].getValues(), {'number': 8037, 'year': 2013} )

    def test_avg_aggregator( self ):

        records = [ r for r in AvgAggregator(
            self.reader,
            fieldNames = 'year',
            targetFieldName = 'number',
            valueFieldName = 'after_year'
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 11 )
        self.assertEqual( records[0].getFieldNames(), ['year','number'] )
        self.assertEqual( records[0].getValues(), {'number': 2008, 'year': 1999} )
        self.assertEqual( records[-1].getValues(), {'number': 2009, 'year': 2013} )

if __name__ == '__main__':
    unittest.main()