
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
from metl.condition.ismatchbyregexpcondition import IsMatchByRegexpCondition
from metl.modifier.setmodifier import SetModifier
from metl.modifier.joinbykeymodifier import JoinByKeyModifier
from metl.modifier.transformfieldmodifier import TransformFieldModifier
from metl.transform.replacebyregexptransform import ReplaceByRegexpTransform
from metl.modifier.ordermodifier import OrderModifier
from metl.source.staticsource import StaticSource

class Test_Modifier( unittest.TestCase ):

    def setUp( self ):

        self.reader = StaticSource(
            FieldSet([
                Field( 'name', StringFieldType() ),
                Field( 'email', StringFieldType() ),
                Field( 'year', IntegerFieldType() ),
                Field( 'after_year', IntegerFieldType() ),
                Field( 'age', IntegerFieldType() )
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

    def test_joinbykey_modifier( self ):

        source = StaticSource(
            FieldSet([
                Field( 'email', StringFieldType(), key = True ),
                Field( 'age', IntegerFieldType() )
            ],
            FieldMap({
                'email': 0,
                'age': 1
            }))
        )
        source.setResource([
            [ 'El Agent@metl-test-data.com', 12 ],
            [ 'Ochala Wild@metl-test-data.com', 14 ],
            [ 'Sina Venomous@metl-test-data.com', 17 ],
            [ 'Akassa Savage Phalloz@metl-test-data.com', 16 ],
            [ 'Sermak Bad@metl-test-data.com', 22 ],
            [ 'Olivia Deadly Dawod@metl-test-data.com', 32 ],
            [ 'PendusInhuman@metl-test-data.com', 42 ],
            [ 'Naria Cold-blodded Greste@metl-test-data.com', 22 ],
            [ 'ShardBrutal@metl-test-data.com', 54 ],
            [ 'Sina Cruel@metl-test-data.com', 56 ],
            [ 'Deadly Ohmar@metl-test-data.com', 43 ],
            [ 'Mylenedriz Cold-blodded@metl-test-data.com', 23 ],
            [ 'Calden rigid@metl-test-data.com', 35 ],
            [ 'AcidReaper@metl-test-data.com', 56 ],
            [ 'Raven Seth@metl-test-data.com', 23 ],
            [ 'RandomLeader@metl-test-data.com', 45 ],
            [ 'Pluto Brigadier@metl-test-data.com', 64 ],
            [ 'Southern Kangaroo@metl-test-data.com', 53 ],
            [ 'Serious Flea@metl-test-data.com', 62 ],
            [ 'NocturnalRaven@metl-test-data.com', 63 ],
            [ 'Risky Flea@metl-test-data.com', 21 ],
            [ 'Rivatha Todal@metl-test-data.com', 56 ],
            [ 'Panic Oliviaezit@metl-test-data.com', 25 ],
            [ 'Tomara Wild@metl-test-data.com', 46 ],
            [ 'Venessa Metalhead@metl-test-data.com', 53 ],
            [ 'Western Ogre@metl-test-data.com', 71 ],
            [ 'SergeantStrawberry@metl-test-data.com', 76 ]
        ])        

        records = [ r for r in JoinByKeyModifier( 
            self.reader,
            fieldNames = [ 'age' ],
            source = source
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 85 )
        self.assertIsNone( records[-1].getField('age').getValue() )
        self.assertEqual( records[-2].getField('age').getValue(), 71 )
        self.assertEqual( records[0].getField('age').getValue(), 12 )

    def test_order_modifier( self ):

        records = [ r for r in OrderModifier(
            self.reader,
            fieldNamesAndOrder = [ { 'year': 'DESC' }, { 'name': 'ASC' } ]
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 85 )

        self.assertEqual( records[-1].getField('year').getValue(), 1998 )
        self.assertEqual( records[-4].getField('year').getValue(), 1999 )
        self.assertEqual( records[0].getField('year').getValue(), 2013 )

        self.assertEqual( records[-1].getField('name').getValue(), 'Western Ogre' )
        self.assertEqual( records[-2].getField('name').getValue(), 'The Sergeant' )
        self.assertEqual( records[-3].getField('name').getValue(), 'The Admiral' )
        self.assertEqual( records[-4].getField('name').getValue(), 'The Skunk' )
        self.assertEqual( records[0].getField('name').getValue(), 'Dragon Insane' )

    def test_transform_field_modifier( self ):

        records = [ r for r in TransformFieldModifier(
            self.reader,
            fieldNames = [ 'email' ],
            transforms = [ ReplaceByRegexpTransform( regexp = u'[ ]+', to = u'' ) ]
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 85 )
        self.assertEqual( records[-1].getField('email').getValue(), 'SergeantStrawberry@metl-test-data.com' )

    def test_set_modifier_without_param( self ):

        records = [ r for r in SetModifier( 
            self.reader,
            fieldNames = [ 'name' ]
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 85 )
        self.assertEqual( len([ r for r in records if r.getField('name').getValue() is not None ]), 0 )

    def test_set_modifier_with_sprintf_value( self ):

        records = [ r for r in SetModifier( 
            self.reader,
            fieldNames = [ 'email' ],
            value = '%(name)s <%(email)s>'
        ).initialize().getRecords() ]

        self.assertEqual( len( records ), 85 )
        self.assertEqual( records[-1].getField('email').getValue(), 'Sergeant Strawberry <Sergeant Strawberry@metl-test-data.com>' )

    def test_set_modifier_value( self ):

        records = [ r for r in SetModifier( 
            self.reader,
            fieldNames = [ 'name' ],
            value = 'Same name for all'
        ).initialize().getRecords() ]
        
        self.assertEqual( len( records ), 85 )
        self.assertEqual( records[-1].getField('name').getValue(), 'Same name for all' )

    def test_set_modifier_fn( self ):

        def setValue( record, field, scope ):

            if record.getField('year').getValue() == 2008:
                return 'Same name for 2008 years'

            return 'Other name for anything else'

        fn = setValue

        records = [ r for r in SetModifier( 
            self.reader,
            fieldNames = [ 'name' ],
            fn = fn,
        ).initialize().getRecords() ]
        
        self.assertEqual( len( records ), 85 )
        self.assertEqual( records[-1].getField('name').getValue(), 'Other name for anything else' )
        self.assertEqual( records[0].getField('name').getValue(), 'Same name for 2008 years' )
        self.assertEqual( len([ r for r in records if r.getField('name').getValue() == 'Same name for 2008 years' ]), 43 )
        self.assertEqual( len([ r for r in records if r.getField('name').getValue() == 'Other name for anything else' ]), 42 )

    def test_set_modifier_source( self ):

        def setValue( record, field, scope ):

            return 'Found same email address' \
                if record.getField('email').getValue() in [ sr.getField('email').getValue() for sr in scope.getSourceRecords() ] \
                else 'Not found same email address'

        fn = setValue

        source = StaticSource(
            FieldSet([
                Field( 'name', StringFieldType() ),
                Field( 'email', StringFieldType() )
            ],
            FieldMap({
                'name': 0,
                'email': 1
            }))
        )
        source.setResource([
            [ 'El Agent', 'El Agent@metl-test-data.com' ],
            [ 'Ochala Wild', 'Ochala Wild@metl-test-data.com' ],
            [ 'Sina Venomous', 'Sina Venomous@metl-test-data.com' ],
            [ 'Akassa Savage Phalloz', 'Akassa Savage Phalloz@metl-test-data.com' ],
            [ 'Sermak Bad', 'Sermak Bad@metl-test-data.com' ],
            [ 'Olivia Deadly Dawod', 'Olivia Deadly Dawod@metl-test-data.com' ],
            [ 'Pendus Inhuman', 'PendusInhuman@metl-test-data.com' ],
            [ 'Naria Cold-blodded Greste', 'Naria Cold-blodded Greste@metl-test-data.com' ],
            [ 'Shard Brutal', 'ShardBrutal@metl-test-data.com' ],
            [ 'Sina Cruel', 'Sina Cruel@metl-test-data.com' ],
            [ 'Deadly Ohmar', 'Deadly Ohmar@metl-test-data.com' ],
            [ 'Mylenedriz Cold-blodded', 'Mylenedriz Cold-blodded@metl-test-data.com' ],
            [ 'Calden Frigid', 'Calden rigid@metl-test-data.com' ],
            [ 'Acid Reaper', 'AcidReaper@metl-test-data.com' ],
            [ 'Raven Seth', 'Raven Seth@metl-test-data.com' ],
            [ 'Random Leader', 'RandomLeader@metl-test-data.com' ],
            [ 'Pluto Brigadier', 'Pluto Brigadier@metl-test-data.com' ],
            [ 'Southern Kangaroo', 'Southern Kangaroo@metl-test-data.com' ],
            [ 'Serious Flea', 'Serious Flea@metl-test-data.com' ],
            [ 'Nocturnal Raven', 'NocturnalRaven@metl-test-data.com' ],
            [ 'Risky Flea', 'Risky Flea@metl-test-data.com' ],
            [ 'Rivatha Todal', 'Rivatha Todal@metl-test-data.com' ],
            [ 'Panic Oliviaezit', 'Panic Oliviaezit@metl-test-data.com' ],
            [ 'Tomara Wild', 'Tomara Wild@metl-test-data.com' ],
            [ 'Venessa Metalhead', 'Venessa Metalhead@metl-test-data.com' ],
            [ 'Western Ogre', 'Western Ogre@metl-test-data.com' ],
            [ 'Sergeant Strawberry', 'SergeantStrawberry@metl-test-data.com' ]
        ])

        modifier = SetModifier( 
            self.reader,
            fieldNames = [ 'name' ],
            source = source,
            fn = fn
        )
        modifier.initialize()
        records = [ r for r in modifier.getRecords() ]
        modifier.finalize()
        
        self.assertEqual( len( records ), 85 )
        self.assertEqual( len([ r for r in records if r.getField('name').getValue() == 'Found same email address' ]), 6 )
        self.assertEqual( len([ r for r in records if r.getField('name').getValue() == 'Not found same email address' ]), 79 )

if __name__ == '__main__':
    unittest.main()