import unittest
from metl.tarr.data import Data
from metl import tarr
from metl.tarr.compiler import IF, ELSE, ENDIF, RETURN_TRUE, Program


@tarr.branch
def is_animal(data):
    return data in ('fish', 'cat', 'dog')


@tarr.rule
def animal(data):
    return 'ANIMAL'


@tarr.rule
def other(data):
    return 'something else'


PROGRAM = [
    IF (is_animal),
        animal,
    ELSE,
        other,
    ENDIF,
    RETURN_TRUE
]


class TestDecorators(unittest.TestCase):

    def test_decorators(self):
        program = Program(PROGRAM)

        assertEqual = self.assertEqual
        assertEqual('ANIMAL', program.run(Data(1, 'fish')).payload)
        assertEqual('ANIMAL', program.run(Data(1, 'cat')).payload)
        assertEqual('ANIMAL', program.run(Data(1, 'dog')).payload)
        assertEqual('something else', program.run(Data(1, 'flower')).payload)
        assertEqual('something else', program.run(Data(1, 'rock')).payload)
