import unittest
from metl.tarr import compiler as m
from metl.tarr.data import Data
from . import test_compiler_base


Noop = m.Instruction()


@m.rule
def add1(n):
    return n + 1


@m.rule
def const_odd(any):
    return 'odd'


@m.rule
def const_even(any):
    return 'even'


@m.branch
def odd(n):
    return n % 2 == 1


@m.branch_rule
def increase_if_odd(n):
    if odd(n):
        return n + 1
    return m.HAVE_NOT_DONE_IT


class WellKnownException(Exception):
    pass


@m.rule
def die(param):
    raise WellKnownException(param)


TEST_TO_TEXT_WITHOUT_STATISTICS = (
'''   0 CALL "su"bprogram"
       # True  -> 1
       # False -> 1
   1 RETURN True
END OF MAIN PROGRAM

DEF ("su"bprogram")
   2 odd
       # True  -> 3
       # False -> 5
   3 add1
   4 RETURN False
   5 RETURN True
END # su"bprogram''')

TEST_TO_TEXT_WITH_STATISTICS = (
'''   0 CALL "su"bprogram"    (*3)
       # True  -> 1   (*2)
       # False -> 1   (*1)
   1 RETURN True   (*3)
END OF MAIN PROGRAM

DEF ("su"bprogram")
   2 odd
       # True  -> 3   (*1)
       # False -> 5   (*2)
   3 add1
   4 RETURN False   (*1)
   5 RETURN True   (*2)
END # su"bprogram''')

TEST_TO_DOT_WITHOUT_STATISTICS = (
r'''digraph {

compound = true;

subgraph "cluster_None" {
    node_0 [label="CALL su\"bprogram"];
    node_0 -> node_1 [label="True"];
    node_0 -> node_1 [label="False"];
    node_1 [label="RETURN True"];
}

subgraph "cluster_su\"bprogram" {
    label = "su\"bprogram";

    node_2 [label="odd"];
    node_2 -> node_3 [label="True"];
    node_2 -> node_5 [label="False"];
    node_3 [label="add1"];
    node_3 -> node_4;
    node_4 [label="RETURN False"];
    node_5 [label="RETURN True"];
}

// inter-cluster-edges
    node_0 -> node_2;
}''')

TEST_TO_DOT_WITH_STATISTICS = (
r'''digraph {

compound = true;

subgraph "cluster_None" {
    node_0 [label="CALL su\"bprogram"];
    node_0 -> node_1 [label="True: 2"];
    node_0 -> node_1 [label="False: 1"];
    node_1 [label="RETURN True: 3"];
}

subgraph "cluster_su\"bprogram" {
    label = "su\"bprogram";

    node_2 [label="odd"];
    node_2 -> node_3 [label="True: 1"];
    node_2 -> node_5 [label="False: 2"];
    node_3 [label="add1"];
    node_3 -> node_4;
    node_4 [label="RETURN False: 1"];
    node_5 [label="RETURN True: 2"];
}

// inter-cluster-edges
    node_0 -> node_2 [label="3"];
}''')


class Test_Program(test_compiler_base.Test_Program):

    # verify, that the functionality of the parent is intact
    # - Liskov's substitution principle

    # NOTE: this test class will pull in all tests from
    #  test_compiler_base.Test_Program but run with
    # the compiler.Program class, not with compiler_base.Program class

    PROGRAM_CLASS = m.Program


class Test_Program_visualization(unittest.TestCase):

    visualized_program_spec = [
        'su"bprogram',  # name contains " to check dot escape
        m.RETURN_TRUE,

        m.DEF ('su"bprogram'),
            m.IF (odd),
                add1,
                m.RETURN_FALSE,
            m.ENDIF,
        m.RETURN_TRUE
    ]

    def program(self):
        return m.Program(self.visualized_program_spec)

    def assertEqualText(self, expected, actual):
        if expected != actual:
            # convert to list of lines and compare them
            self.assertEqual(expected.splitlines(), actual.splitlines())
            self.fail(
                'output differ from expected'
                ' in whitespace stripped by .splitlines()')

    def test_to_text_without_statistics(self):
        prog = self.program()

        text = prog.to_text()

        self.assertEqualText(TEST_TO_TEXT_WITHOUT_STATISTICS, text)

    def test_to_text_with_statistics(self):
        prog = self.program()

        prog.run(Data(id, 1))
        prog.run(Data(id, 2))
        prog.run(Data(id, 2))
        text = prog.to_text(with_statistics=True)

        self.assertEqualText(TEST_TO_TEXT_WITH_STATISTICS, text)

    def test_to_dot_without_statistics(self):
        prog = self.program()

        text = prog.to_dot()

        self.assertEqualText(TEST_TO_DOT_WITHOUT_STATISTICS, text)

    def test_to_dot_with_statistics(self):
        prog = self.program()

        prog.run(Data(id, 1))
        prog.run(Data(id, 2))
        prog.run(Data(id, 2))
        text = prog.to_dot(with_statistics=True)

        self.assertEqualText(TEST_TO_DOT_WITH_STATISTICS, text)


class Test_Program_statistics(unittest.TestCase):

    def prog(self, condition=None):
        RETURN_MAP = {
            True: m.RETURN_TRUE,
            False: m.RETURN_FALSE,
            None: m.RETURN_TRUE
        }
        prog = m.Program(
            [Noop, RETURN_MAP[condition], Noop, Noop, m.RETURN_TRUE])
        prog.runner.ensure_statistics(1)
        return prog

    def test_statistics_created(self):
        prog = m.Program([Noop, m.RETURN_TRUE, Noop, Noop, m.RETURN_TRUE])
        prog.run(None)

        self.assertEqual(2, len(prog.statistics))

    COUNT_SENTINEL = -98

    def test_process_increments_count(self):
        prog = self.prog()
        stat = prog.statistics[1]
        stat.item_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.item_count)

    def test_success_increments_success_count(self):
        prog = self.prog(True)
        stat = prog.statistics[1]
        stat.success_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.success_count)

    def test_failure_increments_failure_count(self):
        prog = self.prog(False)
        stat = prog.statistics[1]
        stat.failure_count = self.COUNT_SENTINEL

        prog.run(None)

        self.assertEqual(self.COUNT_SENTINEL + 1, stat.failure_count)

    def test_time_is_increased(self):
        prog = self.prog()
        stat = prog.statistics[1]
        run_time = stat.run_time

        prog.run(None)
        run_time2 = stat.run_time

        self.assertLess(run_time, run_time2)

        prog.run(None)
        run_time3 = stat.run_time

        self.assertLess(run_time2, run_time3)

    def die_prog(self):
        prog = m.Program([die, m.RETURN_TRUE])
        prog.runner.ensure_statistics(1)
        return prog

    def run_and_expect_exception(self, prog):
        try:
            prog.run(Data(None, None))
        except WellKnownException:
            # expected
            return
        else:
            self.fail('expected a WellKnownException from prog.run!')

    def test_on_exception_item_count_is_incremented(self):
        prog = self.die_prog()
        self.assertEqual(0, prog.statistics[0].item_count)

        self.run_and_expect_exception(prog)

        self.assertEqual(1, prog.statistics[0].item_count)

    def assert_exception_raised_attribute_is_untouched(
            self, attribute, initial_value):
        prog = self.die_prog()
        setattr(prog.statistics[0], attribute, initial_value)

        self.run_and_expect_exception(prog)

        self.assertEqual(initial_value, getattr(prog.statistics[0], attribute))

    def test_assert_exception_raised_attribute_is_untouched(self):
        try:
            self.assert_exception_raised_attribute_is_untouched(
                'item_count', 1)
        except AssertionError:
            # item_count supposed to be incremented, so the above should fail
            pass
        else:
            self.fail(
                'Expected a failure - item_count supposed to be incremented!')

    def test_on_exception_success_count_is_not_touched(self):
        self.assert_exception_raised_attribute_is_untouched('success_count', 2)

    def test_on_exception_failure_count_is_not_touched(self):
        self.assert_exception_raised_attribute_is_untouched('failure_count', 3)

    def test_on_exception_run_time_is_not_touched(self):
        self.assert_exception_raised_attribute_is_untouched('run_time', '')

    def test_on_exception_statistics_had_exception(self):
        prog = self.die_prog()
        self.assertFalse(prog.statistics[0].had_exception)

        self.run_and_expect_exception(prog)

        self.assertTrue(prog.statistics[0].had_exception)


class Test_decorators(unittest.TestCase):

    def assertEqualData(self, expected, actual):
        self.assertEqual(expected.id, actual.id)
        self.assertEqual(expected.payload, actual.payload)

    def test_rule(self):
        prog = m.Program([add1, m.RETURN_TRUE])

        self.assertEqualData(Data(id, 1), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 2), prog.run(Data(id, 1)))

    def test_branch(self):
        # program: convert an odd number to string 'odd',
        # an even number to 'even'
        prog = m.Program([
            m.IF (odd),
                const_odd,
            m.ELSE,
                const_even,
            m.ENDIF,
            m.RETURN_TRUE])

        self.assertEqualData(Data(id, 'even'), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 'odd'), prog.run(Data(id, 1)))

    def test_rule_branch(self):
        # program: add 2 to value if odd, else leave it
        prog = m.Program(
            [
            m.IF (increase_if_odd),
                add1,
            m.ENDIF,
            m.RETURN_TRUE,
            ])

        self.assertEqualData(Data(id, 0), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 3), prog.run(Data(id, 1)))

    def test_multiple_use_of_instructions(self):
        # program: convert an odd number to string 'odd',
        # an even number to 'even'
        prog = m.Program([
            odd,
            m.IF (odd),
                odd,
                const_even,
                const_odd,
            m.ELSE,
                odd,
                const_odd,
                const_even,
            m.ENDIF,
            m.RETURN_TRUE])

        self.assertEqualData(Data(id, 'even'), prog.run(Data(id, 0)))
        self.assertEqualData(Data(id, 'odd'), prog.run(Data(id, 1)))
