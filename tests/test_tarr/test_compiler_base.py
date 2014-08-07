import unittest
import metl.tarr.compiler_base as m
from metl.tarr.compiler_base import (
    Instruction, BranchingInstruction,
    RETURN_TRUE, RETURN_FALSE,
    DEF, IF, ELSE, ELIF, ENDIF,
    IF_NOT, ELIF_NOT,
    DuplicateLabelError, UndefinedLabelError, BackwardReferenceError,
    FallOverOnDefineError, UnclosedProgramError, MissingEndIfError,
    MultipleElseError, ElIfAfterElseError)


class Div2(Instruction):

    def run(self, runner, state):
        return state / 2

Div2 = Div2()


class IsOdd(BranchingInstruction):

    def run(self, runner, state):
        runner.set_exit_status(state % 2 == 1)
        return state

IsOdd = IsOdd()


class Noop(Instruction):
    pass

Noop = Noop()


class ValueMixin(object):

    def __init__(self, value):
        self.value = value

    def clone(self):
        return self.__class__(self.value)

    @property
    def instruction_name(self):
        return '{}({})'.format(self.__class__.__name__, self.value)


class Eq(ValueMixin, BranchingInstruction):

    def run(self, runner, state):
        runner.set_exit_status(state == self.value)
        return state


class Const(ValueMixin, Instruction):

    def run(self, runner, state):
        return self.value


class Add(ValueMixin, Instruction):

    def run(self, runner, state):
        return state + self.value


Add1 = Add(1)


def next_instruction(i):
    return i.next_instruction(exit_status=True)


class Test_Path(unittest.TestCase):

    def test_NewPathAppender(self):
        # append
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.append(i1)
        path.append(i2)

        self.assertEqual(i2, next_instruction(i1))

    def test_InstructionAppender(self):
        # append
        i0 = m.Instruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path(m.InstructionAppender(i0))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, next_instruction(i0))

    def test_join(self):
        # p1 p1i1   p1i2
        # p2 p2i1 /
        p1i1 = m.Instruction()
        p1i2 = m.Instruction()
        p2i1 = m.Instruction()

        path1 = m.Path()
        path1.append(p1i1)
        path2 = m.Path()
        path2.append(p2i1)

        path1.join(path2)

        path1.append(p1i2)

        self.assertEqual(p1i2, next_instruction(p1i1))
        self.assertEqual(p1i2, next_instruction(p2i1))

    def test_join_a_joined_path(self):
        # p1 p1i1   p1i2
        # p2 p2i1 /
        # p3 p3i1 /
        # or
        # p2.join(p3)
        # p1.join(p2)
        p1i1 = m.Instruction()
        p1i2 = m.Instruction()
        p2i1 = m.Instruction()
        p3i1 = m.Instruction()

        path1 = m.Path()
        path1.append(p1i1)
        path2 = m.Path()
        path2.append(p2i1)
        path3 = m.Path()
        path3.append(p3i1)

        path2.join(path3)
        path1.join(path2)

        path1.append(p1i2)

        self.assertEqual(p1i2, next_instruction(p1i1))
        self.assertEqual(p1i2, next_instruction(p2i1))
        self.assertEqual(p1i2, next_instruction(p3i1))

    def test_join_to_a_closed_path(self):
        # p1 RETURN   p1i2
        # p2 p2i1 /
        p2i1 = m.Instruction()
        p1i2 = m.Instruction()

        path1 = m.Path()
        path1.append(m.Return())
        path1.close()
        self.assertTrue(path1.is_closed)
        path2 = m.Path()
        path2.append(p2i1)

        path1.join(path2)
        self.assertFalse(path1.is_closed)

        path1.append(p1i2)

        self.assertEqual(p1i2, next_instruction(p2i1))

    def test_TrueBranchAppender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)

        self.assertEqual(i1, bi.instruction_on_yes)

    def test_TrueBranchAppender_does_not_touch_no_path(self):
        sentinel = object()
        bi = m.BranchingInstruction()
        bi.instruction_on_no = sentinel
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(sentinel, bi.instruction_on_no)

    def test_TrueBranchAppender_resets_appender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.TrueBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, bi.instruction_on_yes)
        self.assertEqual(i2, next_instruction(i1))

    def test_FalseBranchAppender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)

        self.assertEqual(i1, bi.instruction_on_no)

    def test_FalseBranchAppender_does_not_touch_yes_path(self):
        sentinel = object()
        bi = m.BranchingInstruction()
        bi.instruction_on_yes = sentinel
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(sentinel, bi.instruction_on_yes)

    def test_FalseBranchAppender_resets_appender(self):
        bi = m.BranchingInstruction()
        i1 = m.Instruction()
        i2 = m.Instruction()

        path = m.Path()
        path.set_appender(m.FalseBranchAppender(path, bi))
        path.append(i1)
        path.append(i2)

        self.assertEqual(i1, bi.instruction_on_no)
        self.assertEqual(i2, next_instruction(i1))


class Test_Compiler(unittest.TestCase):

    def test_start_define_label_stores_label_with_index(self):
        c = m.Compiler()
        c.instructions = [None] * 4

        label1 = 'label1'
        c.start_define_label(label1)
        c.instructions += [None] * 6

        label2 = 'label2'
        c.start_define_label(label2)

        self.assertEqual([(label1, 4), (label2, 10)], c.labels_with_indices)


class Test_Program(unittest.TestCase):

    PROGRAM_CLASS = m.Program

    def program(self, program_spec):
        return self.PROGRAM_CLASS(program_spec)

    def test_instruction_sequence(self):
        prog = self.program([Add1, Div2, RETURN_TRUE])
        self.assertEqual(2, prog.run(3))

    def test_duplicate_definition_of_label_is_not_compilable(self):
        with self.assertRaises(DuplicateLabelError):
            self.program(
                [
                RETURN_TRUE,
                DEF('label'), Noop, RETURN_TRUE,
                DEF('label'), Noop])

    def test_incomplete_program_is_not_compilable(self):
        with self.assertRaises(UndefinedLabelError):
            self.program(['label', RETURN_TRUE])

    def test_incomplete_before_label_is_not_compilable(self):
        with self.assertRaises(FallOverOnDefineError):
            self.program([Noop, DEF('label'), Noop, RETURN_TRUE])

    def test_label_definition_within_label_def_is_not_compilable(self):
        with self.assertRaises(FallOverOnDefineError):
            self.program(
                [RETURN_TRUE, DEF('label'), DEF('label2'), RETURN_TRUE])

    def test_program_without_closing_return_is_not_compilable(self):
        with self.assertRaises(UnclosedProgramError):
            self.program([Noop])

    def test_backward_reference_is_not_compilable(self):
        with self.assertRaises(BackwardReferenceError):
            self.program([RETURN_TRUE, DEF('label'), Noop, 'label'])

    def test_branch_on_yes(self):
        prog = self.program(
            [
            IF (IsOdd),
                Add1,
            ELSE,
                'add2',
            ENDIF,
            RETURN_TRUE,

            DEF('add2'),
                Add1,
                Add1,
            RETURN_TRUE
            ])
        self.assertEqual(4, prog.run(3))
        self.assertEqual(6, prog.run(4))

    def test_branch_on_no(self):
        prog = self.program(
            [
            IF (IsOdd),
                Add1,
                Add1,
            ELSE,
                'add1',
            ENDIF,
            RETURN_TRUE,

            DEF('add1'),
                Add1,
            RETURN_TRUE
            ])
        self.assertEqual(5, prog.run(4))
        self.assertEqual(5, prog.run(3))

    def test_string_as_call_symbol(self):
        prog = self.program(
            [
            '+1', '+2', RETURN_TRUE,

            DEF('+2'), '+1', '+1', RETURN_TRUE,
            DEF('+1'), Add1, RETURN_TRUE
            ])
        self.assertEqual(3, prog.run(0))

    def test_compilation_with_missing_ENDIF_is_not_possible(self):
        with self.assertRaises(MissingEndIfError):
            self.program([IF (IsOdd), RETURN_TRUE])

    def test_compilation_with_multiple_ELSE_is_not_possible(self):
        with self.assertRaises(MultipleElseError):
            self.program([IF (IsOdd), ELSE, ELSE, ENDIF, RETURN_TRUE])

    def test_IF(self):
        prog = self.program([
            IF (IsOdd),
                Add1,
            ENDIF,
            Add1,
            RETURN_TRUE])

        self.assertEqual(1, prog.run(0))
        self.assertEqual(3, prog.run(1))
        self.assertEqual(3, prog.run(2))

    def test_IF_NOT(self):
        prog = self.program([
            IF_NOT (Eq('value')),
                Const('IF_NOT'),
                IF_NOT (Eq('value')),
                    Const('IF_NOT'),
                    # here we have the bug: this path is merged to here,
                    # but outside this path is *ignored*,
                    # its ELSE path is merged
                ENDIF,
            ENDIF,
            Add('.'),
            RETURN_TRUE])

        self.assertEqual('value.', prog.run('value'))
        self.assertEqual('IF_NOT.', prog.run('?'))

    def test_ELSE(self):
        prog = self.program([
            IF (IsOdd),
            ELSE,
                Add1,
            ENDIF,
            Add1,
            RETURN_TRUE])

        self.assertEqual(2, prog.run(0))
        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(2))

    def test_IF_ELSE(self):
        prog = self.program([
            IF (IsOdd),
                Add1,
            ELSE,
                Add1,
                Add1,
            ENDIF,
            Add1,
            RETURN_TRUE])

        self.assertEqual(3, prog.run(0))
        self.assertEqual(3, prog.run(1))
        self.assertEqual(5, prog.run(2))

    def test_IF_ELIF_ELSE(self):
        prog = self.program([
            IF (Eq('value')),
                Const('IF'),
            ELIF (Eq('variant1')),
                Const('ELIF1'),
            ELIF (Eq('variant2')),
                Const('ELIF2'),
            ELSE,
                Const('ELSE'),
            ENDIF,
            Add('.'),
            RETURN_TRUE])

        self.assertEqual('IF.', prog.run('value'))
        self.assertEqual('ELIF1.', prog.run('variant1'))
        self.assertEqual('ELIF2.', prog.run('variant2'))
        self.assertEqual('ELSE.', prog.run('unknown'))

    def test_IF_ELIF_NOT_ELSE(self):
        prog = self.program([
            IF (Eq('value')),
                Const('IF'),
            ELIF_NOT (Eq('variant')),
                # not variant
                Const('ELIF_NOT'),
            ELSE,
                # variant
                Const('ELSE'),
            ENDIF,
            Add('.'),
            RETURN_TRUE])

        self.assertEqual('IF.', prog.run('value'))
        self.assertEqual('ELIF_NOT.', prog.run('not_variant'))
        self.assertEqual('ELSE.', prog.run('variant'))

    def test_IF_NOT_ELSE(self):
        prog = self.program([
            IF_NOT (Eq('value')),
                # not value
                Const('IF_NOT'),
            ELSE,
                # value
                Const('ELSE'),
            ENDIF,
            Add('.'),
            RETURN_TRUE])

        self.assertEqual('IF_NOT.', prog.run('unkown'))
        self.assertEqual('ELSE.', prog.run('value'))

    def test_compilation_with_ELIF_after_ELSE_is_not_possible(self):
        with self.assertRaises(ElIfAfterElseError):
            self.program([IF (IsOdd), ELSE, ELIF (IsOdd), ENDIF, RETURN_TRUE])

    def test_embedded_IFs(self):
        prog = self.program([
            IF (IsOdd),
                Add1,
                Div2,
                IF (IsOdd),
                    Add1,
                ENDIF,
            ELSE,
                Add1,
                Add1,
            ENDIF,
            Div2,
            RETURN_TRUE])

        self.assertEqual(1, prog.run(0))
        self.assertEqual(1, prog.run(1))
        self.assertEqual(2, prog.run(2))
        self.assertEqual(1, prog.run(3))
        self.assertEqual(3, prog.run(4))
        self.assertEqual(2, prog.run(5))

    def test_macro_return_yes(self):
        prog = self.program(
            [
            IF ('odd?'),
                Add1,
            ENDIF,
            RETURN_TRUE,

            DEF ('odd?'),
                IF (IsOdd),
                    RETURN_TRUE,
                ELSE,
                    RETURN_FALSE,
                ENDIF
            ])

        self.assertEqual(2, prog.run(1))
        self.assertEqual(4, prog.run(3))

    def test_macro_return_no(self):
        prog = self.program(
            [
            IF ('odd?'),
                Add1,
            ENDIF,
            RETURN_TRUE,

            DEF ('odd?'),
                IF (IsOdd),
                    RETURN_TRUE,
                ELSE,
                    RETURN_FALSE,
                ENDIF
            ])

        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(4))

    # used in the next 2 tests
    complex_prog_spec = [
        IF ('even?'),
            RETURN_TRUE,
        ELSE,
            Add1,
        ENDIF,
        RETURN_TRUE,

        DEF ('even?'),
            IF (IsOdd),
                RETURN_FALSE,
            ELSE,
                RETURN_TRUE,
            ENDIF,
    ]

    def test_macro_return(self):
        prog = self.program(self.complex_prog_spec)

        self.assertEqual(2, prog.run(1))
        self.assertEqual(2, prog.run(2))
        self.assertEqual(4, prog.run(3))
        self.assertEqual(4, prog.run(4))

    def test_instruction_index(self):
        prog = self.program(self.complex_prog_spec)

        indices = [i.index for i in prog.instructions]
        self.assertEqual(range(len(prog.instructions)), indices)

    def test_joining_into_a_closed_path_reopens_it(self):
        with self.assertRaises(UnclosedProgramError):
            self.program(
                [
                IF(IsOdd),
                    RETURN_TRUE,
                ENDIF,
                ])

    def test_sub_programs(self):
        prog = self.program(
            [
            Add1,
            m.RETURN_TRUE,
            m.DEF ('x1'),
            m.RETURN_TRUE,
            m.DEF ('x2'),
            m.RETURN_TRUE,
            m.DEF ('x3'),
            Add1,
            Add1,
            m.RETURN_TRUE,
            ])

        sub_programs = iter(prog.sub_programs())
        sub_program = sub_programs.next()
        self.assertEqual(None, sub_program[0])
        self.assertEqual(2, len(sub_program[1]))  # Add1, RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x1', sub_program[0])
        self.assertEqual(1, len(sub_program[1]))  # RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x2', sub_program[0])
        self.assertEqual(1, len(sub_program[1]))  # RETURN

        sub_program = sub_programs.next()
        self.assertEqual('x3', sub_program[0])
        self.assertEqual(3, len(sub_program[1]))  # Add1, Add1, RETURN

        with self.assertRaises(StopIteration):
            sub_programs.next()

    def program_for_visiting_with_all_features(self):
        return self.program(
            [
            'x', m.RETURN_TRUE,

            m.DEF ('x'),
                m.IF (IsOdd),
                    Add1,
                m.ENDIF,
                m.RETURN_TRUE
            ])

    def check_visitor(self, visitor):
        prog = self.program_for_visiting_with_all_features()
        prog.accept(visitor)

    def test_remembering_visitor_is_an_accepted_visitor(self):
        self.check_visitor(RememberingVisitor())

    def test_visit(self):
        prog = self.program_for_visiting_with_all_features()

        remembering_visitor = RememberingVisitor()

        prog.accept(remembering_visitor)

        i = prog.instructions
        self.assertEqual(
            [
            ('subprogram', None),
                ('call', i[0]),
                ('return', i[1]),
            ('end', None),

            ('subprogram', 'x'),
                ('branch', i[2]),
                ('instruction', i[3]),
                ('return', i[4]),
            ('end', 'x')
            ], remembering_visitor.calls)


class RememberingVisitor(m.ProgramVisitor):

    calls = None

    def __init__(self):
        self.calls = []

    def enter_subprogram(self, label, instructions):
        self.calls.append(('subprogram', label))

    def leave_subprogram(self, label):
        self.calls.append(('end', label))

    def visit_call(self, i_call):
        self.calls.append(('call', i_call))

    def visit_return(self, i_return):
        self.calls.append(('return', i_return))

    def visit_instruction(self, instruction):
        self.calls.append(('instruction', instruction))

    def visit_branch(self, i_branch):
        self.calls.append(('branch', i_branch))
