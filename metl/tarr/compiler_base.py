class DuplicateLabelError(Exception):
    pass


class UndefinedLabelError(Exception):
    pass


class BackwardReferenceError(Exception):
    pass


class FallOverOnDefineError(Exception):
    pass


class UnclosedProgramError(Exception):
    pass


class MissingEndIfError(Exception):
    pass


class MultipleElseError(Exception):
    pass


class ElIfAfterElseError(Exception):
    pass


class Compilable(object):

    def compile(self, compiler):
        pass


class InstructionBase(Compilable):

    index = None

    # run time
    def run(self, runner, state):
        return state

    def next_instruction(self, exit_status):
        return None

    # compile time
    def set_next_instruction(self, instruction):
        pass

    def compile(self, compiler):
        compiler.add_instruction(self.clone())

    def clone(self):
        return self.__class__()

    # visitor
    def accept(self, visitor):
        pass


class Instruction(InstructionBase):

    _next_instruction = None

    def next_instruction(self, exit_status):
        return self._next_instruction

    def set_next_instruction(self, instruction):
        self._next_instruction = instruction

    def accept(self, visitor):
        visitor.visit_instruction(self)


class Return(InstructionBase):

    return_value = None

    def __init__(self, return_value=True):
        self.return_value = bool(return_value)

    def next_instruction(self, exit_status):
        return None

    def run(self, runner, state):
        if self.return_value is not None:
            runner.set_exit_status(self.return_value)

        return state

    def compile(self, compiler):
        super(Return, self).compile(compiler)
        compiler.path.close()

    def clone(self):
        return self.__class__(self.return_value)

    def accept(self, visitor):
        visitor.visit_return(self)


RETURN_TRUE = Return(return_value=True)
RETURN_FALSE = Return(return_value=False)


class BranchingInstruction(InstructionBase):

    instruction_on_yes = None
    instruction_on_no = None

    def next_instruction(self, exit_status):
        if exit_status:
            return self.instruction_on_yes
        return self.instruction_on_no

    def set_next_instruction(self, instruction):
        if self.instruction_on_yes is None:
            self.instruction_on_yes = instruction
        if self.instruction_on_no is None:
            self.instruction_on_no = instruction

    def set_instruction_on_yes(self, instruction):
        self.instruction_on_yes = instruction

    def set_instruction_on_no(self, instruction):
        self.instruction_on_no = instruction

    def accept(self, visitor):
        visitor.visit_branch(self)


class Define(Compilable):

    label = None

    def __init__(self, label):
        self.label = label

    def compile(self, compiler):
        if compiler.path.is_open:
            raise FallOverOnDefineError

        compiler.start_define_label(self.label)

DEF = Define


class Runner(object):

    exit_status = None

    def set_exit_status(self, value):
        self.exit_status = value

    def run_instruction(self, instruction, state):
        return instruction.run(self, state)

    def run(self, start_instruction, state):
        instruction = start_instruction

        while instruction:
            state = self.run_instruction(instruction, state)
            instruction = instruction.next_instruction(self.exit_status)

        return state


class Call(BranchingInstruction):

    label = None
    start_instruction = None

    def __init__(self, label):
        self.label = label

    def run(self, runner, state):
        return runner.run(self.start_instruction, state)

    def compile(self, compiler):
        super(Call, self).compile(compiler)
        compiler.register_linker(
            self.label, compiler.last_instruction.set_start_instruction)

    def set_start_instruction(self, instruction):
        self.start_instruction = instruction

    def clone(self):
        return self.__class__(self.label)

    def accept(self, visitor):
        visitor.visit_call(self)


class CompileIf(Compilable):

    def __init__(self, branch_instruction):
        self.branch_instruction = branch_instruction

    def compile(self, compiler):
        branch_instruction = compiler.compilable(self.branch_instruction)
        branch_instruction.compile(compiler)

        if_path, else_path = compiler.path.split(compiler.last_instruction)
        compiler.control_stack.append(
            IfElseControlFrame(compiler.path, if_path, else_path))

        compiler.path = if_path

IF = CompileIf


class CompileIfNot(CompileIf):

    def compile(self, compiler):
        super(CompileIfNot, self).compile(compiler)
        frame = compiler.control_stack.pop()

        # swap if_path and else_path
        frame.if_path, frame.else_path = frame.else_path, frame.if_path

        compiler.path = frame.if_path
        compiler.control_stack.append(frame)

IF_NOT = CompileIfNot


class CompileElIf(Compilable):

    def __init__(self, branch_instruction):
        self.branch_instruction = branch_instruction

    def compile(self, compiler):
        frame = compiler.control_stack.pop()

        if frame.else_used:
            raise ElIfAfterElseError
        if frame.elif_path is not None:
            frame.if_path.join(frame.elif_path)

        compiler.path = frame.else_path
        branch_instruction = compiler.compilable(self.branch_instruction)
        branch_instruction.compile(compiler)

        frame.elif_path, frame.else_path = frame.else_path.split(
            compiler.last_instruction)
        compiler.path = frame.elif_path

        compiler.control_stack.append(frame)

ELIF = CompileElIf


class CompileElIfNot(CompileElIf):

    def compile(self, compiler):
        super(CompileElIfNot, self).compile(compiler)
        frame = compiler.control_stack.pop()

        # swap elif_path and else_path
        frame.elif_path, frame.else_path = frame.else_path, frame.elif_path
        compiler.path = frame.elif_path

        compiler.control_stack.append(frame)

ELIF_NOT = CompileElIfNot


class CompileElse(Compilable):

    def compile(self, compiler):
        frame = compiler.control_stack.pop()

        if frame.else_used:
            raise MultipleElseError

        compiler.path = frame.else_path
        frame.else_used = True

        compiler.control_stack.append(frame)

ELSE = CompileElse()


class CompileEndIf(Compilable):

    def compile(self, compiler):
        frame = compiler.control_stack.pop()

        frame.main_path.join(frame.if_path)
        if frame.elif_path is not None:
            frame.main_path.join(frame.elif_path)
        frame.main_path.join(frame.else_path)

        compiler.path = frame.main_path

ENDIF = CompileEndIf()


class Appender(object):
    '''Knows how to continue a path

    Continue = how to append a new instruction to
    '''

    def append(self, instruction):
        pass


class NoopAppender(Appender):
    pass


class InstructionAppender(Appender):
    '''Appends to previous instruction
    '''

    def __init__(self, instruction):
        self.last_instruction = instruction

    def append(self, instruction):
        self.last_instruction.set_next_instruction(instruction)
        self.last_instruction = instruction


class NewPathAppender(Appender):
    '''Appends to empty path
    '''

    def __init__(self, path):
        self.path = path

    def append(self, instruction):
        self.path.set_appender(InstructionAppender(instruction))


class DefineAppender(Appender):
    '''Defines the label when appending an instruction
    '''

    def __init__(self, compiler, path, label):
        self.compiler = compiler
        self.path = path
        self.label = label

    def append(self, instruction):
        self.path.set_appender(InstructionAppender(instruction))
        self.compiler.complete_define_label(self.label, instruction)


class TrueBranchAppender(Appender):
    '''Appends to True side of a branch instruction
    '''

    def __init__(self, path, branch_instruction):
        self.path = path
        self.branch_instruction = branch_instruction

    def append(self, instruction):
        self.branch_instruction.set_instruction_on_yes(instruction)
        self.path.set_appender(InstructionAppender(instruction))


class FalseBranchAppender(Appender):
    '''Appends to False side of a branch instruction
    '''

    def __init__(self, path, branch_instruction):
        self.path = path
        self.branch_instruction = branch_instruction

    def append(self, instruction):
        self.branch_instruction.set_instruction_on_no(instruction)
        self.path.set_appender(InstructionAppender(instruction))


class JoinAppender(Appender):

    def __init__(self, path, merged_path):
        self.path = path
        self.orig_appender = path.appender
        self.merged_path = merged_path

    def append(self, instruction):
        self.merged_path.append(instruction)
        self.path.set_appender(InstructionAppender(instruction))
        self.orig_appender.append(instruction)


class Path(object):
    '''
    An execution path.

    Instructions can be appended to it and other paths can be joined in.
    Real work happens in appenders, which are changed as needed.
    '''

    def __init__(self, appender=None):
        self.appender = appender or NewPathAppender(self)
        self._closed = False

    def append(self, instruction):
        self.appender.append(instruction)

    def split(self, branch_instruction):
        self.close()
        true_path = Path()
        true_path.set_appender(
            TrueBranchAppender(true_path, branch_instruction))
        false_path = Path()
        false_path.set_appender(
            FalseBranchAppender(false_path, branch_instruction))
        return true_path, false_path

    def join(self, path):
        self._closed = self.is_closed and path.is_closed
        self.appender = JoinAppender(self, path)

    def set_appender(self, appender):
        self.appender = appender

    @property
    def is_open(self):
        return not self._closed

    @property
    def is_closed(self):
        return self._closed

    def close(self):
        self.set_appender(NoopAppender())
        self._closed = True


class IfElseControlFrame(object):

    '''
    Records paths for IF, IF_NOT, ELIF, ELIF_NOT, ELSE, ENDIF

    * main_path: stored when entering IF or IF_NOT, restored on ENDIF,
                 not used in between
    * if_path:   the first conditional path to define
    * elif_path: optional, used by ELIF to keep the current conditional path
    * else_path: ELSE branch goes here

    ENDIF merges if_path, elif_path, else_path back to main_path,
    before restoring
    '''

    def __init__(self, main_path, if_path, else_path):
        self.main_path = main_path
        self.if_path = if_path
        self.elif_path = None
        self.else_path = else_path
        self.else_used = False


class Compiler(object):

    instructions = None
    control_stack = None
    path = None

    labels_with_indices = []
    previous_labels = None
    linkers = None

    @property
    def last_instruction(self):
        return self.instructions[-1]

    def __init__(self):
        self.control_stack = []
        self.path = Path()
        self.instructions = list()
        self.labels_with_indices = []
        self.previous_labels = set()
        self.linkers = dict()

    def compile(self, program_spec):
        for instruction in program_spec:
            compilable = self.compilable(instruction)
            compilable.compile(self)

        if self.control_stack:
            raise MissingEndIfError

        if self.linkers:
            raise UndefinedLabelError(set(self.linkers.keys()))

        if self.path.is_open:
            raise UnclosedProgramError

    def compilable(self, instruction):
        if isinstance(instruction, basestring):
            return Call(instruction)

        return instruction

    def add_instruction(self, instruction):
        self.path.append(instruction)
        instruction.index = len(self.instructions)
        self.instructions.append(instruction)

    def start_define_label(self, label):
        if label in self.previous_labels:
            raise DuplicateLabelError

        self.labels_with_indices.append((label, len(self.instructions)))
        self.path = Path()
        # can not resolve label references yet, as the content
        # (first instruction) is not known yet
        self.path.set_appender(DefineAppender(self, self.path, label))

    def complete_define_label(self, label, instruction):
        self.previous_labels.add(label)
        if label in self.linkers:
            for linker in self.linkers[label]:
                linker(instruction)
            del self.linkers[label]

    def register_linker(self, label, linker):
        if label in self.previous_labels:
            raise BackwardReferenceError

        self.linkers.setdefault(label, []).append(linker)


class ProgramVisitor(object):

    def enter_subprogram(self, label, instructions):
        pass

    def leave_subprogram(self, label):
        pass

    def visit_call(self, i_call):
        pass

    def visit_return(self, i_return):
        pass

    def visit_instruction(self, instruction):
        pass

    def visit_branch(self, i_branch):
        pass


class Program(object):

    instructions = None
    runner = None

    def __init__(self, program_spec):
        self.labels_with_indices = None
        self.compile(program_spec)

    def run(self, state):
        return self.runner.run(self.start_instruction, state)

    def compile(self, program_spec):
        compiler = Compiler()
        compiler.compile(program_spec)
        self.init(compiler.instructions, compiler.labels_with_indices)

    def init(self, instructions, labels_with_indices):
        self.instructions = instructions
        self.labels_with_indices = labels_with_indices
        self.runner = self.make_runner()

    @property
    def start_instruction(self):
        return self.instructions[0]

    def make_runner(self):
        return Runner()

    def sub_programs(self):
        (label, index) = (None, 0)
        i = 0
        while i < len(self.labels_with_indices):
            (next_label, next_index) = self.labels_with_indices[i]
            yield (label, self.instructions[index:next_index])
            (label, index) = (next_label, next_index)
            i += 1

        yield (label, self.instructions[index:])

    def accept(self, visitor):
        for (label, instructions) in self.sub_programs():
            visitor.enter_subprogram(label, instructions)
            for i in instructions:
                i.accept(visitor)
            visitor.leave_subprogram(label)
