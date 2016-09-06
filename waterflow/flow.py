# -*- coding: utf-8 -*-
from collections import namedtuple
import itertools


"""Action namedtuple used to differentiate map transformation from filter op"""
Action = namedtuple('action', ['type', 'f'])


def read_csv(path, sep, line_terminator):
    """Yield a value from a csv file on disk

    Keyword arguments:
    path            (string) -- path to file
    sep             (string) -- separator substring
    line_terminator (string) -- line terminator substring
    """

    with open(path) as fin:

        for line in fin:
            yield line.replace('"', '').replace(line_terminator, '').split(sep)


def read_file(fin, sep, line_terminator):
    """Yield a value from a csv file in memory

    Keyword arguments:
    path            (string) -- path to file
    sep             (string) -- separator substring
    line_terminator (string) -- line terminator substring
    """

    for line in fin:
        yield line.replace('"', '').replace(line_terminator, '').split(sep)


def apply_action(data, action):
    """Compose successive generators from ordered map and filter transformation
    """

    if action.type == 'FLOW::MAP':
        return (action.f(_) for _ in data)

    elif action.type == 'FLOW::FILTER':
        return (_ for _ in data if action.f(_))

    else:
        return data


class Flow(object):

    def __init__(self):
        """Init Flow object"""

        self.data = None
        self.source = None
        self.chain = []

    def from_csv(self, path, sep=',', line_terminator='\n'):
        """Hook Flow to disk csv source

        Keyword arguments:
        path            (string) -- path to file
        sep             (string) -- separator substring
        line_terminator (string) -- line terminator substring
        """

        self.data = read_csv(path, sep, line_terminator)
        self.source = (read_csv, path, sep, line_terminator)

        return self

    def from_file(self, fin, sep=',', line_terminator='\n'):
        """Hook Flow to memory csv source

        Keyword arguments:
        path            (string) -- path to file
        sep             (string) -- separator substring
        line_terminator (string) -- line terminator substring
        """

        self.data = read_file(fin, sep, line_terminator)
        self.source = (read_file, fin, sep, line_terminator)

        return self

    def map(self, f):
        """Register a map transformation of function f

        Keyword arguments:
        f               (function) -- function with signature x => value
        """

        self.chain += [Action('FLOW::MAP', f)]

        return self

    def filter(self, f):
        """Register a filter transformation of function f

        Keyword arguments:
        f               (function) -- function with signature x => boolean
        """

        self.chain += [Action('FLOW::FILTER', f)]

        return self

    def reduce(self, f):
        """Reduce data with function f

        Keyword arguments:
        f               (function) -- function with signature a, b => value
        """

        return reduce(f, self.flow().data)

    def eval(self):
        """Evaluate dataset

        Apply all successive transformations and return resulting dataset
        """

        return [_ for _ in self.flow().data]

    def batch(self, size):
        """Evaluate dataset

        Apply all successive transformations and return resulting dataset slice

        Keyword arguments:
        size            (int) -- size of slice
        """

        return list(itertools.islice(self.flow().data, size))

    def flow(self):
        """Compose generator from successive registered actions"""

        for a in self.chain:
            self.data = apply_action(self.data, a)

        return self

    def __repr__(self):
        """Evaluate whole dataset and return string representation"""

        return '\n'.join(self.eval())
