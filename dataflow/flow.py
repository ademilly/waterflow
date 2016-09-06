# -*- coding: utf-8 -*-
from collections import namedtuple
import itertools


Action = namedtuple('action', ['type', 'f'])


def read_csv(path, sep, line_terminator):

    with open(path) as fin:

        for line in fin:
            yield line.replace('"', '').replace(line_terminator, '').split(sep)


def read_file(fin, sep, line_terminator):

    for line in fin:
        yield line.replace('"', '').replace(line_terminator, '').split(sep)


def apply_action(data, action):

    if action.type == 'FLOW::MAP':
        return (action.f(_) for _ in data)

    elif action.type == 'FLOW::FILTER':
        return (_ for _ in data if action.f(_))

    else:
        return data


class Flow(object):

    def __init__(self, init=None):

        self.data = init.data if init is not None else None
        self.source = init.source if init is not None else None
        self.chain = init.chain if init is not None else []

    def from_csv(self, path, sep=',', line_terminator='\n'):

        self.data = read_csv(path, sep, line_terminator)
        self.source = (read_csv, path, sep, line_terminator)

        return self

    def from_file(self, fin, sep=',', line_terminator='\n'):

        self.data = read_file(fin, sep, line_terminator)
        self.source = (read_file, fin, sep, line_terminator)

        return self

    def map(self, f):

        self.chain += [Action('FLOW::MAP', f)]

        return self

    def filter(self, f):

        self.chain += [Action('FLOW::FILTER', f)]

        return self

    def reduce(self, f):

        return reduce(f, self.flow().data)

    def eval(self):

        return [_ for _ in self.flow().data]

    def batch(self, size):

        return list(itertools.islice(self.flow().data, size))

    def flow(self):

        for a in self.chain:
            self.data = apply_action(self.data, a)

        return self

    def __repr__(self):

        return '\n'.join([
            str(_) for _ in self.flow().data
        ])
