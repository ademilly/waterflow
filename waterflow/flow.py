# -*- coding: utf-8 -*-

import itertools
import numpy
import cloudpickle as pickle

from ml import ML
from action import Action


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

    fin.seek(0)

    for line in fin:
        yield line.replace('"', '').replace(line_terminator, '').split(sep)


class Flow(object):

    def __init__(self, seed=1, flow=None):
        """Init Flow object"""

        self.data = None if flow is None else flow.regenerate_generator()
        self.source = None if flow is None else flow.source
        self.chain = [] if flow is None else list(flow.chain)

        self.header = [] if flow is None else list(flow.header)
        self.clfs = {} if flow is None else dict(flow.clfs)

        self.seed = seed if flow is None else flow.seed

        self.random_state = numpy.random.RandomState(self.seed)

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

    def apply_action(self, data, action):
        """Compose successive generators from ordered map and filter transformation
        """

        if action.type == 'FLOW::MAP':
            return (action.f(_) for _ in data)

        elif action.type == 'FLOW::FILTER':
            return (_ for _ in data if action.f(_))

        elif action.type == 'FLOW::SPLIT':
            for a in [Action(
                'FLOW::MAP',
                lambda x: x + [
                    self.random_state.choice(
                        ['left', 'right'],
                        p=[action.f[0], 1.0 - action.f[0]]
                    )
                ]), Action('FLOW::FILTER', lambda x: x[-1] == action.f[1]),
                Action('FLOW::MAP', lambda x: x[:-1])
            ]:
                data = self.apply_action(data, a)
            return data

        else:
            return data

    def flow(self):
        """Compose generator from successive registered actions"""

        for a in self.chain:
            self.data = self.apply_action(self.data, a)

        return self

    def regenerate_generator(self):
        """Renew the self.data generator"""

        return self.source[0](*self.source[1:])

    def reload(self):
        """Renew self.data and self.random_state"""

        self.random_state = numpy.random.RandomState(self.seed)
        self.data = self.regenerate_generator()

        return self

    def split(self, rate=0.5, on='left'):
        """Split data long rate and select one part

        Keyword arguments:
        rate    (float) -- number between 0 and 1 splitting data
        on      ('left' or 'right') -- choose which part of the split to take
        """

        self.chain += [Action('FLOW::SPLIT', (rate, on))]
        # self.map(lambda x: x + [
        #     self.random_state.choice(['left', 'right'], p=[rate, 1.0 - rate])
        # ]).filter(lambda x: x[-1] == on).map(lambda x: x[:-1])

        return self

    def __repr__(self):
        """Evaluate whole dataset and return string representation"""

        return '\n'.join([
            str(_) for _ in self.eval()
        ]) if self.data is not None else 'None'

    def tensorize(self, target=''):
        """Return data and target column if the latter is given"""

        M = self.eval()
        if target != '':
            return [
                [x for i, x in enumerate(_) if i != self.header.index(target)]
                for _ in M
            ], [
                [
                    x for i, x in enumerate(_)
                    if i == self.header.index(target)
                ][0] for _ in M
            ]
        else:
            return M

    def header_is(self, names=[]):
        """Set header"""

        self.header = names
        return self

    def register_clf(self, ml):
        """Register classifier from another flow"""

        self.clfs[
            ml.meta['name']
        ] = ML(classifier=ml.clf, name=ml.meta['name'])

        return self

    def register_ml(self, ml):
        """Register a new ML object in flow"""

        self.clfs[
            ml.meta['name']
        ] = ml

        return self

    def fit_with(self, classifier, name='', target=''):
        """Fit classifier to data"""

        X, y = self.tensorize(target)
        clf = ML(classifier, name)

        clf.fit(X, y)
        return self.register_ml(clf)

    def score_with(self, name, target, metric_function, metric_name):
        """Score data with clf named name using metric"""

        X, y = self.tensorize(target)

        self.clfs[name].metric(X, y, metric_function, metric_name)

        return self

    def save(self, pickle_path):

        with open(pickle_path, 'wb') as fout:
            pickle.dump((
                self.source, self.chain, self.header, self.clfs, self.seed
            ), fout)

        return self

    def load(self, pickle_path):

        with open(pickle_path, 'rb') as fin:
            obj = pickle.load(fin)

            self.source, self.chain, self.header, self.clfs, self.seed = obj

        self.reload()

        return self
