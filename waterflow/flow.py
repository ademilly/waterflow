# -*- coding: utf-8 -*-

import itertools
import numpy
import cloudpickle as pickle

from ml import ML
from action import Action


class Flow(object):

    def __init__(self, seed=1, flow=None):
        """Init Flow object"""

        self.data = None if flow is None else flow.source.read()
        self.source = None if flow is None else flow.source
        self.chain = [] if flow is None else list(flow.chain)

        self.header = [] if flow is None else list(flow.header)
        self.clfs = {} if flow is None else dict(flow.clfs)

        self.seed = seed if flow is None else flow.seed

        self.random_state = numpy.random.RandomState(self.seed)

    def from_source(self, source):
        """Set data source"""

        self.data = source.read()
        self.source = source

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

        self.chain += [Action('FLOW::REDUCE', f)]

        return self

    def split(self, n, which):
        """Split data in n parts and select which part

        Keyword arguments:
        n               (int) -- number of parts
        which           (int) -- index of the selected part [0 ; n-1]
        """

        self.chain += [Action('FLOW::SPLIT', (n, which))]

        return self

    def eval(self):
        """Evaluate dataset

        Apply all successive transformations and return resulting dataset
        """

        return [_ for _ in self.run().data]

    def batch(self, size):
        """Evaluate dataset

        Apply all successive transformations and return resulting dataset slice

        Keyword arguments:
        size            (int) -- size of slice
        """

        return list(itertools.islice(self.run().data, size))

    def reload(self):
        """Renew self.data and self.random_state"""

        self.random_state = numpy.random.RandomState(self.seed)
        self.data = self.source.read()

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

    def register_ml(self, ml):
        """Register a new ML object in flow"""

        self.clfs[
            ml.meta['name']
        ] = ML(classifier=ml.clf, name=ml.meta['name'])

        return self

    def fit_with(self, name, target):
        """Fit classifier to data"""

        X, y = self.tensorize(target)
        self.clfs[name].fit(X, y)

        return self

    def score_with(self, name, target, metric_function, metric_name):
        """Score data with clf named name using metric"""

        X, y = self.tensorize(target)

        self.clfs[name].metric(X, y, metric_function, metric_name)

        return self

    def apply_action(self, data, action):
        """Compose successive generators
        from ordered map and filter transformation
        """

        if action.type == 'FLOW::MAP':
            return (action.payload(_) for _ in data)

        elif action.type == 'FLOW::FILTER':
            return (_ for _ in data if action.payload(_))

        elif action.type == 'FLOW::SPLIT':
            for a in [Action(
                'FLOW::MAP',
                lambda x: x + [
                    self.random_state.choice(
                        range(action.payload[0])
                    )
                ]),
                Action('FLOW::FILTER', lambda x: x[-1] == action.payload[1]),
                Action('FLOW::MAP', lambda x: x[:-1])
            ]:
                data = self.apply_action(data, a)
            return data

        elif action.type == 'FLOW::REDUCE':
            return [reduce(action.payload, data)]

        else:
            return data

    def run(self):
        """Compose generator from successive registered actions"""

        for a in self.chain:
            self.data = self.apply_action(self.data, a)

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
