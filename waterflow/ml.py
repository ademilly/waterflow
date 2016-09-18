# -*- coding: utf-8 -*-


class ML(dict):

    def __init__(self, classifier=None, name=''):
        """Init ML object

        Keyword arguments:
        classifier  (object) -- sklearn-like classifier
        """

        self.clf = classifier
        self.meta = {
            'name': name
        }

    def fit(self, X, y):
        """Fit classifier to X, y

        Keyword arguments:
        X   ([entries x features] matrix) -- data
        y   ([entries] vector) -- target
        """

        self.clf.fit(X, y)
        return self

    def predict_proba(self, X):
        """Build the probabilities vector of X

        Keyword arguments:
        X   ([entries x features] matrix) -- data
        """

        self.meta['probabilities'] = self.clf.predict_proba(X)
        return self

    def metric(self, X, y, function, name):
        """Evaluate metric name from X and y with function

        Keyword arguments:
        X           ([entries x features] matrix) -- data
        y           ([entries] vector) -- target
        function    ((y_true, y_pred) => value) -- function evaluating metric
        name        (string) -- name for metric
        """

        self.predict_proba(X)

        self.meta[name] = function(y, self.meta['probabilities'])
        return self

    def __repr__(self):

        return str(
            {
                'classifier': self.clf,
                'meta': self.meta
            }
        )
