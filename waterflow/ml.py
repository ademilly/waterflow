from sklearn.metrics import log_loss


class ML(dict):

    def __init__(self, classifier=None):
        """Init ML object

        Keyword arguments:
        classifier  (object) -- sklearn-like classifier
        """

        self.clf = classifier
        self.meta = {}

    def fit(self, X, y):

        self.clf.fit(X, y)
        return self

    def predict_proba(self, X):

        self.meta['probabilities'] = self.clf.predict_proba(X)
        return self

    def log_loss(self, X, y):

        if 'probabilities' not in self.meta.keys():
            self.predict_proba(X)

        self.meta['log_loss'] = log_loss(
            y, self.meta['probabilities']
        )
        return self
