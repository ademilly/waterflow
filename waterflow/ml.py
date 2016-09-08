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

    def log_loss(self, X, y):
        """Evaluate log_loss metric from X and y

        Keyword arguments:
        X   ([entries x features] matrix) -- data
        y   ([entries] vector) -- target
        """

        if 'probabilities' not in self.meta.keys():
            self.predict_proba(X)

        self.meta['log_loss'] = log_loss(
            y, self.meta['probabilities']
        )
        return self
