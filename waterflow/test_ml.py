from sklearn.naive_bayes import GaussianNB, BernoulliNB

from ml import ML


class TestML:

    def test_init(self):

        ml = ML()

        assert hasattr(ml, 'clf')
        assert ml.clf is None
        assert ml.meta == {}

    def test_init_with_object(self):

        ml = ML(classifier=GaussianNB())

        assert hasattr(ml, 'clf')
        assert hasattr(ml.clf, 'fit')
        assert hasattr(ml.clf, 'predict_proba')

    def test_fit(self, numerics):

        X = numerics
        y = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]

        ml = ML(classifier=GaussianNB())
        assert ml.fit(X, y) == ml

    def test_predict(self, numerics):

        X = numerics
        y = [0, 1, 0, 1, 0, 1, 0, 1, 0, 1]

        ml = ML(classifier=GaussianNB())
        assert ml.fit(X, y) == ml

        ml.predict_proba(X)
        assert 'probabilities' in ml.meta.keys()
        assert len(ml.meta['probabilities']) == 10

    def test_logloss(self, booleans):

        X = booleans
        y = [1, 0, 0, 1]

        ml = ML(classifier=BernoulliNB())
        assert ml.fit(X, y) == ml

        ml.log_loss(X, y)
        assert 'log_loss' in ml.meta
