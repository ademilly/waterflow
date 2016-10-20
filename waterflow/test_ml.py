# -*- coding: utf-8 -*-

from ml import Classifier


class TestML:

    def test_init(self):

        clf = Classifier()

        assert hasattr(clf, 'clf')
        assert clf.clf is None
        assert clf.meta == {'name': ''}
