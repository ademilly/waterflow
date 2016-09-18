# -*- coding: utf-8 -*-

from ml import ML


class TestML:

    def test_init(self):

        ml = ML()

        assert hasattr(ml, 'clf')
        assert ml.clf is None
        assert ml.meta == {'name': ''}
