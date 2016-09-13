# -*- coding: utf-8 -*-

from sklearn.naive_bayes import BernoulliNB

from flow import Flow


class TestFlow:

    def test_init(self):
        """Test initialization"""

        flow = Flow()
        assert hasattr(flow, 'data')
        assert hasattr(flow, 'source')
        assert hasattr(flow, 'chain')
        assert flow.data is None

    def test_filter(self, string_dataset):
        """Test filter registering"""

        flow = Flow().from_file(string_dataset.open()).filter(
            lambda x: 'line' not in x
        )

        assert flow.chain[0].type == 'FLOW::FILTER'
        assert flow.batch(1)[0] == ['0', '1', '2', '3', '4']

    def test_map(self, numeric_dataset):
        """Test map registering"""

        flow = Flow().from_file(numeric_dataset.open()).map(
            lambda x: [float(_) for _ in x]
        ).map(
            lambda x: [2 * _ for _ in x]
        )

        assert flow.chain[0].type == 'FLOW::MAP'
        assert flow.batch(1)[0] == [0, 0, 2, 4, 6, 8]

    def test_reduce(self, numeric_dataset):
        """Test reduce"""

        flow = Flow().from_file(numeric_dataset.open()).map(
            lambda x: [float(_) for _ in x]
        ).map(lambda x: sum(x))

        assert flow.reduce(lambda a, b: a + b) == 145

    def test_reload(self, numeric_dataset):
        """Test reloading"""

        flow = Flow().from_file(numeric_dataset.open()).map(lambda x: 1)

        assert flow.reduce(lambda a, b: a + b) == 10
        assert flow.reload().reduce(lambda a, b: a + b) == 10

    def test_split_train_test(self, numeric_dataset):
        """Test split"""

        flow = Flow(seed=42)

        flow = flow.from_file(numeric_dataset.open()).map(
            lambda x: [float(_) for _ in x]
        )

        flow_left = Flow(flow=flow).map(
            lambda x: x + [
                flow_left.random_state.choice([0, 1], p=[0.3, 0.7])
            ]
        ).filter(
            lambda x: x[-1] == 0
        )

        flow_right = Flow(flow=flow).map(
            lambda x: x + [
                flow_right.random_state.choice([0, 1], p=[0.3, 0.7])
            ]
        ).filter(
            lambda x: x[-1] == 1
        )

        assert flow_left.map(lambda x: 1).reduce(lambda a, b: a + b) == 3
        assert flow_right.map(lambda x: 1).reduce(lambda a, b: a + b) == 7

    def test_fit(self, boolean_dataset):
        """Test fit"""

        flow = Flow().from_file(boolean_dataset.open()).map(
            lambda x: [int(_) for _ in x]
        ).header_is(['var_0', 'var_1', 'target']) \
            .fit_with(BernoulliNB(), name='bNB', target='target').reload()

        assert len(flow.batch(1)[0]) == len(flow.header)
        assert 'bNB' in flow.clfs.keys()
