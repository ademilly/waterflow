# -*- coding: utf-8 -*-

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

        split_rate = 0.3
        flow = Flow(seed=42)

        flow = flow.from_file(numeric_dataset.open()).map(
            lambda x: [float(_) for _ in x]
        )

        flow_left = Flow(flow=flow).split(split_rate, on='left')
        flow_right = Flow(flow=flow).split(split_rate, on='right')

        assert flow_left.map(lambda x: 1).reduce(lambda a, b: a + b) == 3
        assert flow_right.map(lambda x: 1).reduce(lambda a, b: a + b) == 7
