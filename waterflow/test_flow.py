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

    def test_filter(self, tmpdir_factory):
        """Test filter registering"""

        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join(
            [','.join(['first', 'line'])] + [
                ','.join([str(i) for i in range(5)]) for _ in range(10)
            ] + [','.join(['last', 'line'])]))

        flow = Flow().from_file(p.open()).filter(
            lambda x: 'line' not in x
        )

        assert flow.chain[0].type == 'FLOW::FILTER'
        assert flow.batch(1)[0] == ['0', '1', '2', '3', '4']

    def test_map(self, tmpdir_factory):
        """Test map registering"""

        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join(
            [','.join([str(i) for i in range(5)]) for _ in range(10)]
        ))

        flow = Flow().from_file(p.open()).map(
            lambda x: [float(_) for _ in x]
        ).map(
            lambda x: [2 * _ for _ in x]
        )

        assert flow.chain[0].type == 'FLOW::MAP'
        assert flow.batch(1)[0] == [0, 2, 4, 6, 8]

    def test_reduce(self, tmpdir_factory):
        """Test reduce"""

        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join(
            [','.join([str(i) for i in range(5)]) for _ in range(10)]
        ))

        flow = Flow().from_file(p.open()).map(
            lambda x: [float(_) for _ in x]
        ).map(lambda x: sum(x))

        assert flow.reduce(lambda a, b: a + b) == 100

    def test_reload(self, tmpdir_factory):
        """Test reloading"""

        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join(
            [','.join([str(i) for i in range(5)]) for _ in range(10)]
        ))

        flow = Flow().from_file(p.open()).map(lambda x: 1)

        assert flow.reduce(lambda a, b: a + b) == 10
        assert flow.reload().reduce(lambda a, b: a + b) == 10
