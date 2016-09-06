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

    def test_map(self, tmpdir_factory):
        """Test map registering"""

        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join([
            ','.join([str(i) for i in range(5)]) for _ in range(10)
        ]))

        assert Flow().from_file(p.open()).map(
            lambda x: [float(_) for _ in x]
        ).eval()[0] == [0, 1, 2, 3, 4]

        assert Flow().from_file(p.open()).map(
            lambda x: [float(_) for _ in x]
        ).map(lambda x: [2 * _ for _ in x]).eval()[0] == [0, 2, 4, 6, 8]

        assert Flow().from_file(p.open()).map(
            lambda x: 1
        ).reduce(
            lambda a, b: a + b
        ) == 10
