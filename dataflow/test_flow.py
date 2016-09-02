from flow import Flow


class TestFlow:

    def test_one(self):

        x = Flow()
        assert hasattr(x, 'data')

    def test_two(self):

        x = Flow()
        assert x.data is None

    def test_create_file(self, tmpdir_factory):
        p = tmpdir_factory.mktemp("data").join("test.csv")

        p.write('\n'.join([
            ','.join([str(i) for i in range(5)]) for _ in range(10)
        ]))

        assert Flow().from_file(p.open()).count() == 10
