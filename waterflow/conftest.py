import pytest


@pytest.fixture(scope="module")
def string_dataset(tmpdir_factory):
    p = tmpdir_factory.mktemp("data").join("test.csv")

    p.write('\n'.join(
        [','.join(['first', 'line'])] + [
            ','.join([str(i) for i in range(5)]) for _ in range(10)
        ] + [','.join(['last', 'line'])]))

    return p


@pytest.fixture(scope="module")
def numeric_dataset(tmpdir_factory):
    p = tmpdir_factory.mktemp("data").join("test.csv")

    p.write('\n'.join(
        [
            ','.join([str(_)] + [str(i) for i in range(5)]) for _ in range(10)
        ]))

    return p


@pytest.fixture(scope="module")
def boolean_dataset(tmpdir_factory):
    p = tmpdir_factory.mktemp("data").join("test.csv")

    p.write('\n'.join(
        [
            "0, 0, 1",
            "0, 1, 0",
            "1, 0, 0",
            "1, 1, 1"
        ]))

    return p


@pytest.fixture(scope="module")
def numerics():

    return [[i for i in range(5)] for _ in range(10)]


@pytest.fixture(scope="module")
def booleans():

    return [
        [0, 0],
        [0, 1],
        [1, 0],
        [1, 1]
    ]
