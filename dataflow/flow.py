def read_csv(path, sep, line_terminator):

    with open(path) as fin:

        for line in fin:
            yield line.replace('"', '').replace(line_terminator, '').split(sep)


def read_file(fin, sep, line_terminator):

    for line in fin:
        yield line.replace('"', '').replace(line_terminator, '').split(sep)


class Flow(object):

    def __init__(self):

        self.data = None

    def from_csv(self, path, sep=',', line_terminator='\n'):

        self.data = read_csv(path, sep, line_terminator)

        return self

    def from_file(self, fin, sep=',', line_terminator='\n'):

        self.data = read_file(fin, sep, line_terminator)

        return self

    def count(self):

        return sum(1 for _ in self.data)

    def __repr__(self):

        return '\n'.join([
            str(_) for _ in self.data
        ])
