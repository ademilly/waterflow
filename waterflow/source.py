# -*- coding: utf-8 -*-


class Source(object):

    def __init__(self, **kwargs):
        """Init Source object from dict"""

        self.path = kwargs['path']
        self.sep = kwargs['sep']
        self.line_terminator = kwargs['line_terminator']

        self.fin = kwargs['fin'] if 'fin' in kwargs.keys() else open(self.path)

    def read(self):
        """Yield a value from a csv file in memory

        Keyword arguments:
        path            (string) -- path to file
        sep             (string) -- separator substring
        line_terminator (string) -- line terminator substring
        """

        self.fin.seek(0)
        for line in self.fin:
            yield line.replace('"', '') \
                .replace(self.line_terminator, '') \
                .split(self.sep)
