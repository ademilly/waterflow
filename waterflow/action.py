# -*- coding: utf-8 -*-

"""Action namedtuple used to differentiate map transformation from filter op"""


class Action(object):

    def __init__(self, action_type, action_function):

        self.type = action_type
        self.f = action_function

    def __repr__(self):

        return str((self.type, str(self.f)))
