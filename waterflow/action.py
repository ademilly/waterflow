# -*- coding: utf-8 -*-

"""Action namedtuple used to differentiate map transformation from filter op"""


class Action(object):

    def __init__(self, action_type, action_payload):

        self.type = action_type
        self.payload = action_payload

    def __repr__(self):

        return str((self.type, str(self.f)))
