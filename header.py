#!/usr/local/bin/python

import struct

class Header:
    def __init__(self, blob):
        (self.type, self.size, self.custom) = struct.unpack("!HHL", blob)

    def serialize(self):
        return struct.pack("!HHL", self.type, self.size, self.custom)
