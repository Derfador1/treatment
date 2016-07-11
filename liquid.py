#!/usr/local/bin/python

import struct

class Node:
    def __init__(self, data, left, right):
        self.data = data
        self.left = left
        self.right = right

class Liquid:
    def __init__(self, blob):
        self.slots = len(blob)//8
        raw_data = struct.unpack("!" + "LHH"*self.slots, blob)
 
        self.hazmats = set()

        self.data = {i//3+1: list(raw_data[i:i+3]) for i in
                range(0, len(raw_data), 3) if raw_data[i]}

    def treat_se(self):
        '''Circularly-linked list'''
        heads = set(self.data.keys())
        for k,v in self.data.items():
            heads.discard(v[2])
            heads.discard(v[1])

        if len(heads) < 1:
            to_remove = max(d[0] for d in self.data.values())
            self.hazmats.add(to_remove)

            for i in self.data:
                if self.data[i][0] == to_remove:
                    break
            del self.data[i]
            for n in self.data:
                if self.data[n][1] == i:
                    self.data[n][1] = 0
                if self.data[n][2] == i:
                    self.data[n][2] = 0


    def treat_pb(self):
        pass

    def treat_hg(self):
        '''Multiple roots in graph'''
        heads = set(self.data.keys())
        for k,v in self.data.items():
            heads.discard(v[1])
            heads.discard(v[2])

        if len(heads) > 1:
            to_remove = min((self.data[i][0] for i in heads))
            self.hazmats.add(to_remove)

            for i in self.data:
                if self.data[i][0] == to_remove:
                    break

            del self.data[i]
            self.treat_hg()

    def serialize_water(self):
        for n in range(self.slots):
            n += 1
            if n not in self.data:
                self.data[n] = (0,0,0)

        array = (x for n in range(self.slots) for x in self.data[n+1])
        return struct.pack("!" + "LHH"*len(self.data), *array)

    def serialize_hazmat(self):
        array = (x for val in self.hazmats for x in (val,0))
        return struct.pack("!" + "LL"*len(self.hazmats), *array)
