#!/usr/local/bin/python3.5

import socket

from header import Header
from liquid import Liquid


incoming = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

incoming.bind(('', 1111))

incoming.listen(3)

while True:
    (client, addr) = incoming.accept()

    header = Header(client.recv(8))
    print("Receiving {} bytes from {}".format(header.size, addr))

    buf = client.recv(header.size - 8)
    while len(buf) < header.size - 8:
        buf += client.recv(header.size - 8 - len(buf))

    liquid = Liquid(buf)

    liquid.treat_hg()
    print("Found {} after mercury".format(len(liquid.hazmats)))
    liquid.treat_pb()
    print("Found {} after lead".format(len(liquid.hazmats)))
    liquid.treat_se()
    print("Found {} after selenium".format(len(liquid.hazmats)))

    print("Found {} hazardous contaminants from {}".format(len(liquid.hazmats),
        addr))

    hazmat_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    hazmat_outgoing.connect(("downstream", 8888))
    header.type = 4
    header.size = 8 + 8*len(liquid.hazmats)
    hazmat_outgoing.send(header.serialize())
    hazmat_outgoing.send(liquid.serialize_hazmat())
    hazmat_outgoing.close()

    sewage_outgoing = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sewage_outgoing.connect(("treatment", 1111))
    header.type = 0
    header.size = 8 + 8*len(liquid.data)
    sewage_outgoing.send(header.serialize())
    sewage_outgoing.send(liquid.serialize_water())
    sewage_outgoing.close()

    client.close()

