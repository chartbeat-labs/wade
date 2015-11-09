
import socket

import msgpack


packer = msgpack.Packer()
unpacker = msgpack.Unpacker()


def ping(s):
    global packer
    global unpacker

    s.sendall(packer.pack('hello'))
    unpacker.feed(s.recv(1024))
    try:
        if unpacker.unpack() != 123:
            raise Exception("error in return response")
    except msgpack.OutOfData:
        pass


s = socket.create_connection(('localhost', 12345))

num = 1000000
for i in xrange(num):
    ping(s)
