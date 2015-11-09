
import gevent
import gevent.monkey ; gevent.monkey.patch_all()
import gevent.socket

import time

from gevent.server import StreamServer

import mbus


st_time = time.time()
count = 0

def func(data):
    #print "called func"
    global count
    global st_time

    if count % 5000 == 0:
        right_now = time.time()
        print count / (right_now - st_time)
        count = 0
        st_time = right_now

    count += 1

    return "foo"

channels = []

def foo(sock):
    gevent.socket.wait_read(sock.fileno())
    print sock.recv(1024 * 1024)

def connect_handler(sock, address):
    print "got connection", address

    #gevent.spawn(foo, sock)
    #foo(sock)

    # global channels
    ch = mbus.ServerChannel(sock, func)
    ch.start()
    #channels.append(ch)

if __name__ == '__main__':
    s = StreamServer(('localhost', 9090), connect_handler)
    s.serve_forever()
