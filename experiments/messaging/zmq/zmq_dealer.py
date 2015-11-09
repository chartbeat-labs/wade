import gevent
import gevent.monkey ; gevent.monkey.patch_all()

import zmq.green as zmq
#import zmq
#import zmq.devices
import zmq.green


class Counter(object):
    def __init__(self):
        self.count = 0

def runner(dest, text, counter):
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.connect(dest)
    while True:
        socket.send(text)
        r = socket.recv()
        #gevent.sleep(1)
        #print r

        counter.count += 1
        #print counter.count

if __name__ == '__main__':
    context = zmq.Context()

    fe = context.socket(zmq.ROUTER)
    fe.bind("tcp://*:11111")
    be = context.socket(zmq.DEALER)
    be.connect("tcp://localhost:12345")

    #gevent.spawn(zmq.green.proxy, fe, be)
    #proxy = zmq.green.proxy(fe, be)

    # proxy = zmq.devices.ThreadProxy(
    #     zmq.ROUTER,
    #     zmq.DEALER,
    # )
    # proxy.bind_in("tcp://*:11111")
    # proxy.connect_out("tcp://localhost:12345")
    # proxy.start()

    print "starting lots of threads"

    counter = Counter()
    counter.count = 0
    gevent.spawn(runner, "tcp://localhost:12345", "hello", counter)
    gevent.spawn(runner, "tcp://localhost:12345", "world", counter)

    #runner("tcp://localhost:11111")

    while True:
        print counter.count
        gevent.sleep(1)

    # socket = context.socket(zmq.REQ)
    # socket.connect("tcp://localhost:11111")
    # #socket.connect("tcp://localhost:12345")
    # print "sending"
    # socket.send("hello!")
    # print socket.recv()

    # socket.send_multipart(["", "hello!"])
    # print socket.recv_multipart()

    # num = 1000
    # for i in range(num):
    #     socket.send_multipart(["", str(i), "hello %d" % i])

    # for i in range(num):
    #     #print socket.recv()
    #     print socket.recv_multipart()
    #     pass

    # socket.close()
