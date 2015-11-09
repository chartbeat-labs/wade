import time

import zmq


if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.ROUTER)
    socket.bind('tcp://*:12345')
    socket.bind('inproc://routing.inproc')

    resp = context.socket(zmq.DEALER)
    resp.connect('inproc://routing.inproc')

    print "listening"

    st_time = time.time()
    count = 0

    while True:
        address, empty, data = socket.recv_multipart()
        if empty == 'RESP':
            pass
        else:
            socket.send_multipart([address, b'', 'world'])

            # test an internal response
            #resp.send_multipart([b'RESP', 'response!'])

            count += 1
            if count % 10000 == 0:
                right_now = time.time()
                print count / (right_now - st_time)
                st_time = right_now
                count = 0
