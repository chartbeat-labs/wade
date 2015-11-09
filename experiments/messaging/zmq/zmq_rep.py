import time

import zmq


if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:12345')

    st_time = time.time()
    count = 0
    while True:
        r = socket.recv()
        #print r
        socket.send("foo")

        if count % 5000 == 0:
            right_now = time.time()
            print count / (right_now - st_time)
            st_time = right_now
            count = 0
        count += 1

        # msg_id, body = socket.recv_multipart()
        # print msg_id, body
        # socket.send_multipart([msg_id, "world: %s" % body])
