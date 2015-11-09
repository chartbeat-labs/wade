import zmq

if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    #socket = context.socket(zmq.DEALER)
    socket.connect('tcp://localhost:12345')

    num = 200000
    for i in xrange(num):
        # REQ
        socket.send("hello")
        socket.recv()

        # DEALER
        # socket.send_multipart([b'', b'hello'])
        # socket.recv_multipart()
