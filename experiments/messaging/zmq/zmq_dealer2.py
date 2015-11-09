import zmq

if __name__ == '__main__':
    context = zmq.Context()
    socket = context.socket(zmq.DEALER)
    socket.connect('tcp://localhost:12345')

    num = 100000
    for i in xrange(num):
        socket.send_multipart([b'', 'hello'])

    for i in xrange(num):
        socket.recv_multipart()
