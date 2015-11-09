
import time

import signal
import pyuv
import msgpack


count = 0
st_time = 0

packer = msgpack.Packer()
unpacker = msgpack.Unpacker()


def on_read(client, data, error):
    if data is None:
        client.close()
        clients.remove(client)
        return

    unpacker.feed(data)
    try:
        msg = unpacker.unpack()
    except msgpack.OutOfData:
        return

    client.write(packer.pack(123))

    global count
    global st_time

    if count % 10000 == 0:
        right_now = time.time()
        print count / (right_now - st_time)
        st_time = right_now
        count = 0

    count += 1

def on_connection(server, error):
    client = pyuv.TCP(server.loop)
    server.accept(client)
    clients.append(client)
    client.start_read(on_read)

def signal_cb(handle, signum):
    [c.close() for c in clients]
    signal_h.close()
    server.close()


print("PyUV version %s" % pyuv.__version__)

loop = pyuv.Loop.default_loop()
clients = []

server = pyuv.TCP(loop)
server.bind(("0.0.0.0", 12345))
server.listen(on_connection)

signal_h = pyuv.Signal(loop)
signal_h.start(signal_cb, signal.SIGINT)

loop.run()
print("Stopped!")
