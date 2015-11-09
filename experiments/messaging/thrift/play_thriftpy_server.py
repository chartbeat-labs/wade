
import time

import thriftpy

play_thrift = thriftpy.load("play.thrift", module_name="play_thrift")

#from thriftpy.rpc import make_server

from thriftpy.tornado import make_server
from thriftpy.tornado import make_client
from tornado import ioloop


class PlayHandler(object):
    def __init__(self):
        self._count = 0
        self._st_time = time.time()

    def ping(self):
        self._count += 1
        if self._count % 10000 == 0:
            right_now = time.time()
            print self._count / (right_now - self._st_time)
            self._st_time = right_now
            self._count = 0

        #print "ping!"
        return 123


if __name__ == '__main__':
    #server = make_server(play_thrift.Play, PlayHandler(), '127.0.0.1', 9090)
    #server.serve()

    server = make_server(play_thrift.Play, PlayHandler())
    server.listen(9090)

    # client = ioloop.IOLoop.current().run_sync(
    #         lambda: make_client(play_thrift.Play, '127.0.0.1', 9090))
    # ioloop.IOLoop.current().run_sync(client.ping)

    ioloop.IOLoop.current().start()
