
import time

from wade import chorus
from wade.into import node_setup


class CallHandler(object):
    def __init__(self):
        self._count = 0
        self._st_time = 0

    def __call__(self, resp, call_peer, message):
        if self._count % 10000 == 0:
            right_now = time.time()
            print self._count / (right_now - self._st_time)
            self._st_time = right_now
            self._count = 0

        self._count += 1
        resp(chorus.OK, message)


if __name__ == '__main__':
    node_setup(12345, CallHandler()).run()
