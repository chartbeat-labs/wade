
import time

from wade.into import node_setup


class CallHandler(object):
    def __init__(self):
        self._count = 0
        self._st_time = 0

    def __call__(self, resp, fail, call_peer, message):
        if self._count % 10000 == 0:
            right_now = time.time()
            print self._count / (right_now - self._st_time)
            self._st_time = right_now
            self._count = 0

        resp(message)
        self._count += 1


if __name__ == '__main__':
    node_setup(12345, CallHandler()).run()
