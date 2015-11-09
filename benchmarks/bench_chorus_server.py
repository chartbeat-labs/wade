
import time

from wade.chorus import Node


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

        resp('pong')
        self._count += 1


if __name__ == '__main__':
    Node.start_standalone(0, { 0: ['localhost', 12345] }, CallHandler())
