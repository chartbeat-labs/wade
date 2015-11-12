"""Test how native ThreadPools in gevent interact with pyuv's event
loop.

We set up a pyuv "service" and a bunch of gevent "clients". The
service outputs a request counter every second, and the clients
increment the counter.

NOTE: Abandoned the effort, as of Nov 12, 2015 it seems like even
non-gevent based threading and use of pyuv.Async doesn't work (or at
least my understanding of it is incorrect). Also, it's not clear how
to get a result from the pyuv loop back into the gevent caller without
the use of synchronization primitives (Queue, Event) that gevent may
have monkey patched.

"""


import gevent.monkey ; gevent.monkey.patch_all()

import time
import signal
import threading

import pyuv
import gevent.threadpool


class Service(object):
    def __init__(self):
        self._loop = pyuv.Loop()
        self._count = 0

        self._signal_stop = pyuv.Signal(self._loop)
        self._signal_stop.start(
            self._unwind_loop,
            signal.SIGINT,
        )

        self._timer = pyuv.Timer(self._loop)
        self._timer.start(
            self._print_count,
            timeout=0,
            repeat=1,
        )

    def start(self):
        self._loop.run()

    def _unwind_loop(self, signal_handle, signal_num):
        self._timer.stop()
        self._signal_stop.close()

    def _print_count(self, timer_handle):
        print self._count

    def increment(self):
        async = pyuv.Async(self._loop, self._inc)
        async.send()

    def _inc(self, async_handle):
        self._count += 1


def incrementing_client(service):
    while True:
        service.increment()
        time.sleep(1)


if __name__ == '__main__':
    service = Service()
    # service_th = threading.Thread(target=service.start)
    # service_th.start()

    tp = gevent.threadpool.ThreadPool(1)
    service_th = tp.apply_async(service.start)

    client_ths = []
    for i in xrange(10):
        # th = threading.Thread(target=incrementing_client,
        #                       args=(service,))
        th = gevent.spawn(incrementing_client, service)
        client_ths.append(th)
        th.start()

    service_th.join()

