
import time

from wade.chorus import Client

if __name__ == '__main__':
    c = Client({
        0: ['localhost', 12345],
    })

    num = 200000
    count = 0
    total_call_time = 0
    st_time = time.time()
    for i in xrange(num):
        req = 'request %d' % i
        call_st_time = time.time()
        status, resp = c.reqrep(0, req)
        total_call_time += time.time() - call_st_time

        if resp != req:
            raise Exception("bad response %s != %s" % (resp, m))
    total_time = time.time() - st_time

    print "%f sec per reqrep" % (total_call_time / num)
