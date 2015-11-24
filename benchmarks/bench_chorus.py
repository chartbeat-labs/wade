
import time

from wade.chorus import Client

if __name__ == '__main__':
    c = Client({
        0: ['localhost', 12345],
    })

    num = 200000
    batch = 100
    count = 0
    total_call_time = 0
    st_time = time.time()
    for i in xrange(num):
        requests = []
        for j in xrange(batch):
            requests.append('request %d' % count)
            count += 1

        call_st_time = time.time()
        result = c.multi_reqrep(0, requests)
        total_call_time += time.time() - call_st_time

        responses = [resp for status, resp in result]
        if requests != responses:
            raise Exception("bad response %s != %s" % (requests, responses))
    total_time = time.time() - st_time

    print "%f sec per multi request" % (total_call_time / num)
    print "%f requests / sec (total time)" % (num * batch / total_time)
