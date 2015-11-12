
import uuid

from wade.chorus import Client

if __name__ == '__main__':
    c = Client({
        0: ['localhost', 12345],
    })

    num = 100000
    for i in xrange(num):
        message = uuid.uuid1(clock_seq=i).hex
        status, resp = c.reqrep(0, message)
        if resp != message:
            raise Exception("bad response %s != %s" % (resp, message))
