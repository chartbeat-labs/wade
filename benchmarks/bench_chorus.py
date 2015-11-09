
from wade.chorus import Client

if __name__ == '__main__':
    c = Client({
        0: ['localhost', 12345],
    })

    num = 100000
    for i in xrange(num):
        c.reqrep(0, b'ping')
