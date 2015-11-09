
import time

import requests


if __name__ == '__main__':
    num = 10000
    s = requests.Session()
    st_time = time.time()
    for i in xrange(num):
        s.get('http://127.0.0.1:8088/')
    diff_time = time.time() - st_time

    print num / diff_time, "requests / sec"
