# import gevent
# import gevent.monkey ; gevent.monkey.patch_all()

import time

import sys, glob
sys.path.append('gen-py')
#sys.path.insert(0, glob.glob('../../lib/py/build/lib.*')[0])

from play import Play
#from tutorial.ttypes import *

#from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

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

handler = PlayHandler()
processor = Play.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TBufferedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

# You could do one of these for a multithreaded server
server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
#server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

print 'Starting the server...'
server.serve()
print 'done.'
