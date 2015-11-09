Network performance matters. A lot. These are experiments with various
messaging implementations.

http
  Simple synchronous GETs w/ gevent.

custom
  Also gevent-based, but with custom protocol rather than http.

thrift
  Apache Thrift, straightforward implementation almost directly from
  docs.

zmq
  ZMQ, using standard reqrep as well as dealer and router
  implementations.

pyuv
  Event loop, client implemented with synchronous reqrep like behavior.
