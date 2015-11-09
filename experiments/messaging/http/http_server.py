import gevent.monkey ; gevent.monkey.patch_all()

from gevent.pywsgi import WSGIServer


def application(env, start_response):
    start_response('200 OK', [('Content-Type', 'text/html')])
    return [b"<b>hello world</b>"]


if __name__ == '__main__':
    print('Serving on 8088...')
    WSGIServer(('', 8088), application, log=None).serve_forever()
