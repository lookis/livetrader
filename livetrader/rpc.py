import asyncio
import functools
import logging
import signal

import gevent
import zerorpc
from zerorpc import Client as _Client
from zerorpc import Publisher as _Publisher
from zerorpc import Subscriber as _Subscriber

from livetrader.utils import FifoQueue


class Publisher(_Publisher):
    pass


class Subscriber(_Subscriber):
    pass


class Client(_Client):
    pass


class MarketSubscriber(Subscriber):

    def __init__(self, symbol: str):
        super().__init__()
        self._symbol = symbol
        self._pill2kill = asyncio.Event()

    def connect(self, endpoint, resolve=True):
        _endpoint = "%s_%s" % (endpoint, self._symbol)
        return super().connect(endpoint=_endpoint, resolve=resolve)

    def on_kline(self, kline: dict):
        raise NotImplementedError()

    def run(self):
        self._gevent_task = gevent.spawn(super().run)

        async def the_loop():
            while not self._pill2kill.is_set():
                await asyncio.sleep(0)
                gevent.sleep(0)
        self._asyncio_task = asyncio.get_event_loop().create_task(the_loop())

    def close(self):
        self._pill2kill.set()
        if self._gevent_task is not None:
            self._gevent_task.kill()
        if self._asyncio_task is not None:
            self._asyncio_task.cancel()


class Method(object):
    def __init__(self, functor):
        self._functor = functor
        self.__doc__ = functor.__doc__
        self.__name__ = getattr(functor, "__name__", str(functor))
        functools.update_wrapper(self, functor)

    @property
    def coroutine(self):
        return asyncio.iscoroutinefunction(self._functor)

    def __get__(self, instance, type_instance=None):
        if instance is None:
            return self
        return self.__class__(self._functor.__get__(instance, type_instance))

    def __call__(self, *args, **kwargs):
        return self._functor(*args, **kwargs)


class Server(object):

    def __init__(self, service):
        self._service = service
        self._publishers = {}
        self._logger = logging.getLogger('Server')
        methods = dict((k, self._decorate_coroutine_method(getattr(service, k))) for k in dir(service)
                       if isinstance(getattr(service, k), Method))
        self.s = zerorpc.Server(methods=methods)
        self._pill2kill = asyncio.Event()

    def bind(self, endpoint: str):
        self._endpoint = endpoint
        self.s.bind(endpoint)

    def _decorate_coroutine_method(self, method):
        if method.coroutine:
            def __deco__(*args, **kwargs):
                return asyncio.get_event_loop().run_until_complete(method(*args, **kwargs))
            return __deco__
        else:
            return method

    def _publish(self, queue: FifoQueue):
        while not self._pill2kill.is_set():
            gevent.sleep(0)
            if not queue.empty():
                symbol, kline = queue.get_nowait()
                if symbol in self._publishers:
                    publisher = self._publishers[symbol]
                else:
                    publisher = self._publishers[symbol] = Publisher()
                    publisher.bind('%s_%s' % (self._endpoint, symbol))
                publisher.on_kline(kline)

    def run(self):
        # register shutdown handler
        gevent.signal_handler(signal.SIGINT, self.close)
        gevent.signal_handler(signal.SIGTERM, self.close)

        queue = self._service.start()
        self._publish_task = gevent.spawn(self._publish, (queue))
        self._server_task = gevent.spawn(self.s.run)

        self._logger.info('Server started')
        while not self._pill2kill.is_set():
            asyncio.get_event_loop().run_until_complete(asyncio.sleep(0))
            gevent.sleep(0)

    def close(self):
        self._logger.info('Server stopping...')
        if self._publish_task is not None:
            self._publish_task.kill()
        self.s.close()
        if self._server_task is not None:
            self._server_task.kill()
        for publisher in self._publishers.values():
            publisher.close()
        self._service.stop()
        self._logger.info('Server stopped')
        self._pill2kill.set()
