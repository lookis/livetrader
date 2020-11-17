
import asyncio
import threading
from datetime import timedelta
from queue import Queue

from examples.market_as_standalone_client import subscribe_kline
from livetrader.rpc import Client, MarketSubscriber, TradeSubscriber
from livetrader.trade.base import OrderBase

from backtrader import TimeFrame
from backtrader.feed import DataBase
from backtrader.metabase import MetaParams
from backtrader.trade import Trade
from backtrader.utils.py3 import with_metaclass


class StoreSubscriber(MarketSubscriber):

    def __init__(self, symbol, queue: Queue):
        super().__init__(symbol)
        self._last_kline = None
        self.queue = queue

    def on_kline(self, kline: dict):
        if self._last_kline != kline:
            self.queue.put_nowait(kline)
            self._last_kline = kline

    def on_state(self, isalive: bool):
        if not isalive:
            self.queue.put_nowait(None)


class MetaSingleton(MetaParams):
    '''Metaclass to make a metaclassed class a singleton'''
    def __init__(cls, name, bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class LiveTraderStore(with_metaclass(MetaSingleton, object)):

    BrokerCls = None  # broker class will autoregister
    DataCls = None  # data class will auto register

    GRANULARITY = [(1, TimeFrame.Minutes),
                   (5, TimeFrame.Minutes),
                   (15, TimeFrame.Minutes),
                   (30, TimeFrame.Minutes),
                   (60, TimeFrame.Minutes),
                   (1, TimeFrame.Days)]

    params = (
        ('endpoint', 'ipc:///tmp/market'),
    )

    def __init__(self):
        self.client = Client()
        self.ord_refs = {}  # key: backtrader order id, value livetrader order id
        self.ord_oids = {}  # key: livetrader order id, value: backtrader order id
        self.all_orders = {}
        self.t = None
        self.client_id = 'backtrader'

    @classmethod
    def getdata(cls, *args, **kwargs):
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        return cls.BrokerCls(*args, **kwargs)

    def start(self, data=None, broker=None):
        self.client.connect(self.p.endpoint)
        self._pill = asyncio.Event()
        self.subscribers = []
        if broker is not None:
            self.broker = broker
        # main loop
        current_loop = asyncio.get_event_loop()

        def _everything_async(loop):
            async def _main_loop():
                while not self._pill.is_set():
                    await asyncio.sleep(0)
            asyncio.set_event_loop(loop)
            current_loop.run_until_complete(_main_loop())
        self.t = threading.Thread(
            target=_everything_async, args=(
                current_loop,))
        self.t.start()
        # trade subscriber

        async def _trade_task():
            subscriber = BacktraderTradeSubscriber(client_id='backtrader')
            subscriber.connect(self.p.endpoint)
            self.subscribers.append(subscriber)
            subscriber.run()
        asyncio.get_event_loop().create_task(_trade_task())

    def stop(self):
        self.client.close()
        self._pill.set()
        current_loop = asyncio.get_event_loop()
        current_loop.stop()
        if self.t and self.t.is_alive():
            self.t.join()

    def get_granularity(self, timeframe, compression):
        return (timeframe, compression) in self.__class__.GRANULARITY

    def get_kline_histories(self, symbol, from_dt,
                            end_dt, timeframe, compression):
        history_queue = Queue()
        if self.get_granularity(timeframe, compression):
            if compression == TimeFrame.Days:
                timeframe = timeframe * 1440
            limit = 200 if not from_dt else None
            from_ts = int(from_dt.timestamp() * 1000) if from_dt else None
            end_ts = int(end_dt.timestamp() * 1000) if end_dt else None
            histories = self.client.get_kline_histories(
                symbol, from_ts, end_ts, limit, timeframe)
            for kline in histories:
                history_queue.put_nowait(kline)
        history_queue.put_nowait(False)
        return history_queue

    def watch_kline(self, symbol: str, queue: Queue):

        async def _kline_task():
            subscriber = StoreSubscriber(symbol, queue)
            subscriber.connect(self.p.endpoint)
            self.subscribers.append(subscriber)
            subscriber.run()
        asyncio.get_event_loop().create_task(_kline_task())
        return queue

    def get_cash(self):
        return float(self.client.fetch_balance()['balance'])

    def get_value(self):
        return float(self.client.fetch_balance()['value'])

    def get_positions(self):
        return self.client.fetch_positions()

    def order_create(self, b_order):
        symbol = b_order.data.dataname
        ordtype = b_order.ordtype
        exectype = b_order.exectype
        size = b_order.size
        price = b_order.price
        l_order_id = self.client.open_order(
            symbol, ordtype, exectype, size, self.client_id, price)
        self.ord_refs[b_order.ref] = l_order_id
        self.ord_oids[l_order_id] = b_order.ref
        if self.broker:
            self.broker._submit(b_order.ref)

    def order_cancel(self, b_order):
        l_order_id = self.ord_refs[b_order.ref]
        self.client.cancel_order(l_order_id)
        if self.broker:
            self.broker._cancel(self.ord_oids[l_order_id])
        del self.ord_oids[l_order_id]
        del self.all_orders[l_order_id]
        del self.ord_refs[b_order.ref]

    def on_order_callback(self, l_order):
        l_order_id = l_order['order_id']
        if l_order_id in self.all_orders:
            old_order = self.all_orders[l_order_id]
            if old_order['exectype'] == OrderBase.Market:
                if self.broker:
                    self.broker._fill(
                        self.ord_oids[l_order_id], abs(
                            l_order['size']), l_order['price'])
                b_order_ref = self.ord_oids[l_order_id]
                del self.ord_oids[l_order_id]
                del self.ord_refs[b_order_ref]
                del self.all_orders[l_order_id]
        else:
            self.all_orders[l_order_id] = l_order
            if self.broker:
                self.broker._accept(self.ord_oids[l_order_id])

    def on_trade_callback(self, l_trade):
        pass


class BacktraderTradeSubscriber(TradeSubscriber):

    def __init__(self, store: LiveTraderStore,
                 client_id: str, heartbeat_freq: int = 5):
        super().__init__(client_id, heartbeat_freq)
        self._store = store

    def on_order(self, order: dict):
        self._store.on_order_callback(order)

    def on_trade(self, trade: dict):
        self._store.on_trade_callback(trade)
