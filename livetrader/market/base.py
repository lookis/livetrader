import asyncio
import logging
from datetime import datetime
from typing import List, Optional

from livetrader.rpc import Method, Publisher
from livetrader.utils import FifoQueue


class MarketBase(object):

    __market_name__ = None
    __timeframe__ = None

    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    @property
    def logger(self):
        return logging.getLogger(self.__class__.__name__)

    async def watch_klines(self, symbol: str):
        raise NotImplementedError()

    async def get_kline_histories(self, symbol: str, from_ts: Optional[int] = None, limit: Optional[int] = None):
        raise NotImplementedError()


class MarketService(object):

    def __init__(self, market: MarketBase, symbols: List[str]):
        self._market = market
        self._symbols = symbols
        self._tasks = []
        self.__logger__ = logging.getLogger('MarketService')

    async def _publish(self, symbol: str, queue: FifoQueue):
        async for kline in self._market.watch_klines(symbol):
            await queue.put((symbol, kline))

    def start(self):
        queue = FifoQueue(maxsize=100)
        self._market.connect()
        for symbol in self._symbols:
            self._tasks.append(asyncio.get_event_loop().create_task(
                self._publish(symbol, queue)))
        return queue

    @Method
    async def get_kline_histories(
            self, symbol: str, from_ts: Optional[datetime] = None, limit: Optional[int] = None):
        return list(await self._market.get_kline_histories(symbol, from_ts, limit))

    def stop(self):
        for task in self._tasks:
            task.cancel()
        self._market.disconnect()
