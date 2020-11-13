from datetime import datetime
from typing import List, Optional

from environs import Env
from future.utils import iteritems
from livetrader.market import MarketBase
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReplaceOne


class CachedMarket(MarketBase):

    def __init__(self, market: MarketBase, mongodb_uri: Optional[str] = None):
        self._env = env = Env()
        env.read_env()
        self._mongodb_uri = mongodb_uri if mongodb_uri else env.str(
            'MONGODB_URI')
        self._market = market
        self._mongo_client = AsyncIOMotorClient(self._mongodb_uri)
        self._database = self._mongo_client.get_default_database('markets')
        self._hist_mongo_client = None
        self._initied = False

    def connect(self):
        self._market.connect()

    def disconnect(self):
        self._mongo_client.close()
        self._market.disconnect()

    def _collection(self, symbol: str):
        db = self._database
        if self._market.__class__.__market_name__ is None or self._market.__class__.__timeframe__ is None:
            raise Exception(
                'Initialize market cache error (without market_name or timeframe)')
        collection_name = '%s_%s_%s' % (
            self._market.__class__.__market_name__, symbol, self._market.__class__.__timeframe__)
        collection = db[collection_name]
        return collection

    async def _insert_db(self, symbol: str, klines: List[dict]):
        collection = self._collection(symbol)
        await collection.bulk_write([
            ReplaceOne({'datetime': kline['datetime']}, kline, upsert=True)
            for kline in klines
        ])

    async def _initial_cache(self, symbol: str):
        collection = self._collection(symbol)
        if await collection.count_documents({}) > 0:
            rs = await (collection.find().sort([('datetime', -1)]).limit(1).to_list(1))
            last_kline = rs[0]
            history_klines = await self._market.get_kline_histories(
                symbol, from_ts=last_kline['datetime'])
        else:
            await collection.create_index('datetime', unique=True)
            history_klines = await self._market.get_kline_histories(
                symbol, limit=5000)
        await self._insert_db(symbol, history_klines)

    async def watch_klines(self, symbol: str):
        if not self._initied:
            self.logger.debug('init cache from watch_klines')
            await self._initial_cache(symbol)
            self._initied = True
        self.logger.debug('do watch klines from delegate')
        async for kline in self._market.watch_klines(symbol):
            await self._insert_db(symbol, [kline])
            yield kline

    async def get_kline_histories(self, symbol: str, from_ts: Optional[int] = None, to_ts: Optional[int] = None, limit: Optional[int] = None):
        if not self._initied:
            await self._initial_cache(symbol)
            self._initied = True
        collection = self._collection(symbol)
        criteria = {}
        datetime_criteria = {}
        if from_ts:
            datetime_criteria['$gte'] = from_ts
        if to_ts:
            datetime_criteria['$lte'] = to_ts
        if from_ts or to_ts:
            criteria['datetime'] = datetime_criteria
        cursor = collection.find(criteria).sort([('datetime', -1)])
        if limit:
            cursor = cursor.limit(limit)
            count = limit
        else:
            count = await collection.count_documents(criteria)
        return reversed(list(map(lambda x: {k: v for k, v in iteritems(x) if not k.startswith('_')}, await cursor.to_list(count))))
