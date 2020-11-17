import asyncio
from asyncio import Queue, sleep
from datetime import datetime
from typing import Optional

import pytz
import tzlocal
from environs import Env
from livetrader.lib.dwx_zeromq_connector import DWX_ZeroMQ_Connector
from livetrader.market import MarketBase
from pandas import Timedelta, Timestamp
from pytz import timezone


class DwxMarket(MarketBase):
    __market_name__ = 'DWX'
    __timeframe__ = '1MIN'

    def __init__(self, host: Optional[str] = None,
                 push_port: Optional[int] = None,
                 pull_port: Optional[int] = None,
                 sub_port: Optional[int] = None,
                 time_zone: Optional[str] = None):
        self._env = env = Env()
        env.read_env()
        self._host = host if host else env.str('DWX_HOST')
        self._push_port = push_port if push_port else env.int(
            'DWX_PUSH_PORT', 32768)
        self._pulling_port = pull_port if pull_port else env.int(
            'DWX_PULL_PORT', 32769)
        self._subscribe_port = sub_port if sub_port else env.int(
            'DWX_SUB_PORT', 32770)
        self._server_tz = timezone(
            time_zone if time_zone else env.str(
                'DWX_TIMEZONE', 'EET'))
        self._last_kline = None

    def connect(self):
        self._kline_resp = {}
        self._kline_sub = {}
        self._connector = DWX_ZeroMQ_Connector(
            _host=self._host,
            _PUSH_PORT=self._push_port,
            _PULL_PORT=self._pulling_port,
            _SUB_PORT=self._subscribe_port)
        self._connector.add_pulldata_handler(self)
        self._connector.add_subdata_handler(self)

    def disconnect(self):
        self._connector._DWX_ZMQ_SHUTDOWN_()

    async def onSubData(self, msg):
        code, rates = msg.split(' ')
        code = code.split('_')[0]
        time, open,  high,  low,  close,  tick_volume, spread, real_volume = rates.split(
            ';')
        dt = self._server_tz.localize(
            datetime.utcfromtimestamp(int(time))).astimezone(pytz.utc)
        kline = {
            'datetime': int(dt.timestamp()) * 1000,
            'open': open,
            'high': high,
            'low': low,
            'close': close,
            'spread': spread,
            'volume': int(tick_volume)
        }
        await self._kline_sub[code].put(kline)

    async def onPullData(self, msg):
        if msg['_action'] and msg['_action'] == 'HIST':
            queue = self._kline_resp[msg['_symbol'].split('_')[0]]
            await queue.put(msg['_data'])

    async def watch_klines(self, symbol: str):
        market, code = symbol.split('.')
        if market == 'FOREX':
            self._kline_sub[code] = Queue()
            await self._connector._DWX_MTX_SUBSCRIBE_MARKETDATA_(code)
            await self._connector._DWX_MTX_SEND_TRACKRATES_REQUEST_(
                [('%s_M1' % code, code, 1)])
            while True:
                kline = await self._kline_sub[code].get()
                yield kline

    async def get_kline_histories(self, symbol: str, from_ts: Optional[int] = None, to_ts: Optional[int] = None, limit: Optional[int] = None, timeframe: Optional[int] = 1):
        market, code = symbol.split('.')
        if market == 'FOREX':
            local_tz = tzlocal.get_localzone()
            if to_ts:
                _end = local_tz.localize(Timestamp.fromtimestamp(to_ts / 1000))
            else:
                _end = local_tz.localize(Timestamp.now())
            if from_ts:
                _start = local_tz.localize(Timestamp.fromtimestamp(
                    from_ts / 1000))
            else:
                _start = _end - Timedelta(minutes=limit - 1)
            if _start >= _end:
                return []
            _start = _start.astimezone(self._server_tz)
            _end = _end.astimezone(self._server_tz)
            self._kline_resp[code] = Queue()
            await self._connector._DWX_MTX_SEND_HIST_REQUEST_(_symbol=code,
                                                              _timeframe=timeframe,
                                                              _start=_start.strftime(
                                                                  '%Y.%m.%d %H:%M:00'),
                                                              _end=_end.strftime('%Y.%m.%d %H:%M:00'))
            klines = [self._parse_kline(kline_raw) for kline_raw in await self._kline_resp[code].get()]
            del self._kline_resp[code]
            return klines
        else:
            return []

    def _parse_kline(self, kline_raw: dict) -> dict:
        dt = self._server_tz.localize(
            datetime.strptime(
                kline_raw['time'],
                '%Y.%m.%d %H:%M')).astimezone(
            pytz.utc)
        return {
            'datetime': int(dt.timestamp() * 1000),
            'open': kline_raw['open'],
            'high': kline_raw['high'],
            'low': kline_raw['low'],
            'close': kline_raw['close'],
            'spread': kline_raw['spread'],
            'volume': kline_raw['tick_volume']
        }
