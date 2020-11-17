from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from datetime import datetime, timedelta

from backtrader import TimeFrame, date2num, num2date
from backtrader.feed import DataBase
from backtrader.feeds import DataBase
from backtrader.utils.py3 import (integer_types, queue, string_types,
                                  with_metaclass)

from .store import LiveTraderStore


class MetaLiveTraderData(DataBase.__class__):
    def __init__(cls, name, bases, dct):
        '''Class has already been created ... register'''
        # Initialize the class
        super(MetaLiveTraderData, cls).__init__(name, bases, dct)

        # Register with the store
        LiveTraderStore.DataCls = cls


class LiveTraderData(with_metaclass(MetaLiveTraderData, DataBase)):

    params = (
        ('qcheck', 0.5),
        ('historical', False),  # only historical download
        ('backfill_start', True),  # do backfilling at the start
        ('backfill', True),  # do backfilling when reconnecting
    )

    _store = LiveTraderStore

    _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(4)

    def islive(self):
        return True

    def __init__(self, **kwargs):
        self.client = self._store(**kwargs)

    def setenvironment(self, env):
        '''Receives an environment (cerebro) and passes it over to the store it
        belongs to'''
        super(LiveTraderData, self).setenvironment(env)
        # env.addstore(self.client)

    def start(self):
        '''Starts the Oanda connecction and gets the real contract and
        contractdetails if it exists'''
        super(LiveTraderData, self).start()

        # Create attributes as soon as possible
        self._storedmsg = dict()  # keep pending live message (under None)
        self.qlive = None
        self._state = self._ST_OVER

        # Kickstart store and get queue to wait on
        self.client.start(data=self)

        # check if the granularity is supported
        otf = self.client.get_granularity(self._timeframe, self._compression)
        if otf is None:
            self.put_notification(self.NOTSUPPORTED_TF)
            self._state = self._ST_OVER
            return

        self._start_finish()
        self._state = self._ST_START  # initial state for _load
        self._st_start()

    def _st_start(self, instart=True, tmout=None):
        if self.p.historical:
            self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float('inf'):
                dtend = num2date(self.todate)

            dtbegin = None
            if self.fromdate > float('-inf'):
                dtbegin = num2date(self.fromdate)

            self.qhist = self.client.get_kline_histories(
                self.p.dataname, dtbegin, dtend,
                self._timeframe, self._compression)

            self._state = self._ST_HISTORBACK
            return True

        if not self.qlive:
            self.qlive = self.client.watch_kline(
                self.p.dataname, queue.Queue())
        if instart:
            self._statelivereconn = self.p.backfill_start
        else:
            self._statelivereconn = self.p.backfill
        if self._statelivereconn:
            self.put_notification(self.DELAYED)

        self._state = self._ST_LIVE

        return True  # no return before - implicit continue

    def stop(self):
        '''Stops and tells the store to stop'''
        super(LiveTraderData, self).stop()
        self.client.stop()

    def haslivedata(self):
        return bool(self._storedmsg or self.qlive)  # do not return the objs

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = (self._storedmsg.pop(None, None) or
                           self.qlive.get(timeout=self._qcheck))
                except queue.Empty:
                    return None  # indicate timeout situation

                if msg is None:  # Conn broken during historical/backfilling
                    self.put_notification(self.CONNBROKEN)
                    self._st_start(instart=False)
                    continue

                # Process the message according to expected return type
                if not self._statelivereconn:
                    if self._laststatus != self.LIVE:
                        if self.qlive.qsize() <= 1:  # very short live queue
                            self.put_notification(self.LIVE)

                    ret = self._load_history(msg)
                    if ret:
                        return True

                    # could not load bar ... go and get new one
                    continue

                # Fall through to processing reconnect - try to backfill
                self._storedmsg[None] = msg  # keep the msg

                # else do a backfill
                if self._laststatus != self.DELAYED:
                    self.put_notification(self.DELAYED)

                dtend = None
                if len(self) > 1:
                    # len == 1 ... forwarded for the 1st time
                    dtbegin = self.datetime.datetime(-1)
                elif self.fromdate > float('-inf'):
                    dtbegin = num2date(self.fromdate)
                else:  # 1st bar and no begin set
                    # passing None to fetch max possible in 1 request
                    dtbegin = None

                dtend = datetime.utcfromtimestamp(
                    int(msg['datetime'] / 1000))

                self.qhist = self.client.get_kline_histories(
                    self.p.dataname, dtbegin, dtend, self._timeframe, self._compression)

                self._state = self._ST_HISTORBACK
                self._statelivereconn = False  # no longer in live
                continue

            elif self._state == self._ST_HISTORBACK:
                msg = self.qhist.get()
                if msg is None:  # Conn broken during historical/backfilling
                    # Situation not managed. Simply bail out
                    self.put_notification(self.DISCONNECTED)
                    self._state = self._ST_OVER
                    return False  # error management cancelled the queue

                if msg:
                    if self._load_history(msg):
                        return True  # loading worked

                    continue  # not loaded ... date may have been seen
                else:
                    # End of histdata
                    if self.p.historical:  # only historical
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # end of historical

                # Live is also wished - go for it
                self._state = self._ST_LIVE
                continue

            elif self._state == self._ST_START:
                if not self._st_start(instart=False):
                    self._state = self._ST_OVER
                    return False

    def _load_history(self, msg):
        dtobj = datetime.utcfromtimestamp(int(msg['datetime'] / 1000))
        dt = date2num(dtobj)
        if dt < self.lines.datetime[-1]:
            return False  # time already seen
        self.lines.datetime[0] = dt
        self.lines.volume[0] = float(msg['volume'])
        self.lines.openinterest[0] = 0.0

        self.lines.open[0] = float(msg['open'])
        self.lines.high[0] = float(msg['high'])
        self.lines.low[0] = float(msg['low'])
        self.lines.close[0] = float(msg['close'])
        # if dt == self.lines.datetime[-1]:
        #     return False  # time already seen, but still update lines
        return True
