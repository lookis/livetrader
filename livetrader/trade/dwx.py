
import asyncio
from asyncio import Event, Lock, Queue, sleep
from datetime import datetime
from io import UnsupportedOperation
from itertools import accumulate
from types import coroutine
from typing import Optional

from environs import Env
from livetrader.lib.dwx_zeromq_connector import DWX_ZeroMQ_Connector
from livetrader.trade.base import OrderBase, TradeBase
from pytz import timezone


class DwxTrade(TradeBase):
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
        self._pull_results_queue = {}
        self._pull_results_lock = {}

    def connect(self):
        self._connector = DWX_ZeroMQ_Connector(
            _host=self._host,
            _PUSH_PORT=self._push_port,
            _PULL_PORT=self._pulling_port,
            _SUB_PORT=self._subscribe_port)

        self._connector.add_pulldata_handler(self)
        self._connector.add_subdata_handler(self)
        self._pub_queue = {}
        self._pending_orders = {}
        self._pill = Event()

    def disconnect(self):
        self._connector._DWX_ZMQ_SHUTDOWN_()
        self._pill.set()
        if hasattr(self, '_watch_order_task'):
            self._watch_order_task.cancel()

    async def onSubData(self, msg):
        pass

    async def onPullData(self, msg):
        if self._pull_results_queue and '_action' in msg and msg[
                '_action'] in self._pull_results_queue:
            action = msg['_action']
            del msg['_action']
            await self._pull_results_queue[action].put(msg)
        else:
            print(msg)

    # 先要求支持前两种，后面四种订单因为无法做到通用性，所以只能部分支持
    def fetch_exectypes(self):
        return OrderBase.ExecTypes[:2]

    async def _with_pull_result(self, action: str, coro: coroutine):
        if action in self._pull_results_lock:
            lock = self._pull_results_lock[action]
        else:
            lock = self._pull_results_lock[action] = Lock()
        await lock.acquire()
        self._pull_results_queue[action] = queue = Queue()
        await coro
        r = await queue.get()
        self._pull_results_queue[action] = None
        lock.release()
        return r

    async def fetch_balance(self):
        account_info = (await self._with_pull_result(
            'ACCOUNT', self._connector._DWX_MTX_SEND_ACCOUNT_REQUEST_()))['_data']
        print(account_info)
        return {
            'cash': account_info['margin_free'],
            'value': account_info['equity'],
        }

    async def fetch_markets(self):
        """
        获取可交易品种信息
            symbol 品种
            leverage 杠杆大小，用于期货计算最大可买量
            mult 乘数，用于期货计算收益
            commission 费率, 百分比（以小数表示）或者固定值
            margin 保证金，当 stocklike 为 False 时有效，固定值。
            shortable 可做空
            commtype: perc 百分比, fixed 确定值，如果为 None 的时候依据如下规则计算：
                如果 margin 为 None，那么推定为股票类型，因此 commtype 推定为 perc， stocklike 为 True
                如果 margin 为非 None，那么推定为期货类型，commtype 推定为 fixed，stocklike 为 False
            stocklike:是否为股票，股票以百分比计算费用，期货以固定值计算费用
            interest: 卖空的年化利率
            interest_long: 做多的年化利率
        """
        return {
            'FOREX.EURUSD': {
                'leverage': 50,
                'mult': 1,
                'commission': 0,
                'margin': 0.02,
                'shortable': True,
                'commtype': 'perc',
                'stocklike': True,
                'interest': 0,
                'interest_long': 0
            }
        }

    async def open_order(self, symbol: str, ordtype: int, exectype: int, size: float, client: str, price: Optional[float] = None):
        """
        symbol: 标的，SHFE.rb2101
        ordtype: @see OrderBase.OrdTypes
        exectype: @see OrderBase.ExecTypes
        price: 价格
        size: 量
        client: 客户端的id，调用用来区分是否为自己发起的订单（与成交）
        返回：多一个 status 属性， OrderBase.Status
        因为不是所有的市场关于定单都是阻塞的，就哪怕是市价单也会通过异步的方式通知订单状态，所以这里定单会有很多不同的状态，参考 OrderBase.Status
        """
        assert(size > 0)
        _type = 0
        if ordtype == OrderBase.Sell:
            _type = 1
        if exectype == OrderBase.Limit:
            _type |= 2
        net_position = await self.fetch_positions(symbol)
        # 因为MT4的交易机制是可以开双向单，所以这里需要判断净头寸之后发现如果是反向单的话处理成先平再开
        if symbol in net_position and ((net_position['symbol']['size'] > 0 and ordtype == OrderBase.Sell) or (
                net_position['symbol']['size'] < 0 and ordtype == OrderBase.Buy)):
            raw_open_orders = (await self._with_pull_result('OPEN_TRADES',
                                                            self._connector._DWX_MTX_GET_ALL_OPEN_TRADES_()))['_trades']
            if exectype == OrderBase.Limit:
                # FIXME: 如果有之前的仓位又用反向挂限价单，暂时不支持，如果之后再来撤单的话情形就变的特别复杂了
                raise UnsupportedOperation()
            open_orders = [
                self._parse_order(
                    order_id,
                    order) for order_id,
                order in raw_open_orders.items()]
            open_orders = filter(
                lambda x: x['client'] == client,
                open_orders)
            filled_orders = filter(lambda x: x['filled'] != 0, open_orders)
            sorted_filled_orders = sorted(
                filled_orders, key=lambda o: o['filled'])
            order_to_close = []
            order_partial_close = None
            left = size
            for filled_order in sorted_filled_orders:
                if abs(filled_order['size']) <= left:
                    order_to_close.append(filled_order)
                    left -= abs(filled_order['size'])
                    continue
                if left == 0:
                    break
                if abs(filled_order['size']) > left:
                    order_partial_close = filled_order
                    break
            # 先平单
            for order in order_to_close:
                await self._connector._DWX_MTX_CLOSE_TRADE_BY_TICKET_(
                    order['order_id'])
            if order_partial_close:
                await self._connector._DWX_MTX_CLOSE_PARTIAL_BY_TICKET_(
                    order_partial_close['order_id'], left)
            elif left > 0:
                # 如果出现平部分单说明反向头寸没有完全close，否则就有可能还剩下一部分需要开新仓
                order_to_open = {'_action': 'OPEN',
                                 '_type': _type,
                                 '_symbol': symbol.replace('FOREX.', ''),
                                 '_price': price,
                                 '_SL': 0,
                                 '_TP': 0,
                                 '_comment': client,
                                 '_lots': left,
                                 '_magic': 0,
                                 '_ticket': 0}
                await self._connector._DWX_MTX_NEW_TRADE_(_order=order_to_open)
        else:
            order_to_open = {'_action': 'OPEN',
                             '_type': _type,
                             '_symbol': symbol.replace('FOREX.', ''),
                             '_price': price,
                             '_SL': 0,
                             '_TP': 0,
                             '_comment': client,
                             '_lots': size,
                             '_magic': 0,
                             '_ticket': 0}
            await self._connector._DWX_MTX_NEW_TRADE_(_order=order_to_open)

    async def cancel_order(self, order_id: str):
        resp = await self._with_pull_result(
            'CLOSE', self._connector._DWX_MTX_CLOSE_TRADE_BY_TICKET_(order_id))
        if resp['_response_value'] == 'SUCCESS':
            del self._pending_orders[order_id]
            return resp['_ticket']
        else:
            return None

    async def watch_orders(self):
        try:
            while not self._pill.is_set():
                raw_all_orders = (await self._with_pull_result('OPEN_TRADES',
                                                               self._connector._DWX_MTX_GET_ALL_OPEN_TRADES_()))['_trades']
                all_orders = [self._parse_order(
                    order_id,
                    order) for order_id,
                    order in raw_all_orders.items()]
                all_orders = [order for order in all_orders if order['client']]

                for order in all_orders:
                    order_id = order['order_id']
                    # filled order, yield and remove from local
                    if order_id in self._pending_orders and order != self._pending_orders[
                            order_id] and order['filled'] != 0:
                        yield order
                        del self._pending_orders[order_id]
                    # created, yield and save
                    if order_id not in self._pending_orders and order['filled'] == 0:
                        print('created yield and save')
                        yield order
                        self._pending_orders[order_id] = order
                # cancel order, yield and remove
                remove_order = []
                for order_id in self._pending_orders.keys():
                    if order_id not in [o['order_id'] for o in all_orders]:
                        print('cancel, yield and remove')
                        old_order = self._pending_orders[order_id]
                        old_order['status'] = 'canceled'
                        yield old_order
                        remove_order.append(order_id)
                for remove_order_id in remove_order:
                    del self._pending_orders[remove_order_id]

                await sleep(1)
        except BaseException as e:
            print(e)

    async def fetch_open_orders(self, client_id: str):
        raw_open_orders = (await self._with_pull_result('OPEN_TRADES',
                                                        self._connector._DWX_MTX_GET_ALL_OPEN_TRADES_()))['_trades']
        open_orders = [
            self._parse_order(
                order_id,
                order) for order_id,
            order in raw_open_orders.items()]
        if client_id:
            open_orders = list(
                filter(
                    lambda x: x['client'] == client_id,
                    open_orders))
        return [order for order in open_orders if order['filled'] == 0]

    async def fetch_trades(self, symbol: Optional[str] = None):
        raise AttributeError()

    async def watch_trades(self):
        return

    async def fetch_positions(self, symbol: Optional[str] = None):
        raw_open_orders = (await self._with_pull_result('OPEN_TRADES',
                                                        self._connector._DWX_MTX_GET_ALL_OPEN_TRADES_()))['_trades']
        open_orders = [
            self._parse_order(
                order_id,
                order) for order_id,
            order in raw_open_orders.items()]
        if symbol:
            open_orders = filter(lambda x: x['symbol'] == symbol, open_orders)
        filled_orders = filter(lambda x: x['filled'] != 0, open_orders)
        net_positions = self._merge_positions([self._parse_position(order)
                                               for order in filled_orders])
        return net_positions

    def _merge_positions(self, positions):
        net_positions = {}
        for each_position in positions:
            if each_position['symbol'] in net_positions:
                accumulate_position = net_positions[each_position['symbol']]
                accumulate_position_size = accumulate_position['size'] + \
                    each_position['size']
                if accumulate_position_size != 0:
                    accumulate_position_cost = (
                        accumulate_position['size'] * accumulate_position['cost'] + each_position['size'] * each_position['cost']) / accumulate_position_size
                    accumulate_position['size'] = accumulate_position_size
                    accumulate_position['cost'] = accumulate_position_cost
                else:
                    accumulate_position = {'size': 0, 'cost': 0}
            else:
                accumulate_position = {
                    'size': each_position['size'],
                    'cost': each_position['cost']
                }
            net_positions[each_position['symbol']] = accumulate_position
        return net_positions

    def _parse_position(self, order):
        return {
            'symbol': order['symbol'],
            'size': order['size'],
            'cost': order['price']
        }

    def _parse_order(self, raw_order_id, raw_order):
        return {
            'order_id': raw_order_id,
            'symbol': 'FOREX.%s' % raw_order['_symbol'],
            'size': raw_order['_lots'] * (1 if raw_order['_type'] % 2 == 0 else -1),
            'price': raw_order['_open_price'],
            'exectype': OrderBase.Market if raw_order['_type'] < 2 else OrderBase.Limit,
            'ordtype': OrderBase.Buy if raw_order['_type'] % 2 == 0 else OrderBase.Sell,
            'client': raw_order['_comment'],
            'filled': (raw_order['_lots'] * (1 if raw_order['_type'] == OrderBase.Buy else -1)) if raw_order['_type'] < 2 else 0,
            'status': 'accepted'
        }
