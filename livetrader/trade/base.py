
import asyncio
import logging
from typing import List, Optional

from livetrader.rpc import Method, Publisher, Subscriber
from livetrader.utils import FifoQueue


# 仅用来标识一些常量，具体order对象依旧是 dict 对象
class OrderBase():
    ExecTypes = [
        'Market',
        'Limit',
        'Stop',
        'StopLimit',
        'StopTrail',
        'StopTrailLimit']
    (Market, Limit, Stop, StopLimit, StopTrail,
     StopTrailLimit) = range(len(ExecTypes))

    OrdTypes = ['Buy', 'Sell']
    Buy, Sell = range(len(OrdTypes))

# 为了适配不同的交易接口，有些是推送数据（比方说期货）有些是拉数据（比方说数字货币），我们都用TradeBase 来代表，然后用 TradeService 封装出统一接口
# 在 TradeBase 里如果是推送的接口，就直接 yield 出来，如果是拉数据的，就定时 polling，然后缓存在
# TradeService 里（数据库），之后都统一出推送接口和polling 接口，这样TradeBase可以自己控制调用接口的频率，然后
# TradeService 对外就没有调用限制了


class TradeBase():
    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    # 先要求支持前两种，后面四种订单因为无法做到通用性，所以只能部分支持
    def fetch_exectypes(self):
        return OrderBase.ExecTypes[:2]

    async def fetch_balance(self):
        """
        balance 里包含字段：
            cash：现金
            value: 总资产
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

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
        raise NotImplementedError()

    async def cancel_order(self, order_id: str):
        """
        这里和 create_order 一样，也是异步调用，返回只能代表交易所收到这个请求，不一定执行成功，有可能会被拒绝，需要 watch_orders 看结果
        """
        raise NotImplementedError()

    async def watch_orders(self):
        raise NotImplementedError()

    async def fetch_open_orders(self, client_id: str):
        raise NotImplementedError()

    async def fetch_trades(self, client_id: str):
        raise NotImplementedError()

    async def watch_trades(self):
        raise NotImplementedError()

    async def fetch_positions(self, client_id: str):
        raise NotImplementedError()


class TradeService():

    def __init__(self, trader: TradeBase, symbols: List[str]):
        self._trader = trader
        self._symbols = symbols
        self._tasks = []
        self.__logger__ = logging.getLogger('TradeService')
        # self._publishers = []
        # self._tasks = []

    async def _publish_order(self, queue: FifoQueue):
        async for order in self._trader.watch_orders():
            if order['client']:
                await queue.put((order['client'], 'on_order', order))

    async def _publish_trade(self, queue: FifoQueue):
        async for trade in self._trader.watch_trades():
            if trade['client']:
                await queue.put((trade['client'], 'on_trade', trade))

    def start(self):
        queue = FifoQueue(maxsize=100)
        self._trader.connect()
        self._tasks.append(asyncio.get_event_loop().create_task(
            self._publish_order(queue)))
        self._tasks.append(asyncio.get_event_loop().create_task(
            self._publish_trade(queue)))
        return queue

    @Method
    async def fetch_balance(self):
        return await self._trader.fetch_balance()

    @Method
    async def fetch_markets(self):
        return await self._trader.fetch_markets()

    @Method
    async def open_order(self, symbol: str, ordtype: int, exectype: int, size: float, client: str, price: Optional[float] = None):
        return await self._trader.open_order(symbol, ordtype, exectype, size, price, client)

    @Method
    async def cancel_order(self, order_id: str):
        return await self._trader.cancel_order(order_id)

    @Method
    async def fetch_open_orders(self, client_id: str):
        return await self._trader.fetch_open_orders(client_id)

    @Method
    async def fetch_trades(self, client_id: str):
        raise NotImplementedError()

    @Method
    async def fetch_positions(self, client_id: str):
        return await self._trader.fetch_positions(client_id)

    def stop(self):
        for task in self._tasks:
            task.cancel()
        self._trader.disconnect()
