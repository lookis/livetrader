
from typing import List, Optional

from livetrader.rpc import Method, Publisher, Subscriber


# 仅用来标识一些常量，具体order对象依旧是 dict 对象
class OrderBase():
    ExecTypes = ['Market', 'Close', 'Limit', 'Stop', 'StopLimit', 'StopTrail',
                 'StopTrailLimit', 'Historical']

    OrdTypes = ['Buy', 'Sell']
    Buy, Sell = range(2)

    Created, Submitted, Accepted, Partial, Completed, \
        Canceled, Expired, Rejected = range(9)

    Cancelled = Canceled  # alias

    Status = [
        'Created', 'Submitted', 'Accepted', 'Partial', 'Completed',
        'Canceled', 'Expired', 'Rejected',
    ]


# 为了适配不同的交易接口，有些是推送数据（比方说期货）有些是拉数据（比方说数字货币），我们都用TradeBase 来代表，然后用 TradeService 封装出统一接口
# 在 TradeBase 里如果是推送的接口，就直接 yield 出来，如果是拉数据的，就定时 polling，然后缓存在
# TradeService 里（数据库），之后都统一出推送接口和polling 接口，这样TradeBase可以自己控制调用接口的频率，然后
# TradeService 对外就没有调用限制了


class TradeBase():
    def connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

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
            leverage 杠杆大小
            mult 乘数
            commission 费率
            margin 保证金
            shortable 可做空
        """
        raise NotImplementedError()

    async def create_order(self, symbol: str, ordtype: int, exectype: int, price: float, size: int, client: Optional[str] = None):
        """
        symbol: 标的，SHFE.rb2101
        ordtype: OrderBase.OrdTypes
        exectype: OrderBase.ExecTypes
        price: 价格
        size: 正数为 Long 多单, 负数为 Short 空单
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

    async def watch_orders(self, symbol: str):
        raise NotImplementedError()

    async def fetch_orders(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    async def fetch_open_orders(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    async def fetch_trades(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    async def watch_trades(self, symbol: str):
        raise NotImplementedError()

    async def fetch_positions(self, symbol: Optional[str] = None):
        raise NotImplementedError()


class TradeSubscriber(Subscriber):

    def on_order(self, order: dict):
        raise NotImplementedError()

    def on_trade(self, trade: dict):
        raise NotImplementedError()


class TradeService():

    def __init__(self):
        self._publishers = []
        self._tasks = []

    async def _publish(self):
        raise NotImplementedError()

    def start(self):
        # start publish service(polling and push)
        raise NotImplementedError()

    @Method
    async def get_balance(self):
        raise NotImplementedError()

    @Method
    async def create_order(self, symbol: str, exectype: str, price: float, size: int):
        raise NotImplementedError()

    @Method
    async def cancel_order(self, order_id: str):
        raise NotImplementedError()

    @Method
    async def open_orders(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    @Method
    async def today_trades(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    @Method
    async def fetch_position(self, symbol: Optional[str] = None):
        raise NotImplementedError()

    def stop(self):
        for publisher in self._publishers:
            publisher.close()
        for task in self._tasks:
            task.cancel()
