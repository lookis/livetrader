## livetrader

### 介绍
livetrader 是一个整合了行情和交易接口的工具链。现在已经有很多比较成熟的交易框架了，比方说 [vnpy](https://github.com/vnpy/vnpy)、[quantaxis](https://github.com/QUANTAXIS/QUANTAXIS)、[backtrader](https://github.com/mementum/backtrader)、[zipline](https://github.com/quantopian/zipline)等，但有一个问题是这些框架都没有很好地解决的，那就是：不同框架对实盘行情、实盘交易的支持都不是很完整。

比方说 vnpy 的[交易接口](https://www.vnpy.com/docs/cn/gateway.html#id7)主要集中在数字货币、期货上，对于外汇只有 MT5 的接口，对于美股也是只有富途证券。再比方说 quantaxis 也主要集中在 A股和期货之上（[链接](https://doc.yutiansut.com/datafetch)），对于实盘交易接口部分因为了解的不多，所以不做过多评价，但就看文档而言，好似并没有找到实盘的支持。更别说国外的几个框架了 backtrader、zipline 的实盘基本上就只有 盈透(美股)、oanda(外汇) 这两 broker 的支持。

这样就会让我们遇到一个问题就是如果我们是一个单策略、多品种的交易模式的话，如果要覆盖所有的可程序化交易的 期货、美股、外汇、数字货币 就需要在不同的平台上维护多份代码，想要交易MT4的，就只能使用 MQL 语言编写策略，想要交易美股盈透证券的，就需要用 backtrader 来写逻辑，而在国内要交易期货的话， vnpy 和 quantaxis 又是两个不一样的选型。

所以，这个项目的目标是为了补充这些框架里不足的那一部分，**集中把行情接口、交易接口统一化**，然后再针对现在比较流行的几个交易框架(vnpy、quantaxis、backtrader) 提供相关的接口代码，这样无论之前是在哪个框架下编写的策略，就都可以获取全品种数据、交易全品种了（期货、美股、外汇、数字货币）

### 使用方式

#### 安装

```bash
pip install -U livetrader
```
使用：可以参考 examples 目录里的代码

```python
# 作为模块启动
import asyncio

from simpletrader.market import (CachedMarket, DwxMarket, MarketService,
                                 TdxMarket)


async def listern_to_latest_kline(kline_queue):
    print('get latest kline')
    for i in range(5):
        (symbol, kline) = await kline_queue.get()
        print("%s, %s" % (symbol, kline))


async def get_kline_histories(service):
    print('get kline histories')
    histories = await service.get_kline_histories('US.BABA', limit=100)
    print(len(histories))


async def main():
    market = TdxMarket(host='xx.xx.xx.xx:7727')
    service = MarketService(market, ['US.BABA'])
    kline_queue = service.start()
    await listern_to_latest_kline(kline_queue)
    await get_kline_histories(service)
    service.stop()

if __name__ == "__main__":
    asyncio.run(main())

```

```python
# 作为服务启动
import logging

from livetrader.market import CachedMarket, DwxMarket, MarketService, TdxMarket
from livetrader.rpc import Server


def create_server():
    market = TdxMarket(host='xx.xx.xx.xx:7727')
    service = MarketService(market, ['US.BABA'])
    server = Server(service)
    server.bind('ipc:///tmp/market')
    return server


if __name__ == "__main__":
    logging.basicConfig(format=' %(name)s :: %(levelname)-8s :: %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    server = create_server()
    server.run()

```
```python
# 对应服务的客户端
import asyncio
import logging

from livetrader.rpc import Client, MarketSubscriber


class PrintSubscriber(MarketSubscriber):

    def on_kline(self, kline: dict):
        print('recv kline: %s' % kline, flush=True)


def subscribe_kline(endpoint: str, symbol: str):
    subscriber = PrintSubscriber(symbol)
    subscriber.connect('ipc:///tmp/market')
    subscriber.run()
    return subscriber


async def main():
    endpoint = "ipc:///tmp/market"
    symbol = 'US.BABA'
    subscriber = subscribe_kline(endpoint, symbol)
    await asyncio.sleep(5)
    subscriber.close()

    client = Client()
    client.connect(endpoint)
    histories = client.get_kline_histories(symbol, None, 100)
    print(len(histories))
    client.close()


if __name__ == "__main__":
    logging.basicConfig(format=' %(name)s :: %(levelname)-8s :: %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    asyncio.run(main())

```

## 程序架构

可以参考知乎的文章，知乎上的文章是随着项目开发，把思考过程都记录下来了：[知乎: 从零架构一个交易框架](https://zhuanlan.zhihu.com/p/268036337)

### RPC
因为这个项目的目标是为了服务各种不同的策略、不同和框架，所以基于不同的策略（日内、高频、日间）场景，提供了不一样的调用方式，而实现的方式主要基于 zeromq 来做的。

如果需要低延迟的话，可以以模块的方式加载服务，或者使用 zeromq 提供的 inproc 连接模式

如果需要本地调试方便，可以使用 ipc 的方式连接

如果需要分布式部署，可以使用 tcp 的方式连接

只要我们使用的不是模块的方式加载，我们都还会通过 MessagePack 的方式来序列化传输数据。在早期为了快速开发，就直接采用了 [zerorpc](http://www.zerorpc.io/) 来实现，但是 zerorpc 在性能上的问题比较大，所以到后期会重写一个 RPC 模块来替换掉

### 行情模块

第一个版本主要是做了以下行情的适配：

1. 美股：使用 pytdx 获取免费分钟数据

2. 外汇MT4：[dwx_zeromq_connector](https://github.com/darwinex/dwx-zeromq-connector)

3. 期货：计划采用 tqsdk 获取数据


### 交易模块

暂无，计划先做期货或者外汇

### 框架适配模块

暂无，计划先做 backtrader 的适配

## 关于 pytdx 的地址

可以参考 quantaxis 里的记录 [美股行情地址](https://github.com/QUANTAXIS/QUANTAXIS/blob/master/QUANTAXIS/QAUtil/QASetting.py#L364-L385)

## 关于外汇 MT4 

因为 MT4 没有提供官方的 Api 接口，想要把里面的行情数据拿出来或者提交下单接口就只有两个方案：

1. 像pytdx一样抓包或者反编译

2. 用 MQL 语言写一个转发

好在现在已经有开源项目完成了后一种做法，这里就直接拿来用了：[dwx_zeromq_connector](https://github.com/darwinex/dwx-zeromq-connector)，具体如何部署可以参考这个项目里面的说明。要注意的是我在原项目上做了一点修改，可以直接使用 extra 目录里的 mq4 代码，然后在 python 这边把原项目的多线程版本改为了 asyncio 的异步版本，可以直接使用。

## 其它

有什么好的想法和建议欢迎提issue，更新应该比较勤快，欢迎 star 和 fork 这个项目。知乎上也请大家多多支持和关注 [知乎专栏](https://www.zhihu.com/column/c_1177533241622593536)
