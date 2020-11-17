import asyncio

from livetrader.market import CachedMarket, DwxMarket, MarketService, TdxMarket


async def listern_to_latest_kline(kline_queue):
    print('get latest kline')
    for i in range(5):
        (symbol, _, kline) = await kline_queue.get()
        print("%s, %s" % (symbol, kline))


async def get_kline_histories(service):
    print('get kline histories')
    histories = await service.get_kline_histories('FOREX.EURUSD', limit=100)
    print(len(histories))


async def main():
    # Dwx 为外汇的行情获取，请参考文档设置 MT4
    market = DwxMarket(host='192.168.50.113')
    service = MarketService(market, ['FOREX.EURUSD'])
    kline_queue = service.start()
    await listern_to_latest_kline(kline_queue)
    await get_kline_histories(service)
    service.stop()

if __name__ == "__main__":
    asyncio.run(main())
