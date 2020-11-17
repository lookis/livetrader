import asyncio
import logging

from livetrader.rpc import Client, MarketSubscriber


class PrintSubscriber(MarketSubscriber):

    def on_kline(self, kline: dict):
        print('recv kline: %s' % kline, flush=True)

    def on_state(self, isalive: bool):
        print('connect state isalive? %s' % isalive)


def subscribe_kline(endpoint: str, symbol: str):
    subscriber = PrintSubscriber(symbol)
    subscriber.connect('ipc:///tmp/market')
    subscriber.run()
    return subscriber


async def main():
    endpoint = "ipc:///tmp/market"
    symbol = 'FOREX.EURUSD'
    subscriber = subscribe_kline(endpoint, symbol)
    await asyncio.sleep(200)
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
