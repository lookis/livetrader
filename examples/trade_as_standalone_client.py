import asyncio
import logging

from livetrader.rpc import Client, TradeSubscriber


class PrintSubscriber(TradeSubscriber):

    def on_order(self, order: dict):
        print(order)

    def on_trade(self, trade: dict):
        print(trade)


def subscribe_trade(endpoint: str, client_id: str):
    subscriber = PrintSubscriber(client_id)
    subscriber.connect('ipc:///tmp/market')
    subscriber.run()
    return subscriber


async def main():
    endpoint = "ipc:///tmp/market"
    symbol = 'FOREX.EURUSD'
    subscriber = subscribe_trade(endpoint, 'backtrader')
    await asyncio.sleep(200)
    subscriber.close()


if __name__ == "__main__":
    logging.basicConfig(format=' %(name)s :: %(levelname)-8s :: %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    asyncio.run(main())
