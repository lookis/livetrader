import logging

from livetrader.market import CachedMarket, DwxMarket, MarketService, TdxMarket
from livetrader.rpc import Server


def create_server():
    market = CachedMarket(
        TdxMarket(
            host='59.175.238.38:7727'),
        mongodb_uri='mongodb://root:example@127.0.0.1:27017/?authMechanism=SCRAM-SHA-256')
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
