import logging

from livetrader.rpc import Server
from livetrader.trade import DwxTrade, TradeService


def create_server():
    trader = DwxTrade(host='192.168.50.113')
    service = TradeService(trader, ['FOREX.EURUSD'])
    server = Server(service)
    server.bind('ipc:///tmp/market')
    return server


if __name__ == "__main__":
    logging.basicConfig(format=' %(name)s :: %(levelname)-8s :: %(message)s')
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    server = create_server()
    server.run()
