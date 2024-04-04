from xchangelib import xchange_client
import asyncio
from typing import List, Dict
from dotenv import dotenv_values, find_dotenv
from collections import defaultdict

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']
ETF: List[str] = ['JAK', 'SCP']

HIGH_CORR: List[tuple] = [('IGM', 'EPT'), ('BRV', 'MKU'), ('MKU', 'DLO'), ('BRV', 'DLO')]

class PairsTradingBot(xchange_client.XChangeClient):
    def __init__(self, host: str, username: str, password: str, asset1: str, asset2: str) -> None:
        super().__init__(host, username, password)
        self.asset1 = asset1
        self.asset2 = asset2
        self.best_bids = defaultdict(int)
        self.best_asks = defaultdict(int)
        self.rolling_window = 20
        self.ratios = [] # List of ratios of asset1 to asset2
        self.qty = 3 # Quantity to trade for now

    async def bot_handle_book_update(self, symbol: str) -> None:
        order_book = self.order_books[symbol]
        best_bid = max(order_book.bids.keys()) if order_book.bids else 0
        best_ask = min(order_book.asks.keys()) if order_book.asks else float('inf')

        if symbol == self.asset1:
            self.best_bids[symbol] = best_bid
            self.best_asks[symbol] = best_ask
        elif symbol == self.asset2:
            self.best_bids[symbol] = best_bid
            self.best_asks[symbol] = best_ask

        if self.best_bids[self.asset1] and self.best_asks[self.asset2]:
            self.ratios.append(self.best_bids[self.asset1] / self.best_asks[self.asset2])


    async def trade(self):
        while True:
            await asyncio.sleep(1)
            await self.check_for_trade()

    async def check_for_trade(self):
        if len(self.ratios) < self.rolling_window:
            return

        rolling_avg = sum(self.ratios[-self.rolling_window:]) / self.rolling_window
        rolling_std = (sum((x - rolling_avg) ** 2 for x in self.ratios[-self.rolling_window:]) / self.rolling_window) ** 0.5

        if rolling_std == 0:
            return

        print(f'Rolling Average: {rolling_avg}, Rolling Std: {rolling_std}')


        z_score = (self.ratios[-1] - rolling_avg) / rolling_std

        print(f"Z Score: {(self.ratios[-1] - rolling_avg) / rolling_std}")

        # If z_score is greater than 1, sell asset1 and buy asset2
        # If z_score is less than -1, buy asset1 and sell asset2
        if z_score == 1:
            print('Selling asset1 and buying asset2 since z_score is greater than 1')
            await self.place_order(self.asset1, self.qty, xchange_client.Side.SELL)
            await self.place_order(self.asset2, self.qty, xchange_client.Side.BUY)
        elif z_score == -1:
            print('Buying asset1 and selling asset2 since z_score is less than -1')
            await self.place_order(self.asset1, self.qty, xchange_client.Side.SELL)
            await self.place_order(self.asset2, self.qty, xchange_client.Side.BUY)

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()

# Create a manger class that will manage all the bots and run them in
# seperate threads.

class PairsTradingManager:
    def __init__(self, host: str, username: str, password: str) -> None:
        self.host = host
        self.username = username
        self.password = password
        self.bots = []

    def create_bots(self) -> None:
        asset1, asset2 = HIGH_CORR[0]
        bot = PairsTradingBot(self.host, self.username, self.password, asset1, asset2)
        self.bots.append(bot)
        # for asset1, asset2 in HIGH_CORR[0]:
        #     bot = PairsTradingBot(self.host, self.username, self.password, asset1, asset2)
        #     self.bots.append(bot)

    async def start(self) -> None:
        self.create_bots()
        await asyncio.gather(*(bot.start() for bot in self.bots))

async def main():
    config = dotenv_values(find_dotenv('.env'))
    SERVER = config['SERVER']
    username = config['USERNAME']
    password = config['PASSWORD']

    manager = PairsTradingManager(SERVER, username, password)
    await manager.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())








