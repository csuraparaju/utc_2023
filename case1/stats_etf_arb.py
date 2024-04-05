from xchangelib import xchange_client
import asyncio
from typing import List, Dict
from dotenv import dotenv_values, find_dotenv
from collections import defaultdict

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']
ETF: List[str] = ['JAK', 'SCP']

HIGH_CORR: List[tuple] = [('IGM', 'EPT'), ('BRV', 'MKU'), ('MKU', 'DLO'), ('BRV', 'DLO')]

ETF_COMPOSITION = {
    'JAK' : {'EPT': 2, 'DLO': 5, 'MKU': 3},
    'SCP' : {'IGM': 3, 'BRV': 4, 'EPT': 3}
}

EFT_SWAP_COST = {
    'JAK' : 5,
    'SCP' : 5
}


class PairsTradETFArbBot(xchange_client.XChangeClient):
    def __init__(self, host: str, username: str, password: str) -> None:
        super().__init__(host, username, password)
        self.best_bids = defaultdict(int)
        self.best_asks = defaultdict(int)
        self.rolling_window = 60 # window size for rollign average and std
        self.ratios = defaultdict(list)
        self.qty = 1 # qty for each asset to trade
        self.my_positions = defaultdict(int)
        self.tick = 0

    async def bot_handle_book_update(self, symbol: str) -> None:
        order_book = self.order_books[symbol]
        best_bid = max(order_book.bids.keys()) if order_book.bids else float('-inf')
        best_ask = min(order_book.asks.keys()) if order_book.asks else float('inf')

        self.best_bids[symbol] = best_bid
        self.best_asks[symbol] = best_ask

        for asset1, asset2 in HIGH_CORR:
            if self.best_bids[asset1] and self.best_asks[asset2]:
                self.ratios[(asset1, asset2)].append(self.best_bids[asset1] / self.best_asks[asset2])

    async def check_etf_arb(self):
        # Compute the theoretical price of the ETF based on the underlying assets
        # Compare this with the actual ETF price and see if there is any deviation
        for etf in ETF:
            theorec_px = sum(self.best_asks[asset] * qty for asset, qty in ETF_COMPOSITION[etf].items())
            actual_px = self.best_bids[etf]
            if theorec_px + EFT_SWAP_COST[etf] < actual_px:
                print(f'Buying {etf} and selling the underlying assets')
                await self.place_swap_order(f'to{etf}', 1)
                self.my_positions[etf] += 1
                for asset, qty in ETF_COMPOSITION[etf].items():
                    self.my_positions[asset] -= qty
            elif theorec_px - EFT_SWAP_COST[etf] > actual_px:
                print(f'Buying the underlying assets and selling {etf}')
                await self.place_swap_order(f'from{etf}', 1)
                self.my_positions[etf] -= 1
                for asset, qty in ETF_COMPOSITION[etf].items():
                    self.my_positions[asset] += qty

    async def check_for_trade(self):
        self.tick += 1
        for asset1, asset2 in HIGH_CORR:
            ratios = self.ratios[(asset1, asset2)]
            if len(ratios) < self.rolling_window:
                continue

            rolling_avg = sum(ratios[-self.rolling_window:]) / self.rolling_window
            rolling_std = (sum((x - rolling_avg) ** 2 for x in ratios[-self.rolling_window:]) / self.rolling_window) ** 0.5

            if rolling_std == 0:
                continue

            z_score = (ratios[-1] - rolling_avg) / rolling_std

            if z_score == 1:
                print(f'Selling {asset1} and buying {asset2} since z_score is greater than 1')
                await self.place_order(asset1, self.qty, xchange_client.Side.SELL)
                self.my_positions[asset1] -= self.qty
                await self.place_order(asset2, self.qty, xchange_client.Side.BUY)
                self.my_positions[asset2] += self.qty
            elif z_score == -1:
                print(f'Selling {asset2} and buying {asset1} since z_score is less than -1')
                await self.place_order(asset1, self.qty, xchange_client.Side.BUY)
                self.my_positions[asset1] += self.qty
                await self.place_order(asset2, self.qty, xchange_client.Side.SELL)
                self.my_positions[asset2] -= self.qty


    def calulate_pnl(self):
        cash = self.positions['cash']
        for symbol, qty in self.positions.items():
            if symbol == 'cash':
                continue
            settlement_fair = (self.best_bids[symbol] + self.best_asks[symbol]) // 2
            cash += qty * settlement_fair

        print()
        print(f"NET PROFIT SO FAR: {cash}")
        print()

    # TODO: Call the check_etf_arb function to do arb in parallel.
    async def trade(self):
        while True:
            await asyncio.sleep(1)
            await self.check_for_trade()

            if self.tick % 12 == 0:
                self.calulate_pnl()

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()

async def main():
    config = dotenv_values(find_dotenv('.env'))
    SERVER = config['SERVER']
    username = config['USERNAME']
    password = config['PASSWORD']

    bot = PairsTradETFArbBot(SERVER, username, password)
    await bot.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())



