from xchangelib import xchange_client
import asyncio
from typing import List, Dict
from collections import defaultdict

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']
ETF: List[str] = ['JAK', 'SCP']

HIGH_CORR: List[tuple] = [('IGM', 'EPT'), ('BRV', 'MKU'), ('MKU', 'DLO'), ('BRV', 'DLO')]

ETF_COMPOSITION = {
    'JAK' : {'EPT': 2, 'DLO': 5, 'MKU': 3},
    'SCP' : {'IGM': 3, 'BRV': 4, 'EPT': 3}
}

ETF_SWAP_COST = {
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
        self.pending_cancel = set()

    async def bot_handle_book_update(self, symbol: str) -> None:
        order_book = self.order_books[symbol]
        asks = [k for k,v in order_book.asks.items() if v != 0]
        bids = [k for k,v in order_book.bids.items() if v != 0]
        best_bid = max(bids) if bids else float('-inf')
        best_ask = min(asks) if asks else float('inf')
        self.best_bids[symbol] = best_bid
        self.best_asks[symbol] = best_ask

        for asset1, asset2 in HIGH_CORR:
            if self.best_bids[asset1] and self.best_asks[asset2]:
                self.ratios[(asset1, asset2)].append(self.best_bids[asset1] / self.best_asks[asset2])
    
    
    async def cancel_outdated_orders(self):
        clone = {id: (order, qty, is_market) for id, (order, qty, is_market) in self.open_orders.items()}
        for id, (_, _, _) in clone.items():
            if id in self.pending_cancel:
                continue
            await self.cancel_order(id)
            self.pending_cancel.add(id)

    async def replace_order(self, order, order_id):
        if order.side == 1:
            price = self.best_bids[order.symbol]
            id = await self.place_order(order.symbol, order.limit.qty, xchange_client.Side.BUY, price)
        else:
            price = self.my_current_prices[order.symbol]["Best Ask"]
            print("SELL ", order.symbol, " AT: ", price, "LIQUIDATE AT: ", self.liquidate_stocks[order.symbol])
            id = await self.place_order(order.symbol, quant, xchange_client.Side.SELL, price)
        del self.id_to_spread[order_id]
        self.id_to_spread[id] = (spread, quant)


    async def check_etf_arb(self):
        self.tick += 1
        for etf in ETF:

            # Check if we can BUY ETF and SELL Underlying to make Profit
            # Calculate the net asset value (NAV) of the ETF
            nav = sum(self.best_bids[asset] * qty for asset, qty in ETF_COMPOSITION[etf].items())
            etf_price = self.best_asks[etf]
            
            theoretical_price = nav / sum(ETF_COMPOSITION[etf].values())
            arb_spread = theoretical_price - etf_price 
            arb_threshold = ETF_SWAP_COST[etf]+40

            print(f"ETF: {etf}, NAV: {nav}, ETF Price: {etf_price}, Theoretical Price: {theoretical_price}, Spread: {arb_spread}")

            if arb_spread > arb_threshold:
                print("ABRING BUY ETF AND SELL UNDERLYING")
                print("BIDS: ")
                print(self.best_bids)
                print("ASKS:")
                print(self.best_asks)
                for key, val in ETF_COMPOSITION[etf].items():
                    await self.place_order(key, val, xchange_client.Side.SELL, self.best_bids[key]-10)
                await self.place_order(etf, 10, xchange_client.Side.BUY, self.best_asks[etf]+10)
                await self.place_swap_order(f"from{etf}", 1)

            # Check if we can SELL ETF and BUY Underlying to make Profit
            # Calculate the net asset cost (NAC) of the ETF
            nac = sum(self.best_asks[asset] * qty for asset, qty in ETF_COMPOSITION[etf].items())
            etf_value = self.best_bids[etf]
            # Calculate the theoretical fair price of the ETF
            theoretical_price = nac / sum(ETF_COMPOSITION[etf].values())

            # Calculate the arbitrage opportunity, considering the redemption/creation costs
            arb_spread = etf_value - theoretical_price
            arb_threshold = ETF_SWAP_COST[etf]+40

            print(f"ETF: {etf}, NAC: {nac}, ETF VALUE: {etf_value}, Theoretical Price: {theoretical_price}, Spread: {arb_spread}")

            # Swap more conservatively since it costs money to swap.
            if arb_spread > arb_threshold:
                print("ABRING SELL ETF AND BUY UNDERLYING")
                print("BIDS: ")
                print(self.best_bids)
                print("ASKS:")
                print(self.best_asks)
                for key, val in ETF_COMPOSITION[etf].items():
                    await self.place_order(key, val, xchange_client.Side.BUY, self.best_asks[key]+10)
                await self.place_order(etf, 10, xchange_client.Side.SELL, self.best_bids[etf]-10)
                await self.place_swap_order(f"to{etf}", 1)
            


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
            await asyncio.sleep(0.2)
            await self.check_for_trade()
            await self.check_etf_arb()
            await self.cancel_outdated_orders()

            if self.tick % 12 == 0:
                self.calulate_pnl()

    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()

async def main():
    SERVER = '18.188.190.235:3333'
    username = "carnegiemellon"
    password = "charizard-exeggutor-399"

    bot = PairsTradETFArbBot(SERVER, username, password)
    await bot.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())



