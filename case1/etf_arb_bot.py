from xchangelib import xchange_client
import asyncio
from typing import List, Dict
from collections import defaultdict

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']
ETF: List[str] = ['JAK', 'SCP']

ETF_COMPOSITION = {
    'JAK' : {'EPT': 2, 'DLO': 5, 'MKU': 3},
    'SCP' : {'IGM': 3, 'BRV': 4, 'EPT': 3}
}

ETF_SWAP_COST = {
    'JAK' : 5,
    'SCP' : 5
}


class ETFArbBot(xchange_client.XChangeClient):
    def __init__(self, host: str, username: str, password: str) -> None:
        super().__init__(host, username, password)
        self.best_bids = defaultdict(int) # symbol -> best bid price
        self.best_asks = defaultdict(int) # symbol -> best ask price
        self.symbol_open_orders = defaultdict(list) # Symbol -> list of order ids
        self.tick = 0


    async def bot_handle_book_update(self, symbol: str) -> None:
        order_book = self.order_books[symbol]
        asks = [k for k,v in order_book.asks.items() if v != 0]
        bids = [k for k,v in order_book.bids.items() if v != 0]
        best_bid = max(bids) if bids else float('-inf')
        best_ask = min(asks) if asks else float('inf')
        self.best_bids[symbol] = best_bid
        self.best_asks[symbol] = best_ask

        self.symbol_open_orders[symbol] = [order for order in self.open_orders.values() if order[0].symbol == symbol]

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        print(f"Order {order_id} filled with {qty} shares at {price}")
        order = self.open_orders[order_id]
        symbol = order[0].symbol
        self.symbol_open_orders[symbol].remove(order) if order in self.symbol_open_orders[symbol] else None

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        print(f"Order {order_id} rejected because of {reason}")
        order = self.open_orders[order_id]
        symbol = order[0].symbol
        self.symbol_open_orders[symbol].remove(order) if order in self.symbol_open_orders[symbol] else None


    async def bot_handle_trade_msg(self, symbol: str, price: int, qty: int):
        # TODO: Insert logic so that the bot can can figure out what the hedge funds
        # are doing and trade accordingly. Any trade with large qty should be considered
        # as a signal that the hedge funds are trying to move the market.
        pass

    async def check_etf_arb(self):
        self.tick += 1
        if len(self.open_orders)>0:
            return
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

    async def trade(self):
        # call the check_etf_arb function every second.
        # At the same time, settle all positions every minute.
        while True:
            await asyncio.sleep(1)
            await self.check_etf_arb()

            if self.tick % 12 == 0:
                self.calulate_pnl()


    async def start(self):
        asyncio.create_task(self.trade())
        await self.connect()

async def main():
    SERVER = '18.188.190.235:3333'
    username = "carnegiemellon"
    password = "charizard-exeggutor-399"

    bot = ETFArbBot(SERVER, username, password)
    await bot.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(main())



