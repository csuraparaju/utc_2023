from typing import Dict, List, Optional
import asyncio
from xchangelib import xchange_client
from dotenv import dotenv_values, find_dotenv
from collections import defaultdict
from dataclasses import dataclass

# Test bot for market making the underlying stock assets,
# but ignores any ETFs that track them. Uses methods described
# in https://tianyi.io/post/chicago1/ to make profits from
# the spread between the bid and ask prices of the underlying assets.

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']

class UnderlyingMM(xchange_client.XChangeClient):
    def __init__(self, host: str, username: str, password: str) -> None:
        super().__init__(host, username, password)
        self.edge = 50 # the spread between the bid and the ask prices
        self.size = 10 # start out with placing 10 orders

        # Keeps track of the current fair price of each symbol
        self.fairs = defaultdict(int) # symbol -> fair price

        # Keeps track of all the open orders for a given symbol.
        # Maps symbol to a list of orders.
        self.symbol_open_orders = defaultdict(list) # symbol -> list of orders


    async def bot_handle_book_update(self, symbol: str) -> None:
        # Update the fair price based on the book updates. Compute
        # the fair price as the average of the best bid and ask prices.
        order_book = self.order_books[symbol]
        best_bid = max(order_book.bids.keys()) if order_book.bids else 0
        best_ask = min(order_book.asks.keys()) if order_book.asks else 0
        self.fairs[symbol] = (best_bid + best_ask) // 2

        # Update the symbol_open_orders dictionary with the new order book.
        self.symbol_open_orders[symbol] = [order for order in self.open_orders.values() if order[0].symbol == symbol]

    async def bot_handle_order_fill(self, order_id: str, qty: int, price: int):
        # Updates the symbol_open_orders dictionary to reflect the
        # filled order.
        order = self.open_orders[order_id]
        symbol = order[0].symbol
        self.symbol_open_orders[symbol].remove(order) if order in self.symbol_open_orders[symbol] else None

    async def bot_handle_order_rejected(self, order_id: str, reason: str) -> None:
        # Removes the rejected order from the symbol_open_orders dictionary.
        order = self.open_orders[order_id]
        symbol = order[0].symbol
        self.symbol_open_orders[symbol].remove(order) if order in self.symbol_open_orders[symbol] else None


    async def trade(self):
        # Start the trading task right before the bot connects to the exchange.
        # This task will run in the background and execute the market making strategy.
        while True:
            await asyncio.sleep(1)
            for symbol in UNDERLYING:
                fair = self.fairs[symbol]
                bid = fair - self.edge
                ask = fair + self.edge

                # Check if there are competing bids or asks of the same price
                # in the order book.
                    # If there are, loop through our open orders and check if we have already
                    # placed an order already. If we haven't, then penny any competing
                    # orders by increasing the bid and decreasing the ask by 1.
                if bid in self.order_books[symbol].bids:
                    for order in self.symbol_open_orders[symbol]:
                        if order[0].limit.px == bid:
                            break
                    else:
                        bid -= 1

                if ask in self.order_books[symbol].asks:
                    for order in self.symbol_open_orders[symbol]:
                        if order[0].limit.px == ask:
                            break
                    else:
                        ask += 1


                print(f"Buying at {bid} and selling at {ask}")
                await self.place_order(symbol, self.size, xchange_client.Side.BUY, bid)
                await self.place_order(symbol, self.size, xchange_client.Side.SELL, ask)

    async def start(self):
        # Start the bot and connect to the exchange.
        asyncio.create_task(self.trade())
        await self.connect()


async def main():
    # Load the environment variables from the .env file.
    env = dotenv_values(find_dotenv())
    host = env['SERVER']
    username = env['USERNAME']
    password = env['PASSWORD']

    # Create an instance of the bot and start it.
    bot = UnderlyingMM(host, username, password)
    await bot.start()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())









