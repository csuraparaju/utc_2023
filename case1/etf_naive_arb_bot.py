from typing import Dict, List, Optional
import asyncio
from xchangelib import xchange_client
from dotenv import dotenv_values, find_dotenv
from collections import defaultdict
from dataclasses import dataclass

# A test bot that checks for arbitrage opportunities
# between the underlying price and the price of the
# ETF that tracks it. Takes advantage of the fact that
# sometimes the ETF price lags behind the underlying.

@dataclass
class SwapInfo:
    swap_name: str
    from_info: list
    to_info: list
    cost: int
    is_flat: bool


SWAP_MAP: Dict[str, SwapInfo] = {'toJAK': SwapInfo('toJAK', [('EPT', 2), ('DLO', 5), ('MKU', 3)], [('JAK', 10)], 5, True),
            'fromJAK': SwapInfo('fromJAK', [('JAK', 10)], [('EPT', 2), ('DLO', 5), ('MKU', 3)], 5, True),
            'toSCP': SwapInfo('toSCP', [('IGM', 3), ('BRV', 4), ('EPT', 3)], [('SCP', 10)], 5, True),
            'fromSCP': SwapInfo('fromSCP', [('SCP', 10)], [('IGM', 3), ('BRV', 4), ('EPT', 3)], 5, True),
            }

UNDERLYING: List[str] = ['EPT', 'DLO', 'MKU', 'IGM', 'BRV']
ETF: List[str] = ['JAK', 'SCP']

class NaiveETFArb(xchange_client.XChangeClient):
    def __init__(self, host: str, username: str, password: str) -> None:
        super().__init__(host, username, password)
        self.best_underlying_asks: Dict[str, int] = defaultdict(int)
        self.best_underlying_bids: Dict[str, int] = defaultdict(int)
        self.best_etf_asks: Dict[str, int] = defaultdict(int)
        self.best_etf_bids: Dict[str, int] = defaultdict(int)

    async def bot_handle_book_update(self, symbol: str) -> None:
        # Update the underlying and ETF prices based on the book updates.
        # The order_books field is a dictionary that maps symbols to the
        # order book object for that symbol. Store the best bid and asks
        # for each symbol in the self.underlying_asks and self.underlying_bids
        order_book = self.order_books[symbol]
        best_bid = max(order_book.bids.keys()) if order_book.bids else 0
        best_ask = min(order_book.asks.keys()) if order_book.asks else 0
        if symbol in UNDERLYING:
            self.best_underlying_asks[symbol] = best_ask
            self.best_underlying_bids[symbol] = best_bid
        elif symbol in ETF:
            self.best_etf_asks[symbol] = best_ask
            self.best_etf_bids[symbol] = best_bid

    async def self_check_arb_opp(self, symbol: str) -> None:
        # Check for arbitrage opportunities between the underlying and ETF prices
        # by comparing the best bid and ask prices for each symbol. If the ETF price
        # is lagging behind the underlying price, there is an opportunity to buy the
        # ETF and sell the underlying to make a profit. If the ETF price is higher
        # than the underlying price, there is an opportunity to buy the underlying
        # and sell the ETF to make a profit.

        if symbol in ETF:
            etf_ask = self.best_etf_asks[symbol]
            etf_bid = self.best_etf_bids[symbol]

            # Buy and sell the ETF
            swap_info = SWAP_MAP[f'to{symbol}']
            underlying_cost = sum(self.best_underlying_asks[u] * amt for u, amt in swap_info.from_info)
            if etf_bid > underlying_cost + swap_info.cost:
                print(f"Arbitrage opportunity: Buy {symbol} ETF at {etf_bid} and sell underlying at {underlying_cost}")
                await self.place_swap_order(swap_info.swap_name, 1)

            # Sell and buy the ETF
            swap_info = SWAP_MAP[f'from{symbol}']
            underlying_value = sum(self.best_underlying_bids[u] * amt for u, amt in swap_info.to_info)
            if etf_ask < underlying_value - swap_info.cost:
                print(f"Arbitrage opportunity: Sell {symbol} ETF at {etf_ask} and buy underlying at {underlying_value}")
                await self.place_swap_order(swap_info.swap_name, 1)


    async def trade(self) -> None:
        # Start the trading task that runs in the background.
        while True:
            await asyncio.sleep(1)
            for symbol in UNDERLYING + ETF:
                await self.self_check_arb_opp(symbol)

    async def start(self) -> None:
        asyncio.create_task(self.trade())
        await self.connect()


async def main() -> None:
    config = dotenv_values(find_dotenv('.env'))
    SERVER = config['SERVER']
    username = config['USERNAME']
    password = config['PASSWORD']
    bot = NaiveETFArb(SERVER, username, password)
    await bot.start()
    return

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

