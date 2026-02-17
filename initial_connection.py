from ib_insync import util, IB, Stock, MarketOrder, LimitOrder
import pandas as pd
util.startLoop()

# Connect API
ib = IB()
ib.connect('127.0.0.1', 7497, clientId=2)  # 7497 paper, 7496 live
ib.reqMarketDataType(3)

def get_1min(symbol):
    contract = Stock(symbol, 'SMART', 'USD')
    ib.qualifyContracts(contract)

    # Trades (High / Low / Last / Volume)
    trades = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='1 D',
        barSizeSetting='1 min',
        whatToShow='TRADES',
        useRTH=True
    )

    # Bid
    bid = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='1 D',
        barSizeSetting='1 min',
        whatToShow='BID',
        useRTH=True
    )

    # Ask
    ask = ib.reqHistoricalData(
        contract,
        endDateTime='',
        durationStr='1 D',
        barSizeSetting='1 min',
        whatToShow='ASK',
        useRTH=True
    )

    df = util.df(trades)[['date','high','low','close','volume']]
    df = df.rename(columns={'date':'timestamp','close':'last'})

    df['bid'] = util.df(bid)['close']
    df['ask'] = util.df(ask)['close']
    df['symbol'] = symbol

    return df

df = pd.concat([get_1min('AAPL'), get_1min('AMZN')])
print(df.head())
print(df.tail())

def get_position(symbol: str) -> float:
    ps = [p for p in ib.positions() if p.contract.symbol == symbol]
    return ps[0].position if ps else 0.0

def place_mkt_order(ticker, side, volume, max_position=0):
    stock = Stock(ticker, 'SMART', 'USD')
    current_position = get_position(ticker)
    print(f"{ticker} current position:", current_position)

    if current_position >= max_position:
        print(f"[SKIP] {ticker} position already >= {max_position}.")
        return False

    order = MarketOrder(side, volume, tif='DAY')
    order.transmit = True
    trade = ib.placeOrder(stock, order)

    ib.sleep(1) # Ensure order is processed

    # Now retrieve order metrics
    print("Order Status:", trade.orderStatus.status)
    return trade

def trades_df(trade):
    fills_df = util.df(trade.fills)
    if fills_df is None or fills_df.empty:
        print("No fills (order not filled). Check TWS Orders/Trades for details.")
        return
    
    fills_df['exec_time'] = fills_df['execution'].apply(lambda e: e.time)
    fills_df['exec_price'] = fills_df['execution'].apply(lambda e: e.price)
    fills_df['exec_shares'] = fills_df['execution'].apply(lambda e: e.shares)
    fills_df['commission'] = fills_df['commissionReport'].apply(
        lambda c: c.commission if c and c.commission is not None else float('nan')
    )
    return fills_df[['exec_time','exec_price','exec_shares','commission']]


aapl_trade = place_mkt_order('AAPL', 'BUY', 100, max_position=300)
df_aapl = trades_df(aapl_trade)