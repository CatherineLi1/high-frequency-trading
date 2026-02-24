# data_layer.py
# Data layer: connect to IBKR, qualify contracts, fetch minute bars + bid/ask/last
# Upgraded to support multi-asset data retrieval

from ib_insync import IB, Stock, util


class IBKRDataClient:
    def __init__(self, host="127.0.0.1", port=7497, client_id=1, market_data_type=3):
        # Connect to TWS/IB Gateway and set market data type (3=Delayed)
        util.startLoop()
        self.ib = IB()
        self.ib.connect(host, port, clientId=client_id)
        self.ib.reqMarketDataType(market_data_type)

    def qualify(self, symbols):
        """
        Create Stock contracts and qualify them.
        Supports multiple symbols.
        """
        contracts = {s: Stock(s, "SMART", "USD") for s in symbols}
        self.ib.qualifyContracts(*contracts.values())
        return contracts
        
    def get_bars_multi(
        self,
        contracts: dict,
        duration="20 D",
        bar_size="5 mins",
        use_rth=True,
        what_to_show="TRADES",
        pause_sec=0.3,
        max_retries=2,
        retry_sleep_sec=3,
    ):
        """
        Fetch OHLCV bars for multiple assets.
        Returns dict[symbol] -> DataFrame (may be empty if failed)
        """
        data_dict = {}
    
        for symbol, contract in contracts.items():
            last_err = None
    
            for attempt in range(max_retries + 1):
                try:
                    bars = self.ib.reqHistoricalData(
                        contract,
                        endDateTime="",
                        durationStr=duration,
                        barSizeSetting=bar_size,
                        whatToShow=what_to_show,
                        useRTH=use_rth,
                        formatDate=1,
                    )
                    df = util.df(bars)
                    if df is None:
                        df = util.df([])
    
                    if len(df) > 0:
                        df["symbol"] = symbol
    
                    data_dict[symbol] = df
                    last_err = None
                    break
    
                except Exception as e:
                    last_err = e
                    self.ib.sleep(retry_sleep_sec)
    
            if last_err is not None:
                data_dict[symbol] = util.df([])
    
            self.ib.sleep(pause_sec)
    
        return data_dict
    
    
    def get_bars(
        self,
        contract,
        duration="20 D",
        bar_size="5 mins",
        use_rth=True,
        what_to_show="TRADES",
    ):
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting=bar_size,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=1,
        )
        return util.df(bars)
    
    # ====== NEW / MODIFIED ======
    def get_minute_bars_multi(self, contracts: dict, duration="1 D", use_rth=True):
        """
        Fetch 1-minute OHLCV bars for multiple assets.
        Returns:
            dict[symbol] -> DataFrame
        """
        data_dict = {}

        for symbol, contract in contracts.items():
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=use_rth,
                formatDate=1
            )
            df = util.df(bars)
            df["symbol"] = symbol  # attach symbol column
            data_dict[symbol] = df

        return data_dict
    # ====== END NEW ======

    def get_minute_bars(self, contract, duration="1 D", use_rth=True):
        """
        Fetch 1-minute OHLCV bars for single asset.
        (Kept for backward compatibility)
        """
        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=duration,
            barSizeSetting="1 min",
            whatToShow="TRADES",
            useRTH=use_rth,
            formatDate=1
        )
        return util.df(bars)

    # ====== NEW / MODIFIED ======
    def get_bid_ask_last_multi(self, contracts: dict, sleep_sec=2):
        """
        Fetch latest bid/ask/last for multiple assets.
        Returns:
            dict[symbol] -> {"bid": ..., "ask": ..., "last": ...}
        """
        result = {}

        tickers = {}
        for symbol, contract in contracts.items():
            tickers[symbol] = self.ib.reqMktData(contract)

        self.ib.sleep(sleep_sec)

        for symbol, ticker in tickers.items():
            result[symbol] = {
                "bid": ticker.bid,
                "ask": ticker.ask,
                "last": ticker.last,
                "mdType": ticker.marketDataType
            }

        return result
    # ====== END NEW ======

    def get_bid_ask_last(self, contract, sleep_sec=2):
        """
        Fetch latest bid/ask/last for single asset.
        (Kept for backward compatibility)
        """
        t = self.ib.reqMktData(contract)
        self.ib.sleep(sleep_sec)
        return {"bid": t.bid, "ask": t.ask, "last": t.last, "mdType": t.marketDataType}

    def disconnect(self):
        """
        Disconnect cleanly
        """
        self.ib.disconnect()