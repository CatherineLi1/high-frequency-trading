import pandas as pd
import numpy as np


class Backtester:
    """
    Pairs mean-reversion backtester with:
    - dollar-neutral sizing (fixed notional per leg)
    - optional beta-neutral sizing
    - realized PnL updates capital
    - optional transaction costs (bps)
    - returns via equity pct_change
    """

    def __init__(
        self,
        df_a,
        df_b,
        z_entry=1.2,
        z_exit=0.2,
        spread_window=300,
        hedge_window=300,
        initial_capital=1_000_000,
        notional_per_leg=100_000,     # NEW: $ per leg
        beta_neutral=True,            # NEW
        cost_bps=0.0,                 # NEW: per trade leg, in bps of notional
        use_log_price_spread=True,    # NEW: log-price spread (recommended)
        max_hold_bars=None,           # NEW: optional risk control
        stop_z=None                   # NEW: optional stop if |z| too large
    ):
        self.df_a = df_a.copy()
        self.df_b = df_b.copy()
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.spread_window = spread_window
        self.hedge_window = hedge_window
        self.initial_capital = initial_capital

        self.notional_per_leg = float(notional_per_leg)
        self.beta_neutral = bool(beta_neutral)
        self.cost_bps = float(cost_bps)
        self.use_log_price_spread = bool(use_log_price_spread)
        self.max_hold_bars = max_hold_bars
        self.stop_z = stop_z

    def prepare_data(self):
        idx = self.df_a.index.intersection(self.df_b.index)
        df = pd.DataFrame(index=idx)

        df["price_a"] = self.df_a.loc[idx, "close"]
        df["price_b"] = self.df_b.loc[idx, "close"]

        if self.use_log_price_spread:
            # log prices for hedge regression
            df["x"] = np.log(df["price_b"])
            df["y"] = np.log(df["price_a"])
        else:
            # fallback: 5-min log returns as in your original
            df["y"] = np.log(df["price_a"] / df["price_a"].shift(1))
            df["x"] = np.log(df["price_b"] / df["price_b"].shift(1))

        df = df.dropna()

        # Rolling hedge ratio beta (and optional alpha for log-price spread)
        betas = []
        alphas = []
        for i in range(len(df)):
            if i < self.hedge_window:
                betas.append(np.nan)
                alphas.append(np.nan)
            else:
                y = df["y"].iloc[i - self.hedge_window:i].values
                x = df["x"].iloc[i - self.hedge_window:i].values
                # y = alpha + beta*x
                beta, alpha = np.polyfit(x, y, 1)
                betas.append(beta)
                alphas.append(alpha)

        df["beta"] = betas
        df["alpha"] = alphas

        # Spread definition
        if self.use_log_price_spread:
            df["spread"] = df["y"] - (df["alpha"] + df["beta"] * df["x"])
        else:
            df["spread"] = df["y"] - df["beta"] * df["x"]

        df["spread_mean"] = df["spread"].rolling(self.spread_window).mean()
        df["spread_std"] = df["spread"].rolling(self.spread_window).std(ddof=0)
        df["z"] = (df["spread"] - df["spread_mean"]) / df["spread_std"]

        return df.dropna()

    def _trade_cost(self):
        # cost_bps applies per leg on notional
        if self.cost_bps <= 0:
            return 0.0
        return 2.0 * self.notional_per_leg * (self.cost_bps / 10_000.0)  # 2 legs

    def run(self):
        df = self.prepare_data()

        capital = float(self.initial_capital)

        # position: +1 means long spread (long A, short B*beta), -1 means short spread
        position = 0
        entry_idx = None

        # shares
        sh_a = 0.0
        sh_b = 0.0

        equity_series = []
        trade_count = 0

        for t, row in df.iterrows():
            z = float(row["z"])
            beta = float(row["beta"])
            price_a = float(row["price_a"])
            price_b = float(row["price_b"])

            # optional stop
            stop_out = (self.stop_z is not None) and (abs(z) >= float(self.stop_z))

            # check max hold
            held_too_long = False
            if position != 0 and self.max_hold_bars is not None and entry_idx is not None:
                # count bars since entry
                held_bars = df.index.get_loc(t) - df.index.get_loc(entry_idx)
                held_too_long = held_bars >= int(self.max_hold_bars)

            # ENTRY
            if position == 0:
                if z > self.z_entry:
                    position = -1  # short spread
                elif z < -self.z_entry:
                    position = 1   # long spread

                if position != 0:
                    # size positions
                    # leg A: fixed notional
                    sh_a = (self.notional_per_leg / price_a) * position

                    # leg B: fixed notional, optionally scaled by beta
                    scale = abs(beta) if self.beta_neutral else 1.0
                    sh_b = -(self.notional_per_leg * scale / price_b) * position

                    # pay entry costs
                    capital -= self._trade_cost()
                    trade_count += 1
                    entry_idx = t

            # EXIT
            else:
                if (abs(z) < self.z_exit) or stop_out or held_too_long:
                    # realize PnL at exit: close both legs at current prices
                    # realized pnl since entry is embedded in mark-to-market; simplest:
                    # add current mark-to-market and then flatten by resetting shares and position.
                    # We realize by updating capital with current MTM (computed below) and then zeroing.
                    pass

            # Mark-to-market PnL
            mtm = sh_a * price_a + sh_b * price_b

            # If in position, we want equity = capital + (MTM - entry_cost_basis).
            # Since we deducted costs from capital and positions are sized in shares,
            # we can treat mtm as the portfolio value of legs and capital as cash.
            equity = capital + mtm
            equity_series.append(equity)

            # If we triggered exit, realize and flatten AFTER recording equity at this bar
            if position != 0 and ((abs(z) < self.z_exit) or stop_out or held_too_long):
                # Realize: set capital = equity (cash out), then pay exit cost, then flatten
                capital = equity
                capital -= self._trade_cost()

                position = 0
                sh_a = 0.0
                sh_b = 0.0
                entry_idx = None

        results = pd.DataFrame({"equity": pd.Series(equity_series, index=df.index)})
        results["returns"] = results["equity"].pct_change()

        print("Total trades:", trade_count)
        return results, trade_count

    @staticmethod
    def performance_metrics(results):
        equity = results["equity"].dropna()
        rets = results["returns"].dropna()

        if rets.std(ddof=0) == 0 or len(rets) < 2:
            sharpe = np.nan
        else:
            sharpe = np.sqrt(252 * 78) * rets.mean() / rets.std(ddof=0)

        rolling_max = equity.cummax()
        drawdown = (equity - rolling_max) / rolling_max
        max_dd = drawdown.min()

        return {"Sharpe": sharpe, "Max_Drawdown": max_dd}