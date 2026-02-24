# feature_layer.py
# Feature layer: compute single-asset intraday features
# + cross-asset relative value features (NEW)

import numpy as np
import pandas as pd


# ==========================================================
# 1️⃣ 原有：单资产日内特征（保持不变）
# ==========================================================

def add_intraday_features(
    df: pd.DataFrame,
    vol_window: int = 20,
    ret5_window: int = 5,
    ema_fast: int = 10,
    ema_slow: int = 30,
    z_window: int = 120,
    vol_pctl: int = 90,
    vol_pctl_lookback: int = 390
) -> pd.DataFrame:
    """
    Add intraday features:
    log_ret, r_vol, vwap, ret_5m, ema_fast/slow, z_rvol, rvol_th
    """
    df = df.copy()

    # log returns
    df["log_ret"] = np.log(df["close"]).diff()

    # rolling volatility
    df["r_vol"] = df["log_ret"].rolling(vol_window).std()

    # VWAP
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    pv = tp * df["volume"]
    df["vwap"] = pv.cumsum() / df["volume"].cumsum()

    # 5-min return
    df["ret_5m"] = np.log(df["close"] / df["close"].shift(ret5_window))

    # EMAs
    df["ema_fast"] = df["close"].ewm(span=ema_fast, adjust=False).mean()
    df["ema_slow"] = df["close"].ewm(span=ema_slow, adjust=False).mean()

    # z-score of rolling volatility
    rv = df["r_vol"]
    rv_mean = rv.rolling(z_window).mean()
    rv_std = rv.rolling(z_window).std()
    df["z_rvol"] = (rv - rv_mean) / rv_std

    # rolling percentile threshold (last row only)
    df["rvol_th"] = np.nan
    recent = df["r_vol"].dropna().iloc[-vol_pctl_lookback:]
    if len(recent) > 20:
        df.loc[df.index[-1], "rvol_th"] = float(np.nanpercentile(recent.values, vol_pctl))

    return df


# ==========================================================
# 2️⃣ NEW：跨资产相对价值特征
# ==========================================================

# ====== NEW / MODIFIED ======
def add_cross_asset_spread_features(
    df_a: pd.DataFrame,
    df_b: pd.DataFrame,
    lookback_ret: int = 20,
    spread_window: int = 60
) -> pd.DataFrame:
    """
    Compute cross-asset relative value features.

    Parameters:
        df_a : DataFrame of asset A (e.g., SPY)
        df_b : DataFrame of asset B (e.g., TLT)

    Returns:
        DataFrame aligned by index with:
            ret_a
            ret_b
            spread
            spread_mean
            spread_std
            z_spread
    """

    # Align on timestamp
    df = pd.DataFrame(index=df_a.index.intersection(df_b.index))

    # 20-period return (can represent 20-day if daily data,
    # or 20-minute if intraday — depends on input frequency)
    df["ret_a"] = np.log(df_a["close"] / df_a["close"].shift(lookback_ret))
    df["ret_b"] = np.log(df_b["close"] / df_b["close"].shift(lookback_ret))

    # Spread between assets
    df["spread"] = df["ret_a"] - df["ret_b"]

    # Rolling mean and std of spread
    df["spread_mean"] = df["spread"].rolling(spread_window).mean()
    df["spread_std"] = df["spread"].rolling(spread_window).std()

    # Z-score of spread
    df["z_spread"] = (
        df["spread"] - df["spread_mean"]
    ) / df["spread_std"]

    return df
# ====== END NEW ======