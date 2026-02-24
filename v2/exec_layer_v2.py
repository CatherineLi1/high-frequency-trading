# exec_layer_v2.py
# Live Execution Layer (Test Version - Lower Threshold)

from dataclasses import dataclass
from typing import Dict, Any
import pandas as pd
import numpy as np
from ib_insync import MarketOrder


# ==========================================================
# 1️⃣ Strategy Parameters
# ==========================================================

@dataclass
class StrategyParams:
    z_entry: float = 0.3       # ↓ 降低门槛
    z_exit: float = 0.05       # ↓ 更小退出区间
    max_hold_min: int = 120
    base_size: int = 50        # 小仓位测试
    high_vol_multiplier: float = 1.0


@dataclass
class ExecParams:
    cooldown_min: int = 1


# ==========================================================
# 2️⃣ State
# ==========================================================

def init_state():
    return {
        "in_trade": False,
        "side": None,
        "entry_time": None,
        "entry_beta": None,
    }


# ==========================================================
# 3️⃣ Decision Logic
# ==========================================================

def decide_portfolio_target(
    features: pd.DataFrame,
    state: Dict[str, Any],
    strategy_params: StrategyParams,
    exec_params: ExecParams,
    now_ts: pd.Timestamp,
) -> Dict[str, int]:

    if features.empty:
        return {}

    p = strategy_params
    last_row = features.iloc[-1]

    z = float(last_row["z_spread"]) if not pd.isna(last_row["z_spread"]) else np.nan

    print("Current z:", z)
    print("Entry threshold:", p.z_entry)
    print("In trade:", state["in_trade"])

    if pd.isna(z):
        return {}

    # ================= ENTRY =================
    if not state["in_trade"]:

        if z > p.z_entry:
            state["in_trade"] = True
            state["side"] = "SHORT_A_LONG_B"
            state["entry_time"] = now_ts
            print(">>> ENTER SHORT A / LONG B")

            return {"A": -p.base_size, "B": p.base_size}

        elif z < -p.z_entry:
            state["in_trade"] = True
            state["side"] = "LONG_A_SHORT_B"
            state["entry_time"] = now_ts
            print(">>> ENTER LONG A / SHORT B")

            return {"A": p.base_size, "B": -p.base_size}

        return {}

    # ================= EXIT =================
    else:

        hold_minutes = (now_ts - state["entry_time"]).total_seconds() / 60

        if abs(z) < p.z_exit or hold_minutes > p.max_hold_min:
            print(">>> EXIT TRADE")
            state["in_trade"] = False
            state["side"] = None
            state["entry_time"] = None
            return {"A": 0, "B": 0}

        return {}


# ==========================================================
# 4️⃣ Executor Class
# ==========================================================

class Executor:

    def __init__(self, ib, exec_params: ExecParams):
        self.ib = ib
        self.exec_params = exec_params
        self.positions = {}   # 当前持仓记录

    def get_position(self, symbol):

        positions = self.ib.positions()

        for p in positions:
            if p.contract.symbol == symbol:
                return p.position

        return 0

    def trade_to_target(self, symbol, contract, target_position, reason):

        current_position = self.get_position(symbol)

        delta = target_position - current_position

        if delta == 0:
            return

        if delta > 0:
            action = "BUY"
        else:
            action = "SELL"

        quantity = abs(delta)

        print(f"Sending order: {action} {quantity} {symbol} | Reason: {reason}")

        order = MarketOrder(action, quantity)

        trade = self.ib.placeOrder(contract, order)

        self.ib.sleep(1)

        print(f"Order submitted for {symbol}.")