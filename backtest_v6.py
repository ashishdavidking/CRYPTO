"""
Backtesting Engine for alpha_bot_v6_institutional
Timeframe: 4-Hour | Strategy: Fibonacci Reversal & Breakthrough with Trailing SL and Chop Volatility Guard
"""

import os
import json
import time
import hmac
import hashlib
import requests
from datetime import datetime, timezone
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

# Config
SYMBOL = "BTCUSD"
TIMEFRAME_SECS = 14400  # 4 hours
ATR_PERIOD = 10
ST_FACTOR = 4.5
CONFIRM_BARS = 3
DOJI_FACTOR = 0.1
MAX_STATE_AGE_BARS = 30
CHOP_ATR_RATIO_MIN = 0.85

# Capital & Sizing config
CAPITAL_USD = 1200.0
RISK_PER_TRADE_PCT = 0.02
LEVERAGE = 15
MIN_STOP_DISTANCE = 150.0  # Wider stops for 4h
CONTRACT_VALUE = 0.001

API_KEY = "2Osa4t9SlClNFROV2ydx0Jmke1P0J0"
API_SECRET = "fZyJp5LzrIJEx2Sa47j2ginlBLgKZ9U1rF6M6BJ6VdFeBaL48fmHBjKC4xr7"
BASE_URL = "https://api.india.delta.exchange"

console = Console()

# ═══════════════════════════════════════════════════════════════
#  API SIGNING
# ═══════════════════════════════════════════════════════════════
def _generate_signature(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

def _get_headers(method: str, path: str, query_string: str = "") -> dict:
    ts = str(int(time.time()))
    sig_data = method + ts + path + query_string
    return {
        "api-key": API_KEY,
        "timestamp": ts,
        "signature": _generate_signature(API_SECRET, sig_data),
        "User-Agent": "python-alpha-backtest-v6",
        "Content-Type": "application/json",
    }

def api_get(path: str, params: dict | None = None) -> dict | None:
    try:
        query_string = ""
        if params:
            query_string = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        response = requests.get(
            BASE_URL + path, params=params,
            headers=_get_headers("GET", path, query_string), timeout=(5, 15)
        )
        if response.status_code == 200:
            return response.json()
    except Exception as exc:
        pass
    return None

# ═══════════════════════════════════════════════════════════════
#  DATA FETCHING (DEDUPLICATED & CHUNKED)
# ═══════════════════════════════════════════════════════════════
def fetch_historical_candles_chunked_4h(days: int = 180) -> list:
    cache_file = f"btc_4h_hist_{days}d.json"
    if os.path.exists(cache_file):
        console.print(f"[green]Loading historical data from cache file: {cache_file}[/green]")
        with open(cache_file, "r") as f:
            return json.load(f)

    console.print(f"[cyan]Fetching {days} days of historical 4h candles in chunks...[/cyan]")
    end_time = int(time.time())
    start_limit = end_time - (days * 24 * 60 * 60)
    
    all_candles = []
    current_end = end_time
    
    # 500 candles per chunk = 500 * 4 hours = 2000 hours = 83.3 days
    chunk_seconds = 500 * TIMEFRAME_SECS
    
    while current_end > start_limit:
        current_start = max(start_limit, current_end - chunk_seconds)
        params = {
            "symbol": SYMBOL,
            "resolution": "4h",
            "start": current_start,
            "end": current_end
        }
        
        data = api_get("/v2/history/candles", params)
        if not data or not data.get("success"):
            console.print(f"[red]Failed to fetch chunk: start={current_start}, end={current_end}. Retrying in 2s...[/red]")
            time.sleep(2)
            continue
            
        candles = data.get("result", [])
        if not candles:
            break
            
        chunk_parsed = []
        for c in candles:
            ts_sec = int(c.get("time", 0))
            if ts_sec <= 0:
                continue
            bar_ts = int((ts_sec // TIMEFRAME_SECS) * TIMEFRAME_SECS)
            chunk_parsed.append({
                "timestamp": datetime.fromtimestamp(bar_ts, tz=timezone.utc).isoformat(),
                "open": float(c.get("open", 0)),
                "high": float(c.get("high", 0)),
                "low": float(c.get("low", 0)),
                "close": float(c.get("close", 0)),
                "volume": float(c.get("volume", 0)),
            })
            
        all_candles.extend(chunk_parsed)
        console.print(f"  Fetched {len(chunk_parsed)} candles up to {datetime.fromtimestamp(current_end, tz=timezone.utc)}")
        current_end = current_start
        time.sleep(0.5)
        
    # Sort and deduplicate
    all_candles = sorted(all_candles, key=lambda c: c["timestamp"])
    deduped = list({c["timestamp"]: c for c in all_candles}.values())
    
    # Save cache
    with open(cache_file, "w") as f:
        json.dump(deduped, f, indent=2)
        
    console.print(f"[green]Successfully saved {len(deduped)} candles to cache.[/green]")
    return deduped

# ═══════════════════════════════════════════════════════════════
#  CALCULATORS
# ═══════════════════════════════════════════════════════════════
def calc_supertrend(highs, lows, closes, period=ATR_PERIOD, factor=ST_FACTOR):
    n = len(closes)
    if n < period + 2:
        return []

    results = []
    prev_ub = prev_lb = prev_st = None
    prev_dir = None
    rma_atr = None

    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        if i < period:
            rma_atr = (tr if rma_atr is None else rma_atr + tr)
            if i == period - 1:
                rma_atr /= period
            continue
        rma_atr = tr if rma_atr is None else rma_atr * (period - 1) / period + tr / period

        hl2 = (highs[i] + lows[i]) / 2
        raw_ub = hl2 + factor * rma_atr
        raw_lb = hl2 - factor * rma_atr

        if prev_ub is None:
            final_ub, final_lb = raw_ub, raw_lb
        else:
            final_ub = raw_ub if (raw_ub < prev_ub or closes[i-1] > prev_ub) else prev_ub
            final_lb = raw_lb if (raw_lb > prev_lb or closes[i-1] < prev_lb) else prev_lb

        if prev_dir is None:
            direction = -1 if closes[i] > final_ub else 1
        elif prev_dir == -1:
            direction = 1 if closes[i] < final_lb else -1
        else:
            direction = -1 if closes[i] > final_ub else 1

        st = final_lb if direction == -1 else final_ub
        results.append((st, direction))
        prev_ub, prev_lb, prev_st, prev_dir = final_ub, final_lb, st, direction

    return results

def calc_fib_prices(phase_st: float, phase_hh: float, phase_ll: float, is_bull: bool) -> dict:
    extreme = phase_hh if is_bull else phase_ll
    span = extreme - phase_st
    return {
        "-0.1": phase_st - 0.1 * span,
        "0": phase_st,
        "0.2": phase_st + 0.20 * span,
        "0.35": phase_st + 0.35 * span,
        "0.5": phase_st + 0.50 * span,
        "1": phase_st + 1.00 * span,
        "1.5": phase_st + 1.50 * span,
        "2": phase_st + 2.00 * span,
    }

def calc_lot_size(capital_usd, risk_pct, entry_price, sl_price, leverage):
    stop_dist = abs(entry_price - sl_price)
    if stop_dist < MIN_STOP_DISTANCE:
        return 0, 0.0
    risk_usd = capital_usd * risk_pct
    risk_per_lot = stop_dist * CONTRACT_VALUE
    lots_by_risk = int(risk_usd / risk_per_lot)
    max_lots = int((capital_usd * leverage) / (entry_price * CONTRACT_VALUE))
    lots = max(1, min(lots_by_risk, max_lots))
    return lots, lots * risk_per_lot

def calc_chop_status(highs, lows, closes, period=10, sma_period=30) -> tuple[float, bool]:
    n = len(closes)
    if n < period + sma_period:
        return 1.0, False

    trs = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        trs.append(tr)

    atrs = []
    rma_atr = None
    for i in range(len(trs)):
        tr = trs[i]
        if i < period - 1:
            rma_atr = (tr if rma_atr is None else rma_atr + tr)
            if i == period - 2:
                rma_atr /= period
            continue
        rma_atr = tr if rma_atr is None else rma_atr * (period - 1) / period + tr / period
        atrs.append(rma_atr)

    if len(atrs) < sma_period:
        return 1.0, False

    current_atr = atrs[-1]
    atr_sma = sum(atrs[-sma_period:]) / sma_period

    ratio = current_atr / atr_sma if atr_sma > 0 else 1.0
    is_chop = ratio < CHOP_ATR_RATIO_MIN
    return ratio, is_chop

# ═══════════════════════════════════════════════════════════════
#  BACKTEST SIMULATION ENGINE (v6 with Chop & Trailing SL)
# ═══════════════════════════════════════════════════════════════
def run_backtest(candles: list):
    capital = CAPITAL_USD
    peak_capital = CAPITAL_USD
    drawdown_pct = 0.0
    
    # State tracking
    phase = {
        "is_bull": None, "phase_st": None, "phase_hh": None, "phase_ll": None,
        "measuring": False,
        "state02": 0, "state035": 0, "state05": 0,
        "state02_age": 0, "state035_age": 0, "state05_age": 0,
        "fib02": None, "fib035": None, "fib05": None,
        "const_count": 0, "prev_dir": None,
    }
    
    position = None
    pending = None
    trades_log = []
    
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]
    
    # Calculate Supertrend
    st_results = calc_supertrend(highs, lows, closes)
    
    for i in range(1, len(candles)):
        st_idx = i - (ATR_PERIOD + 1)
        if st_idx < 0 or st_idx >= len(st_results):
            continue
            
        c_curr = candles[i]
        c_prev = candles[i-1]
        
        # Get Supertrend value
        latest_st, latest_dir = st_results[st_idx]
        
        # 1. Manage Active Position (Trailing SL & SL/TP check)
        if position:
            side = position["side"]
            sl = position["sl_price"]
            tp = position["tp_price"]
            
            # Trailing Stop: Lock profits behind Supertrend
            if side == "buy":
                position["sl_price"] = max(sl, latest_st)
            else:
                position["sl_price"] = min(sl, latest_st)
            sl = position["sl_price"]
            
            p_high = c_curr["high"]
            p_low = c_curr["low"]
            
            hit_sl = (side == "buy" and p_low <= sl) or (side == "sell" and p_high >= sl)
            hit_tp = (side == "buy" and p_high >= tp) or (side == "sell" and p_low <= tp)
            
            if hit_sl or hit_tp:
                exit_reason = "SL" if hit_sl else "TP"
                exit_price = sl if hit_sl else tp
                
                pnl_usd = ((exit_price - position["entry_price"]) if side == "buy"
                           else (position["entry_price"] - exit_price)) * position["lots"] * CONTRACT_VALUE
                           
                capital += pnl_usd
                peak_capital = max(peak_capital, capital)
                
                trades_log.append({
                    "type": "trade",
                    "entry_time": position["entry_time"],
                    "exit_time": c_curr["timestamp"],
                    "side": side,
                    "fib_level": position["fib_level"],
                    "entry_price": position["entry_price"],
                    "exit_price": exit_price,
                    "pnl_usd": pnl_usd,
                    "exit_reason": exit_reason,
                    "capital": capital
                })
                position = None
                
        # 2. Check Pending Activation
        elif pending:
            side = pending["side"]
            pp = pending["pending_price"]
            p_high = c_curr["high"]
            p_low = c_curr["low"]
            
            triggered = (side == "buy" and p_high >= pp) or (side == "sell" and p_low <= pp)
            if triggered:
                position = {
                    "side": side,
                    "entry_price": pp,
                    "sl_price": pending["sl_price"],
                    "tp_price": pending["tp_price"],
                    "fib_level": pending["fib_level"],
                    "lots": pending["lots"],
                    "entry_time": c_curr["timestamp"]
                }
                pending = None
                
                # Check immediate stop/TP
                sl = position["sl_price"]
                tp = position["tp_price"]
                hit_sl = (side == "buy" and p_low <= sl) or (side == "sell" and p_high >= sl)
                hit_tp = (side == "buy" and p_high >= tp) or (side == "sell" and p_low <= tp)
                
                if hit_sl or hit_tp:
                    exit_reason = "SL" if hit_sl else "TP"
                    exit_price = sl if hit_sl else tp
                    pnl_usd = ((exit_price - position["entry_price"]) if side == "buy"
                               else (position["entry_price"] - exit_price)) * position["lots"] * CONTRACT_VALUE
                               
                    capital += pnl_usd
                    peak_capital = max(peak_capital, capital)
                    
                    trades_log.append({
                        "type": "trade",
                        "entry_time": position["entry_time"],
                        "exit_time": c_curr["timestamp"],
                        "side": side,
                        "fib_level": position["fib_level"],
                        "entry_price": position["entry_price"],
                        "exit_price": exit_price,
                        "pnl_usd": pnl_usd,
                        "exit_reason": exit_reason,
                        "capital": capital
                    })
                    position = None
            
        # 3. Supertrend & Phase Logic updates
        _prev_st, prev_dir_v = st_results[st_idx - 1] if st_idx > 0 else (latest_st, latest_dir)
        is_bull = (latest_dir == -1)
        
        st_changed = (latest_st != _prev_st)
        if st_changed:
            phase["const_count"] = 0
        else:
            phase["const_count"] += 1
            
        dir_flip = (phase["prev_dir"] is not None and latest_dir != phase["prev_dir"])
        just_confirmed = (phase["const_count"] == CONFIRM_BARS)
        
        if dir_flip:
            phase.update({
                "measuring": False, "phase_st": None, "phase_hh": None, "phase_ll": None,
                "state02": 0, "state035": 0, "state05": 0,
                "state02_age": 0, "state035_age": 0, "state05_age": 0,
                "fib02": None, "fib035": None, "fib05": None,
            })
            if pending:
                pending = None
                
        if just_confirmed and i >= CONFIRM_BARS + 2:
            phase["measuring"] = True
            phase["is_bull"] = is_bull
            phase["phase_st"] = latest_st
            phase["phase_hh"] = max(highs[i - CONFIRM_BARS : i + 1])
            phase["phase_ll"] = min(lows[i - CONFIRM_BARS : i + 1])
            phase["state02"] = phase["state035"] = phase["state05"] = 0
            phase["state02_age"] = phase["state035_age"] = phase["state05_age"] = 0
            
            fib_prices = calc_fib_prices(phase["phase_st"], phase["phase_hh"], phase["phase_ll"], is_bull)
            phase["fib02"] = fib_prices["0.2"]
            phase["fib035"] = fib_prices["0.35"]
            phase["fib05"] = fib_prices["0.5"]
            
        phase["prev_dir"] = latest_dir
        
        # 4. Check for Trade Signals
        if (phase["measuring"] and not dir_flip and not just_confirmed
                and not position and not pending):
            
            # Volatility Chop status (historical checks)
            slice_highs = highs[:i]
            slice_lows = lows[:i]
            slice_closes = closes[:i]
            _, chop_active = calc_chop_status(slice_highs, slice_lows, slice_closes)
            
            if not chop_active:
                c_open = c_prev["open"]
                c_close = c_prev["close"]
                c_range = c_prev["high"] - c_prev["low"]
                c_body = abs(c_close - c_open)
                is_doji = c_range > 0 and (c_body / c_range) < DOJI_FACTOR
                
                def check_break(fp):
                    if fp is None: return False
                    return (c_open > fp and c_close < fp) if phase["is_bull"] else (c_open < fp and c_close > fp)
                    
                def check_reversal(fp):
                    if fp is None or is_doji: return False
                    return (c_open < fp and c_close > fp) if phase["is_bull"] else (c_open > fp and c_close < fp)
                    
                for sk, ak in [("state02","state02_age"),("state035","state035_age"),("state05","state05_age")]:
                    if phase[sk] == 1:
                        phase[ak] += 1
                        if phase[ak] > MAX_STATE_AGE_BARS:
                            phase[sk] = 0; phase[ak] = 0
                            
                reversal_close = c_close
                for lvl, fk, sk, ak in [
                    ("0.2", "fib02", "state02", "state02_age"),
                    ("0.35", "fib035", "state035", "state035_age"),
                    ("0.5", "fib05", "state05", "state05_age"),
                ]:
                    fp = phase[fk]
                    if phase[sk] == 0 and check_break(fp):
                        phase[sk] = 1; phase[ak] = 0
                    elif phase[sk] == 1 and check_reversal(fp):
                        phase[sk] = 2
                        
                        fib_prices = calc_fib_prices(phase["phase_st"], phase["phase_hh"], phase["phase_ll"], phase["is_bull"])
                        SL_TP = {
                            "0.2": {"sl": "-0.1", "tp": "2"},
                            "0.35": {"sl": "0.2", "tp": "2"},
                            "0.5": {"sl": "0.35", "tp": "2"},
                        }
                        mapping = SL_TP[lvl]
                        sl_price = fib_prices[mapping["sl"]]
                        tp_price = fib_prices[mapping["tp"]]
                        side = "buy" if phase["is_bull"] else "sell"
                        
                        lots, risk_usd = calc_lot_size(capital, RISK_PER_TRADE_PCT, reversal_close, sl_price, LEVERAGE)
                        if lots > 0:
                            pending = {
                                "side": side,
                                "fib_level": lvl,
                                "pending_price": reversal_close,
                                "sl_price": sl_price,
                                "tp_price": tp_price,
                                "lots": lots,
                                "risk_usd": risk_usd
                            }
                            break
                            
        if peak_capital > 0:
            dd = (peak_capital - capital) / peak_capital * 100
            drawdown_pct = max(drawdown_pct, dd)

    return trades_log, capital, drawdown_pct

# ═══════════════════════════════════════════════════════════════
#  MAIN EXECUTION
# ═══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    candles = fetch_historical_candles_chunked_4h(days=180)
    if not candles:
        console.print("[red]No historical candles available for backtest.[/red]")
        exit(1)
        
    trades, final_cap, max_dd = run_backtest(candles)
    
    total_trades = len(trades)
    winning_trades = sum(1 for t in trades if t["pnl_usd"] >= 0)
    losing_trades = total_trades - winning_trades
    win_rate = (winning_trades / total_trades * 100) if total_trades > 0 else 0.0
    
    total_win_usd = sum(t["pnl_usd"] for t in trades if t["pnl_usd"] >= 0)
    total_loss_usd = sum(t["pnl_usd"] for t in trades if t["pnl_usd"] < 0)
    profit_factor = abs(total_win_usd / total_loss_usd) if total_loss_usd < 0 else (total_win_usd if total_win_usd > 0 else 1.0)
    total_pnl = final_cap - CAPITAL_USD
    
    trade_table = Table(title="Historical Executed Trades (Last 180 Days - 4h)", header_style="bold cyan")
    trade_table.add_column("Entry Time", style="dim")
    trade_table.add_column("Exit Time", style="dim")
    trade_table.add_column("Side")
    trade_table.add_column("Fib")
    trade_table.add_column("Entry Px", justify="right")
    trade_table.add_column("Exit Px", justify="right")
    trade_table.add_column("PnL ($)", justify="right")
    trade_table.add_column("Reason")
    trade_table.add_column("Capital ($)", justify="right")
    
    for t in trades:
        pnl_col = "green" if t["pnl_usd"] >= 0 else "red"
        trade_table.add_row(
            t["entry_time"][:16].replace("T", " "),
            t["exit_time"][:16].replace("T", " "),
            t["side"].upper(),
            t["fib_level"],
            f"{t['entry_price']:,.2f}",
            f"{t['exit_price']:,.2f}",
            f"[{pnl_col}]${t['pnl_usd']:+,.2f}[/{pnl_col}]",
            t["exit_reason"],
            f"${t['capital']:,.2f}"
        )
        
    metric_table = Table(show_header=False, box=None, padding=(0, 2))
    metric_table.add_column("K", style="cyan")
    metric_table.add_column("V", style="white")
    metric_table.add_row("Initial Capital", f"${CAPITAL_USD:,.2f}")
    metric_table.add_row("Final Capital", f"${final_cap:,.2f}")
    pnl_col = "green" if total_pnl >= 0 else "red"
    metric_table.add_row("Net Profit (USD)", f"[{pnl_col}]${total_pnl:+,.2f} ({total_pnl/CAPITAL_USD*100:+.2f}%)[/{pnl_col}]")
    metric_table.add_row("Net Profit (INR)", f"[{pnl_col}]INR {total_pnl/0.012:+,.2f}[/{pnl_col}]")
    metric_table.add_row("Total Trades", f"{total_trades} (Win: {winning_trades} | Loss: {losing_trades})")
    metric_table.add_row("Win Rate", f"{win_rate:.1f}%")
    metric_table.add_row("Profit Factor", f"{profit_factor:.2f}")
    metric_table.add_row("Max Peak Drawdown", f"[red]{max_dd:.2f}%[/red]")
    
    console.print(trade_table)
    console.print("\n")
    console.print(Panel(metric_table, title="[bold yellow]BACKTEST PERFORMANCE SUMMARY (4H V6 BOT)[/bold yellow]", border_style="yellow"))
