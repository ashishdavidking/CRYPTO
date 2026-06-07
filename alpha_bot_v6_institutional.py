"""
Alpha v6 — Institutional BTCUSD Paper Trading Bot
Delta Exchange | 4-Hour Timeframe | Whale Wall Overlay | Chop Volatility Guard
Version: 6.0
"""

import sqlite3
import time
import requests
import hashlib
import hmac
import json
import os
import logging
from datetime import datetime, timezone, timedelta
from collections import deque
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich.text import Text

# ═══════════════════════════════════════════════════════════════
#  LOGGING
# ═══════════════════════════════════════════════════════════════
logging.basicConfig(
    filename="alpha_v6_errors.log",
    level=logging.ERROR,
    format="%(asctime)s %(levelname)s %(message)s",
)

def log_err(msg, exc=None):
    if exc:
        logging.error(msg, exc_info=exc)
    else:
        logging.error(msg)

# ═══════════════════════════════════════════════════════════════
#  CONFIGURATION
# ═══════════════════════════════════════════════════════════════
API_KEY        = "ENTERyourapikey"
API_SECRET     = "enteryour_API_SECRET "
BASE_URL       = "https://api.india.delta.exchange"
if not API_KEY or not API_SECRET:
    raise RuntimeError("Set DELTA_API_KEY and DELTA_API_SECRET first.")

# Capital & Risk
CAPITAL_INR        = 100_000.0
INR_TO_USD         = 0.012
CAPITAL_USD        = CAPITAL_INR * INR_TO_USD
RISK_PER_TRADE_PCT = 0.02
MAX_DRAWDOWN_PCT   = 0.05
LEVERAGE           = 15
MIN_STOP_DISTANCE  = 150.0      # USD — wider for 4h macro moves

# Contract
CONTRACT_VALUE = 0.001
SYMBOL         = "BTCUSD"

# ── 4-HOUR TIMEFRAME ─────────────────────────────────────────
TIMEFRAME_SECS     = 14400      # 4 hours
LOOP_SLEEP         = 30         # loop tick interval (seconds)
OB_SAVE_INTERVAL   = 60         # save order book/options every N seconds
HIST_LOOKBACK_MINS = 48000      # pre-load 200× 4h bars (33.3 days)

# Supertrend (TV Alpha v3 parameters)
ATR_PERIOD         = 10
ST_FACTOR          = 4.5
CONFIRM_BARS       = 3
DOJI_FACTOR        = 0.1
MAX_STATE_AGE_BARS = 30         # Max state lifetime (5 days on 4h)

# Chop Volatility Filter
CHOP_ATR_RATIO_MIN = 0.85

# Whale Wall Filters
WHALE_OPTION_OI_MIN     = 5000  # Minimum option contracts near Fib strike
WHALE_FUTURES_SIZE_MIN  = 100000 # Minimum limit orders size in orderbook near Fib

# Files
DB_FILE    = "alpha_btcusd_v6.db"
STATE_FILE = "runtime_state_v6.json"

console = Console()
_cached_options_tickers = []

# ═══════════════════════════════════════════════════════════════
#  PERSISTENT DB CONNECTION
# ═══════════════════════════════════════════════════════════════
_db_conn: sqlite3.Connection | None = None

def get_db() -> sqlite3.Connection:
    global _db_conn
    if _db_conn is None:
        _db_conn = sqlite3.connect(DB_FILE, check_same_thread=False)
        _db_conn.execute("PRAGMA journal_mode=WAL")
        _db_conn.execute("PRAGMA synchronous=NORMAL")
    return _db_conn

# ═══════════════════════════════════════════════════════════════
#  DATABASE SETUP
# ═══════════════════════════════════════════════════════════════
def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS market_data (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            mark_price      REAL,
            spot_price      REAL,
            best_bid        REAL,
            best_ask        REAL,
            spread          REAL,
            volume          REAL,
            turnover_usd    REAL,
            open_interest   REAL,
            oi_value_usd    REAL,
            funding_rate    REAL,
            high_24h        REAL,
            low_24h         REAL,
            ltp_change_24h  REAL
        );
        CREATE TABLE IF NOT EXISTS order_book (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            side      TEXT,
            price     REAL,
            size      INTEGER,
            depth     REAL,
            level     INTEGER
        );
        CREATE TABLE IF NOT EXISTS market_breadth (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            oi_change       REAL,
            oi_change_pct   REAL,
            buy_volume      REAL,
            sell_volume     REAL,
            volume_delta    REAL,
            bid_depth_total REAL,
            ask_depth_total REAL,
            ls_ratio        REAL
        );
        CREATE TABLE IF NOT EXISTS candles (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            open      REAL,
            high      REAL,
            low       REAL,
            close     REAL,
            volume    REAL
        );
        CREATE TABLE IF NOT EXISTS trades (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            side            TEXT,
            pending_price   REAL,
            entry_price     REAL,
            sl_price        REAL,
            tp_price        REAL,
            fib_level       TEXT,
            lots            INTEGER,
            size_btc        REAL,
            notional_usd    REAL,
            risk_usd        REAL,
            status          TEXT,
            exit_price      REAL,
            exit_time       TEXT,
            pnl_usd         REAL,
            pnl_inr         REAL,
            exit_reason     TEXT,
            oi_at_entry     REAL,
            ls_at_entry     REAL,
            oi_phase        TEXT,
            option_oi       REAL,
            futures_depth   REAL
        );
        CREATE TABLE IF NOT EXISTS account_state (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp      TEXT,
            capital_usd    REAL,
            capital_inr    REAL,
            peak_capital   REAL,
            drawdown_pct   REAL,
            total_trades   INTEGER,
            winning_trades INTEGER,
            losing_trades  INTEGER,
            total_pnl_usd  REAL
        );
        CREATE TABLE IF NOT EXISTS options_metrics (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            pcr_oi          REAL,
            total_call_oi   REAL,
            total_put_oi    REAL,
            avg_call_delta  REAL,
            avg_put_delta   REAL,
            avg_call_theta  REAL,
            avg_put_theta   REAL,
            avg_call_gamma  REAL,
            avg_put_gamma   REAL,
            avg_call_vega   REAL,
            avg_put_vega    REAL
        );
        CREATE TABLE IF NOT EXISTS chop_status (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT,
            atr_ratio       REAL,
            is_chop         INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_market_data_ts  ON market_data(timestamp);
        CREATE INDEX IF NOT EXISTS idx_candles_ts      ON candles(timestamp);
        CREATE INDEX IF NOT EXISTS idx_trades_ts       ON trades(timestamp);
        CREATE INDEX IF NOT EXISTS idx_account_ts      ON account_state(timestamp);
        CREATE INDEX IF NOT EXISTS idx_options_ts      ON options_metrics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_chop_ts         ON chop_status(timestamp);
    """)
    db.commit()

# ═══════════════════════════════════════════════════════════════
#  STATE PERSISTENCE
# ═══════════════════════════════════════════════════════════════
def save_state(account: dict, position: dict | None, pending: dict | None, phase: dict | None):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({
                "account": account,
                "position": position,
                "pending": pending,
                "phase": phase
            }, f, indent=2)
    except Exception as exc:
        log_err("save_state failed", exc)

def load_state() -> tuple:
    if not os.path.exists(STATE_FILE):
        return None, None, None, None
    try:
        with open(STATE_FILE) as f:
            data = json.load(f)
        return (
            data.get("account"),
            data.get("position"),
            data.get("pending"),
            data.get("phase")
        )
    except Exception as exc:
        log_err("load_state failed", exc)
        return None, None, None, None

# ═══════════════════════════════════════════════════════════════
#  API HELPERS
# ═══════════════════════════════════════════════════════════════
def _generate_signature(secret: str, message: str) -> str:
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()

def _get_headers(method: str, path: str, query_string: str = "", payload: str = "") -> dict:
    ts        = str(int(time.time()))
    sig_data  = method + ts + path + query_string + payload
    return {
        "api-key":      API_KEY,
        "timestamp":    ts,
        "signature":    _generate_signature(API_SECRET, sig_data),
        "User-Agent":   "python-alpha-bot-v6",
        "Content-Type": "application/json",
    }

def api_get(path: str, params: dict | None = None) -> dict | None:
    try:
        query_string = ""
        if params:
            query_string = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        response = requests.get(
            BASE_URL + path, params=params,
            headers=_get_headers("GET", path, query_string), timeout=(3, 10)
        )
        if response.status_code == 200:
            return response.json()
        log_err(f"api_get {path} HTTP {response.status_code}")
    except Exception as exc:
        log_err(f"api_get {path}", exc)
    return None

# ═══════════════════════════════════════════════════════════════
#  MARKET DATA FETCHERS
# ═══════════════════════════════════════════════════════════════
def fetch_ticker() -> dict | None:
    data = api_get(f"/v2/tickers/{SYMBOL}")
    return data["result"] if data and data.get("success") else None

def fetch_orderbook(depth: int = 10) -> dict | None:
    data = api_get(f"/v2/l2orderbook/{SYMBOL}", {"depth": depth})
    return data["result"] if data and data.get("success") else None

# ═══════════════════════════════════════════════════════════════
#  HISTORICAL CANDLES (Official history endpoint)
# ═══════════════════════════════════════════════════════════════
def fetch_historical_candles(minutes: int = HIST_LOOKBACK_MINS) -> list:
    try:
        console.print(f"[cyan]Fetching historical data ({minutes} min lookback)...[/cyan]")
        end_ts = int(time.time())
        start_ts = end_ts - (minutes * 60)
        params = {
            "symbol": SYMBOL,
            "resolution": "4h",
            "start": start_ts,
            "end": end_ts
        }
        data = api_get("/v2/history/candles", params)
        if not data or not data.get("success"):
            console.print("[yellow]Candle history unavailable — starting fresh[/yellow]")
            return []

        candles_data = data.get("result", [])
        result = []
        for c in candles_data:
            ts_sec = int(c.get("time", 0))
            if ts_sec <= 0:
                continue
            bar_ts = int((ts_sec // TIMEFRAME_SECS) * TIMEFRAME_SECS)
            result.append({
                "timestamp": datetime.fromtimestamp(bar_ts, tz=timezone.utc).isoformat(),
                "open": float(c.get("open", 0)),
                "high": float(c.get("high", 0)),
                "low": float(c.get("low", 0)),
                "close": float(c.get("close", 0)),
                "volume": float(c.get("volume", 0)),
            })

        result = sorted(result, key=lambda c: c["timestamp"])
        result = list({c["timestamp"]: c for c in result}.values())
        console.print(f"[green]Loaded {len(result)} historical 4h candles[/green]")
        return result

    except Exception as exc:
        log_err("fetch_historical_candles", exc)
        return []

# ═══════════════════════════════════════════════════════════════
#  CANDLE BUILDER (4h)
# ═══════════════════════════════════════════════════════════════
class CandleBuilder:
    def __init__(self):
        self.candles:   deque           = deque(maxlen=200)
        self.current:   dict | None     = None
        self.bar_start: int  | None     = None

    def add_historical(self, candles_list: list):
        if not candles_list:
            return
        seen_ts = {c["timestamp"] for c in self.candles}
        for c in candles_list[:-1]:
            if c["timestamp"] not in seen_ts:
                self.candles.append(dict(c))
                self._save_candle(c)
                seen_ts.add(c["timestamp"])
        
        last_candle = candles_list[-1]
        last_ts = datetime.fromisoformat(last_candle["timestamp"]).timestamp()
        self.bar_start = int((last_ts // TIMEFRAME_SECS) * TIMEFRAME_SECS)
        self.current = dict(last_candle)

    def update(self, price: float, volume: float, ts: int) -> bool:
        bar_ts = (ts // TIMEFRAME_SECS) * TIMEFRAME_SECS
        if self.bar_start is None or bar_ts != self.bar_start:
            closed = False
            if self.current is not None:
                self.candles.append(dict(self.current))
                self._save_candle(self.current)
                closed = True
            self.bar_start = bar_ts
            self.current   = {
                "timestamp": datetime.fromtimestamp(bar_ts, tz=timezone.utc).isoformat(),
                "open": price, "high": price, "low": price, "close": price, "volume": volume,
            }
            return closed
        else:
            c = self.current
            c["high"]   = max(c["high"], price)
            c["low"]    = min(c["low"],  price)
            c["close"]  = price
            c["volume"] += volume
            return False

    def _save_candle(self, c: dict):
        try:
            db = get_db()
            db.execute(
                "INSERT INTO candles (timestamp, open, high, low, close, volume) "
                "SELECT ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM candles WHERE timestamp = ?)",
                (c["timestamp"], c["open"], c["high"], c["low"], c["close"], c["volume"], c["timestamp"]),
            )
            db.commit()
        except Exception as exc:
            log_err("_save_candle", exc)

    def get_closes(self) -> list:
        r = [c["close"] for c in self.candles]
        if self.current: r.append(self.current["close"])
        return r
    def get_highs(self) -> list:
        r = [c["high"]  for c in self.candles]
        if self.current: r.append(self.current["high"])
        return r
    def get_lows(self) -> list:
        r = [c["low"]   for c in self.candles]
        if self.current: r.append(self.current["low"])
        return r
    def count(self) -> int:
        return len(self.candles) + (1 if self.current else 0)
    def last_closed(self) -> dict | None:
        return self.candles[-1] if self.candles else None

# ═══════════════════════════════════════════════════════════════
#  SUPERTREND — RMA ATR (Alpha v3 Pine aligned)
# ═══════════════════════════════════════════════════════════════
def calc_supertrend(highs, lows, closes, period=ATR_PERIOD, factor=ST_FACTOR):
    n = len(closes)
    if n < period + 2:
        return []

    results  = []
    prev_ub  = prev_lb = prev_st = None
    prev_dir = None
    rma_atr  = None

    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i]  - closes[i - 1]),
        )
        if i < period:
            rma_atr = (tr if rma_atr is None else rma_atr + tr)
            if i == period - 1:
                rma_atr /= period
            continue
        rma_atr = tr if rma_atr is None else rma_atr * (period - 1) / period + tr / period

        hl2    = (highs[i] + lows[i]) / 2
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
            direction =  1 if closes[i] < final_lb else -1
        else:
            direction = -1 if closes[i] > final_ub else 1

        st = final_lb if direction == -1 else final_ub
        results.append((st, direction))
        prev_ub, prev_lb, prev_st, prev_dir = final_ub, final_lb, st, direction

    return results

# ═══════════════════════════════════════════════════════════════
#  FIB CALCULATOR
# ═══════════════════════════════════════════════════════════════
def calc_fib_prices(phase_st: float, phase_hh: float, phase_ll: float, is_bull: bool) -> dict:
    extreme = phase_hh if is_bull else phase_ll
    span    = extreme - phase_st
    return {
        "-0.1": phase_st - 0.1 * span,
        "0":    phase_st,
        "0.2":  phase_st + 0.20 * span,
        "0.35": phase_st + 0.35 * span,
        "0.5":  phase_st + 0.50 * span,
        "1":    phase_st + 1.00 * span,
        "1.5":  phase_st + 1.50 * span,
        "2":    phase_st + 2.00 * span,
    }

# ═══════════════════════════════════════════════════════════════
#  POSITION SIZING
# ═══════════════════════════════════════════════════════════════
def calc_lot_size(capital_usd, risk_pct, entry_price, sl_price, leverage):
    stop_dist = abs(entry_price - sl_price)
    if stop_dist < MIN_STOP_DISTANCE:
        return 0, 0.0
    risk_usd     = capital_usd * risk_pct
    risk_per_lot = stop_dist * CONTRACT_VALUE
    lots_by_risk = int(risk_usd / risk_per_lot)
    max_lots     = int((capital_usd * leverage) / (entry_price * CONTRACT_VALUE))
    lots         = max(1, min(lots_by_risk, max_lots))
    return lots, lots * risk_per_lot

# ═══════════════════════════════════════════════════════════════
#  OI FLOW CLASSIFIER
# ═══════════════════════════════════════════════════════════════
def classify_oi_phase(oi_prev: float, oi_curr: float, price_prev: float, price_curr: float) -> str:
    dOI = oi_curr - oi_prev
    dP  = price_curr - price_prev
    threshold_oi    = 2.0
    threshold_price = 30.0
    if dOI >  threshold_oi and dP >  threshold_price: return "LONGS ADDING"
    if dOI >  threshold_oi and dP < -threshold_price: return "SHORTS ADDING"
    if dOI < -threshold_oi and dP < -threshold_price: return "LONG LIQUIDATION"
    if dOI < -threshold_oi and dP >  threshold_price: return "SHORT COVERING"
    return "NEUTRAL"

def oi_allows_trade(phase: str, is_bull: bool) -> bool:
    if is_bull and phase in ("SHORTS ADDING", "LONG LIQUIDATION"):
        return False
    if not is_bull and phase in ("LONGS ADDING",):
        return False
    return True

def market_trend_label(oi_start, oi_now, price_start, price_now) -> str:
    dP  = price_now - price_start
    dOI = oi_now - oi_start
    if dOI < -10 and dP < -200:  return "LONG LIQUIDATION"
    if dOI < -10 and dP >  200:  return "SHORT COVERING"
    if dOI >  10 and dP >  200:  return "LONGS ADDING"
    if dOI >  10 and dP < -200:  return "SHORTS ADDING"
    if dP > 200:   return "BULLISH"
    if dP < -200:  return "BEARISH"
    return "NEUTRAL"

# ═══════════════════════════════════════════════════════════════
#  VOLATILITY CHOP FILTER
# ═══════════════════════════════════════════════════════════════
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

def save_chop_status(ratio: float, is_chop: bool):
    try:
        db = get_db()
        db.execute(
            "INSERT INTO chop_status (timestamp, atr_ratio, is_chop) VALUES (?,?,?)",
            (datetime.now(timezone.utc).isoformat(), ratio, 1 if is_chop else 0)
        )
        db.commit()
    except Exception as exc:
        log_err("save_chop_status failed", exc)

# ═══════════════════════════════════════════════════════════════
#  WHALE WALL FITTING
# ═══════════════════════════════════════════════════════════════
def evaluate_whale_wall(fib_price: float, ob: dict) -> tuple[float, float, bool]:
    global _cached_options_tickers
    
    option_oi = 0.0
    strike_range = 1000.0  # strikes within +/- $1000 of fib_price
    
    for t in _cached_options_tickers:
        try:
            strike = float(t.get("strike_price", 0) or 0)
            if abs(strike - fib_price) <= strike_range:
                oi = float(t.get("oi_contracts", 0) or 0)
                option_oi += oi
        except:
            continue
            
    ob_size = 0.0
    price_range = 200.0
    
    if ob:
        for side in ["buy", "sell"]:
            for level in ob.get(side, []):
                try:
                    price = float(level.get("price", 0) or 0)
                    size = float(level.get("size", 0) or 0)
                    if abs(price - fib_price) <= price_range:
                        ob_size += size
                except:
                    continue
                    
    is_whale_wall = (option_oi >= WHALE_OPTION_OI_MIN) or (ob_size >= WHALE_FUTURES_SIZE_MIN)
    return option_oi, ob_size, is_whale_wall

# ═══════════════════════════════════════════════════════════════
#  DATABASE WRITERS
# ═══════════════════════════════════════════════════════════════
def save_market_data(ticker: dict):
    try:
        q   = ticker.get("quotes", {}) or {}
        bid = float(q.get("best_bid", 0) or 0)
        ask = float(q.get("best_ask", 0) or 0)
        db  = get_db()
        db.execute(
            """INSERT INTO market_data
               (timestamp,mark_price,spot_price,best_bid,best_ask,spread,
                volume,turnover_usd,open_interest,oi_value_usd,funding_rate,
                high_24h,low_24h,ltp_change_24h)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                float(ticker.get("mark_price",    0) or 0),
                float(ticker.get("spot_price",    0) or 0),
                bid, ask, round(ask - bid, 2),
                float(ticker.get("volume",        0) or 0),
                float(ticker.get("turnover_usd",  0) or 0),
                float(ticker.get("oi",            0) or 0),
                float(ticker.get("oi_value_usd",  0) or 0),
                float(ticker.get("funding_rate",  0) or 0),
                float(ticker.get("high",          0) or 0),
                float(ticker.get("low",           0) or 0),
                float(ticker.get("ltp_change_24h",0) or 0),
            ),
        )
        db.commit()
    except Exception as exc:
        log_err("save_market_data", exc)

def save_orderbook(ob: dict):
    try:
        ts = datetime.now(timezone.utc).isoformat()
        db = get_db()
        rows = []
        for lvl, e in enumerate(ob.get("buy",  [])[:10]):
            rows.append((ts, "buy",  float(e["price"]), int(e["size"]), float(e["depth"]), lvl))
        for lvl, e in enumerate(ob.get("sell", [])[:10]):
            rows.append((ts, "sell", float(e["price"]), int(e["size"]), float(e["depth"]), lvl))
        db.executemany(
            "INSERT INTO order_book (timestamp,side,price,size,depth,level) VALUES (?,?,?,?,?,?)", rows)
        db.commit()
    except Exception as exc:
        log_err("save_orderbook", exc)

def save_market_breadth(ob: dict, prev_oi: float, curr_oi: float):
    try:
        bids    = ob.get("buy",  [])
        asks    = ob.get("sell", [])
        bid_tot = sum(float(b["size"]) for b in bids)
        ask_tot = sum(float(a["size"]) for a in asks)
        ls_ratio= round(bid_tot / ask_tot, 4) if ask_tot > 0 else 0
        oi_chg  = curr_oi - prev_oi
        oi_pct  = round((oi_chg / prev_oi) * 100, 4) if prev_oi > 0 else 0
        db = get_db()
        db.execute(
            """INSERT INTO market_breadth
               (timestamp,oi_change,oi_change_pct,buy_volume,sell_volume,
                volume_delta,bid_depth_total,ask_depth_total,ls_ratio)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (datetime.now(timezone.utc).isoformat(), oi_chg, oi_pct,
             bid_tot, ask_tot, round(bid_tot - ask_tot, 2), bid_tot, ask_tot, ls_ratio),
        )
        db.commit()
    except Exception as exc:
        log_err("save_market_breadth", exc)

def fetch_options_metrics_and_cache() -> dict | None:
    global _cached_options_tickers
    try:
        params = {
            "underlying_asset_symbols": "BTC",
            "contract_types": "call_options,put_options"
        }
        data = api_get("/v2/tickers", params)
        if not data or not data.get("success"):
            return None
            
        tickers = data.get("result", [])
        _cached_options_tickers = tickers
        
        total_call_oi = 0.0
        total_put_oi = 0.0
        
        call_deltas = []
        put_deltas = []
        call_thetas = []
        put_thetas = []
        call_gammas = []
        put_gammas = []
        call_vegas = []
        put_vegas = []
        
        for t in tickers:
            contract_type = t.get("contract_type")
            oi = float(t.get("oi_contracts", 0) or 0)
            
            g = t.get("greeks", {}) or {}
            delta = float(g.get("delta", 0) or 0)
            theta = float(g.get("theta", 0) or 0)
            gamma = float(g.get("gamma", 0) or 0)
            vega  = float(g.get("vega", 0) or 0)
            
            if contract_type == "call_options":
                total_call_oi += oi
                if delta != 0: call_deltas.append(delta)
                if theta != 0: call_thetas.append(theta)
                if gamma != 0: call_gammas.append(gamma)
                if vega != 0:  call_vegas.append(vega)
            elif contract_type == "put_options":
                total_put_oi += oi
                if delta != 0: put_deltas.append(delta)
                if theta != 0: put_thetas.append(theta)
                if gamma != 0: put_gammas.append(gamma)
                if vega != 0:  put_vegas.append(vega)
                
        pcr = total_put_oi / total_call_oi if total_call_oi > 0 else 0.0
        
        avg_c_delta = sum(call_deltas) / len(call_deltas) if call_deltas else 0.0
        avg_p_delta = sum(put_deltas) / len(put_deltas) if put_deltas else 0.0
        avg_c_theta = sum(call_thetas) / len(call_thetas) if call_thetas else 0.0
        avg_p_theta = sum(put_thetas) / len(put_thetas) if put_thetas else 0.0
        avg_c_gamma = sum(call_gammas) / len(call_gammas) if call_gammas else 0.0
        avg_p_gamma = sum(put_gammas) / len(put_gammas) if put_gammas else 0.0
        avg_c_vega  = sum(call_vegas) / len(call_vegas) if call_vegas else 0.0
        avg_p_vega  = sum(put_vegas) / len(put_vegas) if put_vegas else 0.0
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pcr_oi": pcr,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "avg_call_delta": avg_c_delta,
            "avg_put_delta": avg_p_delta,
            "avg_call_theta": avg_c_theta,
            "avg_put_theta": avg_p_theta,
            "avg_call_gamma": avg_c_gamma,
            "avg_put_gamma": avg_p_gamma,
            "avg_call_vega": avg_c_vega,
            "avg_put_vega": avg_p_vega,
        }
    except Exception as exc:
        log_err("fetch_options_metrics failed", exc)
        return None

def save_options_metrics(metrics: dict):
    try:
        db = get_db()
        db.execute(
            """INSERT INTO options_metrics
               (timestamp, pcr_oi, total_call_oi, total_put_oi,
                avg_call_delta, avg_put_delta, avg_call_theta, avg_put_theta,
                avg_call_gamma, avg_put_gamma, avg_call_vega, avg_put_vega)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                metrics["timestamp"], metrics["pcr_oi"],
                metrics["total_call_oi"], metrics["total_put_oi"],
                metrics["avg_call_delta"], metrics["avg_put_delta"],
                metrics["avg_call_theta"], metrics["avg_put_theta"],
                metrics["avg_call_gamma"], metrics["avg_put_gamma"],
                metrics["avg_call_vega"], metrics["avg_put_vega"],
            ),
        )
        db.commit()
    except Exception as exc:
        log_err("save_options_metrics failed", exc)

def save_trade(trade: dict) -> int | None:
    db = get_db()
    db.execute(
        """INSERT INTO trades
           (timestamp,side,pending_price,entry_price,sl_price,tp_price,fib_level,
            lots,size_btc,notional_usd,risk_usd,status,
            exit_price,exit_time,pnl_usd,pnl_inr,exit_reason,
            oi_at_entry,ls_at_entry,oi_phase,option_oi,futures_depth)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            trade["timestamp"], trade["side"],
            trade["pending_price"], trade.get("entry_price"),
            trade["sl_price"],  trade["tp_price"], trade["fib_level"],
            trade["lots"],      trade["size_btc"],  trade["notional_usd"],
            trade["risk_usd"],  trade["status"],
            trade.get("exit_price"), trade.get("exit_time"),
            trade.get("pnl_usd"),    trade.get("pnl_inr"),
            trade.get("exit_reason"),
            trade.get("oi_at_entry", 0), trade.get("ls_at_entry", 0),
            trade.get("oi_phase", ""),
            trade.get("option_oi", 0.0), trade.get("futures_depth", 0.0)
        ),
    )
    db.commit()
    row = db.execute("SELECT MAX(id) FROM trades").fetchone()
    return row[0] if row else None

def update_trade(trade_id, status, entry_price=None, exit_price=None,
                 exit_time=None, pnl_usd=None, pnl_inr=None, reason=None):
    db  = get_db()
    db.execute(
        """UPDATE trades SET status=?,entry_price=COALESCE(?,entry_price),
           exit_price=COALESCE(?,exit_price),exit_time=COALESCE(?,exit_time),
           pnl_usd=COALESCE(?,pnl_usd),pnl_inr=COALESCE(?,pnl_inr),
           exit_reason=COALESCE(?,exit_reason)
           WHERE id=?""",
        (status, entry_price, exit_price, exit_time, pnl_usd, pnl_inr, reason, trade_id),
    )
    db.commit()

def update_trade_sl(trade_id, sl_price):
    try:
        db = get_db()
        db.execute("UPDATE trades SET sl_price=? WHERE id=?", (sl_price, trade_id))
        db.commit()
    except Exception as exc:
        log_err("update_trade_sl failed", exc)

def save_account_state(state: dict):
    try:
        db = get_db()
        db.execute(
            """INSERT INTO account_state
               (timestamp,capital_usd,capital_inr,peak_capital,drawdown_pct,
                total_trades,winning_trades,losing_trades,total_pnl_usd)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                state["capital_usd"],  state["capital_inr"],
                state["peak_capital"], state["drawdown_pct"],
                state["total_trades"], state["winning_trades"],
                state["losing_trades"],state["total_pnl_usd"],
            ),
        )
        db.commit()
    except Exception as exc:
        log_err("save_account_state", exc)

# ═══════════════════════════════════════════════════════════════
#  PENDING ORDER SYSTEM (Whale Wall & Chop guard integrated)
# ═══════════════════════════════════════════════════════════════
def create_pending(fib_level: str, reversal_close: float, phase: dict,
                   is_bull: bool, account: dict, signals: dict,
                   oi_curr: float, ls_curr: float, oi_phase: str,
                   ob: dict, chop_active: bool) -> dict | None:
    if signals.get("halted"):
        return None

    side = "buy" if is_bull else "sell"

    if chop_active:
        signals["last_signal"] = f"[CHOP BLOCKED] {side.upper()} @ Fib {fib_level}"
        console.print(f"[yellow]Chop filter blocked {side.upper()} at Fib {fib_level}[/yellow]")
        return None

    fib_prices = calc_fib_prices(phase["phase_st"], phase["phase_hh"], phase["phase_ll"], is_bull)
    target_price = fib_prices[fib_level]

    # Evaluate Institutional Whale Wall
    opt_oi, fut_size, is_whale = evaluate_whale_wall(target_price, ob)
    if not is_whale:
        signals["last_signal"] = f"[WHALE BLOCKED] {side.upper()} @ {target_price:,.0f} (OI:{opt_oi:,.0f}, Sz:{fut_size:,.0f})"
        console.print(f"[yellow]Whale Wall blocked {side.upper()} @ {target_price:,.0f}: OptionOI={opt_oi:,.0f}, FutSize={fut_size:,.0f}[/yellow]")
        return None

    # SL/TP mapping (Dynamic Wider TP for trend-riding macro moves)
    SL_TP = {
        "0.2":  {"sl": "-0.1", "tp": "2"},
        "0.35": {"sl": "0.2",  "tp": "2"},
        "0.5":  {"sl": "0.35", "tp": "2"},
    }
    mapping  = SL_TP[fib_level]
    sl_price = fib_prices[mapping["sl"]]
    tp_price = fib_prices[mapping["tp"]]

    # OI institutional filter
    if not oi_allows_trade(oi_phase, is_bull):
        signals["last_signal"] = f"[OI BLOCKED] {side.upper()} @ Fib {fib_level} — {oi_phase}"
        console.print(f"[yellow]OI filter blocked {side.upper()} at Fib {fib_level}: {oi_phase}[/yellow]")
        return None

    lots, risk_usd = calc_lot_size(account["capital_usd"], RISK_PER_TRADE_PCT,
                                    reversal_close, sl_price, LEVERAGE)
    if lots == 0:
        console.print(f"[yellow]Stop too tight at Fib {fib_level} — skipped[/yellow]")
        return None

    pending = {
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "side":          side,
        "fib_level":     fib_level,
        "pending_price": reversal_close,
        "sl_price":      sl_price,
        "tp_price":      tp_price,
        "lots":          lots,
        "size_btc":      lots * CONTRACT_VALUE,
        "notional_usd":  lots * reversal_close * CONTRACT_VALUE,
        "risk_usd":      risk_usd,
        "oi_at_entry":   oi_curr,
        "ls_at_entry":   ls_curr,
        "oi_phase":      oi_phase,
        "option_oi":     opt_oi,
        "futures_depth": fut_size,
        "_id":           None,
    }

    trade_record = dict(pending)
    trade_record["status"]      = "pending"
    trade_record["entry_price"] = None
    pending["_id"] = save_trade(trade_record)

    signals["last_signal"]  = f"PENDING {side.upper()} @ {reversal_close:,.2f} | Fib {fib_level} | {oi_phase}"
    signals["signal_level"] = f"Fib {fib_level}"
    signals["signal_time"]  = datetime.now().strftime("%H:%M:%S")

    console.print(f"[cyan]Pending {side.upper()} created @ {reversal_close:,.2f} "
                  f"SL:{sl_price:,.2f} TP:{tp_price:,.2f}[/cyan]")
    return pending

def activate_pending(pending: dict, mark_price: float, account: dict, signals: dict) -> dict | None:
    side    = pending["side"]
    pp      = pending["pending_price"]

    triggered = (side == "buy"  and mark_price >= pp) or \
                (side == "sell" and mark_price <= pp)

    if not triggered:
        return None

    entry_price = pp
    position = {
        "side":         side,
        "entry_price":  entry_price,
        "sl_price":     pending["sl_price"],
        "tp_price":     pending["tp_price"],
        "fib_level":    pending["fib_level"],
        "lots":         pending["lots"],
        "size_btc":     pending["size_btc"],
        "notional_usd": pending["lots"] * entry_price * CONTRACT_VALUE,
        "risk_usd":     pending["risk_usd"],
        "oi_at_entry":  pending["oi_at_entry"],
        "ls_at_entry":  pending["ls_at_entry"],
        "oi_phase":     pending["oi_phase"],
        "_id":          pending["_id"],
    }

    update_trade(pending["_id"], "open", entry_price=entry_price)
    account["total_trades"] += 1

    signals["last_signal"] = (f"ACTIVATED {side.upper()} @ {entry_price:,.2f} "
                               f"SL:{pending['sl_price']:,.2f} TP:{pending['tp_price']:,.2f}")
    console.print(f"[green]Trade ACTIVATED {side.upper()} @ {entry_price:,.2f}[/green]")
    return position

# ═══════════════════════════════════════════════════════════════
#  POSITION MANAGER (Trailing stop logic integrated)
# ═══════════════════════════════════════════════════════════════
def manage_position(position: dict, mark_price: float,
                    account: dict, signals: dict,
                    current_supertrend: float) -> tuple:
    if not position:
        return position, account

    side = position["side"]
    sl   = position["sl_price"]
    tp   = position["tp_price"]

    # Trailing Stop: move SL behind the Supertrend line (protecting macro trend)
    if side == "buy":
        new_sl = max(sl, current_supertrend)
        if new_sl != sl:
            position["sl_price"] = new_sl
            update_trade_sl(position["_id"], new_sl)
    else:
        new_sl = min(sl, current_supertrend)
        if new_sl != sl:
            position["sl_price"] = new_sl
            update_trade_sl(position["_id"], new_sl)

    sl = position["sl_price"]

    hit_sl = (side == "buy"  and mark_price <= sl) or (side == "sell" and mark_price >= sl)
    hit_tp = (side == "buy"  and mark_price >= tp) or (side == "sell" and mark_price <= tp)

    if hit_sl or hit_tp:
        exit_reason = "SL" if hit_sl else "TP"
        exit_price  = mark_price
        pnl_usd     = ((exit_price - position["entry_price"]) if side == "buy"
                        else (position["entry_price"] - exit_price)) * position["lots"] * CONTRACT_VALUE
        pnl_inr     = pnl_usd / INR_TO_USD
        exit_time   = datetime.now(timezone.utc).isoformat()

        update_trade(position["_id"], "closed",
                     exit_price=exit_price, exit_time=exit_time,
                     pnl_usd=round(pnl_usd, 4), pnl_inr=round(pnl_inr, 2),
                     reason=exit_reason)

        account["capital_usd"]   += pnl_usd
        account["capital_inr"]    = account["capital_usd"] / INR_TO_USD
        account["total_pnl_usd"] += pnl_usd
        account["peak_capital"]   = max(account["peak_capital"], account["capital_usd"])
        if pnl_usd >= 0:
            account["winning_trades"] += 1
        else:
            account["losing_trades"]  += 1

        signals["last_signal"] = f"{exit_reason} @ {exit_price:,.2f} | PnL ${pnl_usd:,.2f}"
        console.print(f"[{'green' if pnl_usd>=0 else 'red'}]{exit_reason} hit "
                      f"@ {exit_price:,.2f} | PnL ${pnl_usd:,.2f}[/]")
        return None, account

    return position, account

# ═══════════════════════════════════════════════════════════════
#  DASHBOARD (Updated with Chop, PCR, and Whale Wall tags)
# ═══════════════════════════════════════════════════════════════
def render_dashboard(state, ticker, ob, fib_prices, phase_state, position,
                     pending, signals, oi_intel, chop_ratio, is_chop):

    q       = (ticker or {}).get("quotes", {}) or {}
    mark    = float((ticker or {}).get("mark_price",    0) or 0)
    spot    = float((ticker or {}).get("spot_price",    0) or 0)
    bid     = float(q.get("best_bid", 0) or 0)
    ask     = float(q.get("best_ask", 0) or 0)
    oi      = float((ticker or {}).get("oi",            0) or 0)
    vol     = float((ticker or {}).get("volume",        0) or 0)
    chg_24h = float((ticker or {}).get("ltp_change_24h",0) or 0)
    chg_col = "green" if chg_24h >= 0 else "red"

    # ── Panel 1: Market ──────────────────────────────────────
    mkt = Table(show_header=False, box=None, padding=(0, 1))
    mkt.add_column("K", style="cyan",  width=16)
    mkt.add_column("V", style="white", width=16)
    mkt.add_row("Mark Price",  f"[bold yellow]{mark:,.2f}[/bold yellow]")
    mkt.add_row("Spot Price",  f"{spot:,.2f}")
    mkt.add_row("Best Bid",    f"[green]{bid:,.2f}[/green]")
    mkt.add_row("Best Ask",    f"[red]{ask:,.2f}[/red]")
    mkt.add_row("Spread",      f"{round(ask-bid,2):.2f}")
    mkt.add_row("24h Change",  f"[{chg_col}]{chg_24h:.4f}%[/{chg_col}]")
    mkt.add_row("OI (lots)",   f"{oi:,.0f}")
    mkt.add_row("Volume",      f"{vol:,.0f}")
    mkt.add_row("Timeframe",   "[bold]4 Hours[/bold]")

    # ── Panel 2: Account ─────────────────────────────────────
    dd_col = "red" if state["drawdown_pct"] > 3 else "yellow" if state["drawdown_pct"] > 1 else "green"
    acc = Table(show_header=False, box=None, padding=(0, 1))
    acc.add_column("K", style="cyan",  width=16)
    acc.add_column("V", style="white", width=16)
    acc.add_row("Capital USD",  f"${state['capital_usd']:,.2f}")
    acc.add_row("Capital INR",  f"₹{state['capital_inr']:,.2f}")
    acc.add_row("Peak Capital", f"${state['peak_capital']:,.2f}")
    acc.add_row("Drawdown",     f"[{dd_col}]{state['drawdown_pct']:.2f}%[/{dd_col}]")
    acc.add_row("Total PnL",    f"[{'green' if state['total_pnl_usd']>=0 else 'red'}]${state['total_pnl_usd']:,.2f}[/]")
    acc.add_row("Trades",       f"{state['total_trades']} (W:{state['winning_trades']} L:{state['losing_trades']})")
    wr = (state["winning_trades"] / state["total_trades"] * 100) if state["total_trades"] > 0 else 0
    acc.add_row("Win Rate",     f"{wr:.1f}%")
    acc.add_row("DD Limit",     f"{MAX_DRAWDOWN_PCT*100:.0f}%")

    # ── Panel 3: Fib Phase (Whale Wall Tagged) ────────────────
    fib_tbl = Table(show_header=False, box=None, padding=(0, 1))
    fib_tbl.add_column("K", style="cyan",  width=10)
    fib_tbl.add_column("V", style="white", width=12)
    fib_tbl.add_column("S", style="white", width=8)
    ib     = phase_state["is_bull"]
    pc     = "green" if ib else "red"
    pt     = "BULL" if ib else "BEAR" if ib is not None else "—"
    fib_tbl.add_row("Phase", f"[{pc}]{pt}[/{pc}]", "")
    fib_tbl.add_row("ST",    f"{phase_state['phase_st']:.2f}" if phase_state["phase_st"] else "—", "")
    fib_tbl.add_row("HH",    f"{phase_state['phase_hh']:.2f}" if phase_state["phase_hh"] else "—", "")
    fib_tbl.add_row("LL",    f"{phase_state['phase_ll']:.2f}" if phase_state["phase_ll"] else "—", "")
    if fib_prices:
        sm = {"0.2": phase_state.get("state02",0), "0.35": phase_state.get("state035",0),
              "0.5": phase_state.get("state05",0)}
        for lvl in ["2","1.5","1","0.5","0.35","0.2","0","-0.1"]:
            p     = fib_prices.get(lvl)
            sv    = sm.get(lvl, -1)
            ss    = "[orange1]BRK[/orange1]" if sv==1 else "[green]REV[/green]" if sv==2 else ""
            
            # Label Whale walls near golden levels
            whale_tag = ""
            if lvl in ["0.2", "0.35", "0.5"] and p:
                o_oi, f_sz, is_w = evaluate_whale_wall(p, ob)
                if is_w: whale_tag = "[bold red][W][/bold red]"
                
            fib_tbl.add_row(f"Fib {lvl}", f"{p:,.1f} {whale_tag}" if p else "—", ss)

    # ── Panel 4: Position / Pending (Trailing SL) ────────────
    pos_tbl = Table(show_header=False, box=None, padding=(0, 1))
    pos_tbl.add_column("K", style="cyan",  width=14)
    pos_tbl.add_column("V", style="white", width=16)

    if position:
        sc  = "green" if position["side"] == "buy" else "red"
        upnl = ((mark - position["entry_price"]) if position["side"] == "buy"
                 else (position["entry_price"] - mark)) * position["lots"] * CONTRACT_VALUE
        pc2 = "green" if upnl >= 0 else "red"
        pos_tbl.add_row("Status",    "[bold green]ACTIVE[/bold green]")
        pos_tbl.add_row("Side",      f"[{sc}]{position['side'].upper()}[/{sc}]")
        pos_tbl.add_row("Entry",     f"{position['entry_price']:,.2f}")
        pos_tbl.add_row("Trailing SL",f"[red]{position['sl_price']:,.2f}[/red]")
        pos_tbl.add_row("TP Target", f"[green]{position['tp_price']:,.2f}[/green]")
        pos_tbl.add_row("Fib Level", position["fib_level"])
        pos_tbl.add_row("Lots",      str(position["lots"]))
        pos_tbl.add_row("Unrealised",f"[{pc2}]${upnl:,.2f}[/{pc2}]")
    elif pending:
        sc = "green" if pending["side"] == "buy" else "red"
        pos_tbl.add_row("Status",   "[bold yellow]PENDING[/bold yellow]")
        pos_tbl.add_row("Side",     f"[{sc}]{pending['side'].upper()}[/{sc}]")
        pos_tbl.add_row("Trigger @",f"[yellow]{pending['pending_price']:,.2f}[/yellow]")
        pos_tbl.add_row("SL Target",f"[red]{pending['sl_price']:,.2f}[/red]")
        pos_tbl.add_row("TP Target",f"[green]{pending['tp_price']:,.2f}[/green]")
        pos_tbl.add_row("Fib Level",pending["fib_level"])
        pos_tbl.add_row("Risk USD", f"${pending['risk_usd']:,.2f}")
    else:
        pos_tbl.add_row("Status", "[grey50]No position[/grey50]")

    # ── Panel 5: Options & Chop Intelligence ────────────────
    oi_tbl = Table(show_header=False, box=None, padding=(0, 1))
    oi_tbl.add_column("K", style="cyan",  width=16)
    oi_tbl.add_column("V", style="white", width=16)

    trend     = oi_intel.get("trend",        "—")
    trend_col = "green" if "BULL" in trend or "LONGS" in trend else "red" if "BEAR" in trend or "SHORTS" in trend else "yellow"

    pcr       = oi_intel.get("pcr_oi", 0.0)
    pcr_col   = "green" if pcr < 0.7 else "red" if pcr > 1.0 else "yellow"
    pcr_label = f"{pcr:.3f} ({'BULL' if pcr<0.7 else 'BEAR' if pcr>1.0 else 'NEUTRAL'})"

    chop_col   = "red" if is_chop else "green"
    chop_label = f"[{chop_col}]{'CHOP ZONE' if is_chop else 'TRENDING'} ({chop_ratio:.2f})[/{chop_col}]"

    oi_tbl.add_row("Market Trend",  f"[{trend_col}]{trend}[/{trend_col}]")
    oi_tbl.add_row("Options PCR",   f"[{pcr_col}]{pcr_label}[/{pcr_col}]")
    oi_tbl.add_row("Call OI",       f"{oi_intel.get('total_call_oi',0):,.0f}")
    oi_tbl.add_row("Put OI",        f"{oi_intel.get('total_put_oi',0):,.0f}")
    oi_tbl.add_row("Chop Filter",   chop_label)
    oi_tbl.add_row("Avg Call Delta",f"{oi_intel.get('avg_call_delta',0):.3f}")
    oi_tbl.add_row("Avg Put Delta", f"{oi_intel.get('avg_put_delta',0):.3f}")
    oi_tbl.add_row("Avg Call Theta",f"{oi_intel.get('avg_call_theta',0):.1f}")
    oi_tbl.add_row("Avg Put Theta", f"{oi_intel.get('avg_put_theta',0):.1f}")

    # ── Panel 6: Order Book ──────────────────────────────────
    book = Table(show_header=True, box=None, padding=(0, 1))
    book.add_column("Bid Sz", style="green", width=8)
    book.add_column("Bid",    style="green", width=10)
    book.add_column("Ask",    style="red",   width=10)
    book.add_column("Ask Sz", style="red",   width=8)
    if ob:
        buys  = ob.get("buy",  [])[:5]
        sells = ob.get("sell", [])[:5]
        for i in range(5):
            b = buys[i]  if i < len(buys)  else {}
            s = sells[i] if i < len(sells) else {}
            book.add_row(
                str(b.get("size", "")),
                f"{float(b['price']):,.1f}" if b.get("price") else "",
                f"{float(s['price']):,.1f}" if s.get("price") else "",
                str(s.get("size", "")),
            )

    # ── Panel 7: Signals ─────────────────────────────────────
    sig = Table(show_header=False, box=None, padding=(0, 1))
    sig.add_column("K", style="cyan",  width=14)
    sig.add_column("V", style="white", width=18)
    sig.add_row("Last Signal",   signals.get("last_signal",  "—"))
    sig.add_row("Signal Level",  signals.get("signal_level", "—"))
    sig.add_row("Signal Time",   signals.get("signal_time",  "—"))
    sig.add_row("Halted",        "[red]YES[/red]" if signals.get("halted") else "[green]NO[/green]")
    sig.add_row("Candles Count", str(signals.get("candle_count", 0)))
    sig.add_row("ST Results",    str(signals.get("st_results", 0)))
    sig.add_row("Timeframe",     "4 Hours")

    title = (f"[bold white] ALPHA v6 — INSTITUTIONAL BTCUSD 4H PAPER BOT | "
             f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} [/bold white]")
    return Panel(
        Columns([
            Panel(mkt,     title="[bold cyan]MARKET[/bold cyan]",           border_style="cyan"),
            Panel(acc,     title="[bold yellow]ACCOUNT[/bold yellow]",       border_style="yellow"),
            Panel(fib_tbl, title="[bold magenta]FIB PHASE[/bold magenta]",   border_style="magenta"),
            Panel(pos_tbl, title="[bold green]POSITION / PENDING[/bold green]",border_style="green"),
            Panel(oi_tbl,  title="[bold red]OI & CHOP INTEL[/bold red]",     border_style="red"),
            Panel(book,    title="[bold white]ORDER BOOK[/bold white]",       border_style="white"),
            Panel(sig,     title="[bold blue]SIGNALS[/bold blue]",            border_style="blue"),
        ]),
        title=title,
        border_style="bright_blue",
    )

# ═══════════════════════════════════════════════════════════════
#  MAIN BOT LOOP
# ═══════════════════════════════════════════════════════════════
def run_bot():
    init_db()
    console.print("[bold green]Alpha v6 — 4-Hour Timeframe | Institutional Whale walls | Chop Filter[/bold green]")

    # ── Restore state ─────────────────────────────────────────
    saved_acct, saved_pos, saved_pending, saved_phase = load_state()
    account = saved_acct or {
        "capital_usd":    CAPITAL_USD,
        "capital_inr":    CAPITAL_INR,
        "peak_capital":   CAPITAL_USD,
        "drawdown_pct":   0.0,
        "total_trades":   0,
        "winning_trades": 0,
        "losing_trades":  0,
        "total_pnl_usd":  0.0,
    }
    position = saved_pos
    pending  = saved_pending

    if saved_acct:  console.print("[cyan]Account state restored[/cyan]")
    if position:    console.print(f"[cyan]Active {position['side'].upper()} position restored[/cyan]")
    if pending:     console.print(f"[cyan]Pending {pending['side'].upper()} order restored[/cyan]")
    if saved_phase: console.print("[cyan]Phase state restored[/cyan]")

    # ── Phase / signal state ──────────────────────────────────
    phase = {
        "is_bull": None, "phase_st": None, "phase_hh": None, "phase_ll": None,
        "measuring": False,
        "state02": 0, "state035": 0, "state05": 0,
        "state02_age": 0, "state035_age": 0, "state05_age": 0,
        "fib02": None, "fib035": None, "fib05": None,
        "const_count": 0, "prev_dir": None,
    }
    if saved_phase:
        phase.update(saved_phase)

    signals   = {"last_signal": "—", "signal_level": "—", "signal_time": "—",
                 "halted": False, "candle_count": 0, "st_results": 0}
    candles   = CandleBuilder()
    
    # Reconstruct fib_prices if active phase was restored
    fib_prices: dict = {}
    if phase.get("measuring") and phase.get("phase_st") is not None:
        fib_prices = calc_fib_prices(
            phase["phase_st"], phase["phase_hh"], phase["phase_ll"], phase["is_bull"]
        )

    # ── OI & Options intelligence state ──────────────────────
    oi_intel = {
        "trend": "—", "oi_change_session": 0.0, "dominant_phase": "—",
        "avg_ls_ratio": 0.0, "ask_bid_ratio": 0.0, "price_change_session": 0.0,
        "current_phase": "NEUTRAL", "trades_blocked": 0,
        "pcr_oi": 0.0, "total_call_oi": 0.0, "total_put_oi": 0.0,
        "avg_call_delta": 0.0, "avg_put_delta": 0.0,
        "avg_call_theta": 0.0, "avg_put_theta": 0.0,
    }
    oi_start     = 0.0
    price_start  = 0.0
    ls_history:   list[float] = []
    phase_counts: dict        = {}
    prev_oi      = 0.0
    prev_price   = 0.0
    ob_last_save = 0.0
    
    chop_ratio   = 1.0
    is_chop      = False

    # ── Historical candles ────────────────────────────────────
    hist = fetch_historical_candles(HIST_LOOKBACK_MINS)
    if hist:
        candles.add_historical(hist)
        console.print(f"[green]Pre-loaded {len(hist)} × 4-hour candles[/green]")
    else:
        console.print("[yellow]No history — candles will build from live data[/yellow]")

    ticker = ob = None

    with Live(console=console, refresh_per_second=0.2, screen=True) as live:
        while True:
            try:
                ticker = fetch_ticker()
                ob     = fetch_orderbook(depth=10)

                if not ticker:
                    time.sleep(LOOP_SLEEP)
                    continue

                mark_price = float(ticker.get("mark_price", 0) or 0)
                curr_oi    = float(ticker.get("oi",          0) or 0)
                volume     = float(ticker.get("volume",      0) or 0)

                # ── Initialise session baseline ───────────────
                if oi_start == 0.0 and curr_oi > 0:
                    oi_start    = curr_oi
                    price_start = mark_price
                if prev_price == 0.0:
                    prev_price  = mark_price
                if prev_oi == 0.0:
                    prev_oi     = curr_oi

                # ── Save market data ──────────────────────────
                save_market_data(ticker)
                now_ts = time.time()

                # Order book & Options metrics — throttled to OB_SAVE_INTERVAL
                if ob and (now_ts - ob_last_save) >= OB_SAVE_INTERVAL:
                    save_orderbook(ob)
                    save_market_breadth(ob, prev_oi, curr_oi)
                    
                    # Fetch active options chain for PCR & Greeks
                    opt_metrics = fetch_options_metrics_and_cache()
                    if opt_metrics:
                        save_options_metrics(opt_metrics)
                        oi_intel.update({
                            "pcr_oi":          opt_metrics["pcr_oi"],
                            "total_call_oi":   opt_metrics["total_call_oi"],
                            "total_put_oi":    opt_metrics["total_put_oi"],
                            "avg_call_delta":  opt_metrics["avg_call_delta"],
                            "avg_put_delta":   opt_metrics["avg_put_delta"],
                            "avg_call_theta":  opt_metrics["avg_call_theta"],
                            "avg_put_theta":   opt_metrics["avg_put_theta"],
                        })
                    ob_last_save = now_ts

                # ── OI intelligence update ────────────────────
                curr_phase = classify_oi_phase(prev_oi, curr_oi, prev_price, mark_price)
                phase_counts[curr_phase] = phase_counts.get(curr_phase, 0) + 1

                if ob:
                    bids    = ob.get("buy",  [])
                    asks    = ob.get("sell", [])
                    bid_tot = sum(float(b["size"]) for b in bids) or 1
                    ask_tot = sum(float(a["size"]) for a in asks) or 1
                    ls_now  = bid_tot / ask_tot
                    ls_history.append(ls_now)
                    if len(ls_history) > 500:
                        ls_history.pop(0)
                    avg_ls     = sum(ls_history) / len(ls_history)
                    ask_bid_r  = ask_tot / bid_tot
                else:
                    ls_now = avg_ls = ask_bid_r = 0.0

                dominant = max(phase_counts, key=phase_counts.get) if phase_counts else "—"
                trend    = market_trend_label(oi_start, curr_oi, price_start, mark_price)

                oi_intel.update({
                    "trend":                trend,
                    "oi_change_session":    curr_oi - oi_start,
                    "dominant_phase":       dominant,
                    "avg_ls_ratio":         avg_ls,
                    "ask_bid_ratio":        ask_bid_r,
                    "price_change_session": mark_price - price_start,
                    "current_phase":        curr_phase,
                })

                prev_oi    = curr_oi
                prev_price = mark_price

                # ── Candle update (4h) ────────────────────────
                candle_closed = candles.update(mark_price, volume, int(now_ts))
                highs  = candles.get_highs()
                lows   = candles.get_lows()
                closes = candles.get_closes()
                signals["candle_count"] = candles.count()

                if candle_closed:
                    # ── Volatility Chop Zone Calculation ──────────
                    if candles.count() >= ATR_PERIOD + 30:
                        chop_ratio, is_chop = calc_chop_status(highs, lows, closes)
                    else:
                        chop_ratio, is_chop = 1.0, False
                    save_chop_status(chop_ratio, is_chop)

                    # ── Supertrend ────────────────────────────────
                    st_results: list = []
                    if candles.count() >= ATR_PERIOD + 2:
                        st_results = calc_supertrend(highs, lows, closes)
                    signals["st_results"] = len(st_results)

                    # ── Phase logic (ST value changed) ────────────
                    if len(st_results) >= CONFIRM_BARS + 2:
                        latest_st,  latest_dir  = st_results[-1]
                        _prev_st,   prev_dir_v  = st_results[-2]
                        is_bull  = (latest_dir == -1)

                        st_changed = (latest_st != _prev_st)
                        if st_changed:
                            phase["const_count"] = 0
                        else:
                            phase["const_count"] += 1

                        dir_flip       = (phase["prev_dir"] is not None and latest_dir != phase["prev_dir"])
                        just_confirmed = (phase["const_count"] == CONFIRM_BARS)

                        if dir_flip:
                            phase.update({
                                "measuring": False, "phase_st": None, "phase_hh": None, "phase_ll": None,
                                "state02": 0, "state035": 0, "state05": 0,
                                "state02_age": 0, "state035_age": 0, "state05_age": 0,
                                "fib02": None, "fib035": None, "fib05": None,
                            })
                            fib_prices = {}
                            if pending:
                                update_trade(pending["_id"], "cancelled")
                                pending = None
                                console.print("[yellow]Pending order cancelled — direction flip[/yellow]")

                        if just_confirmed and len(closes) >= CONFIRM_BARS + 2:
                            phase["measuring"] = True
                            phase["is_bull"]   = is_bull
                            phase["phase_st"]  = latest_st
                            phase["phase_hh"]  = max(highs[-(CONFIRM_BARS + 1):])
                            phase["phase_ll"]  = min(lows[-(CONFIRM_BARS + 1):])
                            phase["state02"] = phase["state035"] = phase["state05"] = 0
                            phase["state02_age"] = phase["state035_age"] = phase["state05_age"] = 0
                            fib_prices = calc_fib_prices(
                                phase["phase_st"], phase["phase_hh"], phase["phase_ll"], is_bull
                            )
                            phase["fib02"]  = fib_prices["0.2"]
                            phase["fib035"] = fib_prices["0.35"]
                            phase["fib05"]  = fib_prices["0.5"]

                        phase["prev_dir"] = latest_dir

                        # ── Signal detection on CLOSED candle only ─
                        if (phase["measuring"] and not dir_flip and not just_confirmed
                                and candles.last_closed() is not None
                                and not position and not pending):

                            lc      = candles.last_closed()
                            c_open  = lc["open"]
                            c_close = lc["close"]
                            c_range = lc["high"] - lc["low"]
                            c_body  = abs(c_close - c_open)
                            is_doji = c_range > 0 and (c_body / c_range) < DOJI_FACTOR

                            def check_break(fp):
                                if fp is None: return False
                                return (c_open > fp and c_close < fp) if is_bull else (c_open < fp and c_close > fp)

                            def check_reversal(fp):
                                if fp is None or is_doji: return False
                                return (c_open < fp and c_close > fp) if is_bull else (c_open > fp and c_close < fp)

                            # Age out stale broken states
                            for sk, ak in [("state02","state02_age"),("state035","state035_age"),("state05","state05_age")]:
                                if phase[sk] == 1:
                                    phase[ak] += 1
                                    if phase[ak] > MAX_STATE_AGE_BARS:
                                        phase[sk] = 0; phase[ak] = 0

                            reversal_close = c_close

                            for lvl, fk, sk, ak in [
                                ("0.2",  "fib02",  "state02",  "state02_age"),
                                ("0.35", "fib035", "state035", "state035_age"),
                                ("0.5",  "fib05",  "state05",  "state05_age"),
                            ]:
                                fp = phase[fk]
                                if phase[sk] == 0 and check_break(fp):
                                    phase[sk] = 1; phase[ak] = 0
                                elif phase[sk] == 1 and check_reversal(fp):
                                    phase[sk] = 2
                                    p = create_pending(
                                        lvl, reversal_close, phase, is_bull, account,
                                        signals, curr_oi, ls_now, curr_phase, ob, is_chop
                                    )
                                    if p is None:
                                        oi_intel["trades_blocked"] += 1
                                    else:
                                        pending = p
                                        break

                # ── Activate pending order ────────────────────
                if pending and not position:
                    pos = activate_pending(pending, mark_price, account, signals)
                    if pos:
                        position = pos
                        pending  = None

                # ── Manage active position (Trailing stop active) 
                if position:
                    position, account = manage_position(position, mark_price, account, signals, latest_st)

                # ── Drawdown check ────────────────────────────
                if account["peak_capital"] > 0:
                    dd = (account["peak_capital"] - account["capital_usd"]) / account["peak_capital"] * 100
                    account["drawdown_pct"] = round(dd, 4)
                    if dd >= MAX_DRAWDOWN_PCT * 100:
                        signals["halted"] = True
                        if pending:
                            update_trade(pending["_id"], "cancelled")
                            pending = None

                save_account_state(account)
                save_state(account, position, pending, phase)

                live.update(render_dashboard(
                    account, ticker, ob, fib_prices, phase, position,
                    pending, signals, oi_intel, chop_ratio, is_chop
                ))

                time.sleep(LOOP_SLEEP)

            except KeyboardInterrupt:
                console.print("\n[yellow]Bot stopped.[/yellow]")
                break
            except Exception as exc:
                log_err("Main loop error", exc)
                console.print(f"[red]Loop error (logged): {exc}[/red]")
                time.sleep(LOOP_SLEEP)

if __name__ == "__main__":
    run_bot()
