import sqlite3
import os
import sys
import time
import requests
import json
from datetime import datetime, timezone

# Add tradingview-mcp/src to Python path so we can import services
mcp_src = os.path.join(os.path.dirname(__file__), "tradingview-mcp", "src")
sys.path.append(mcp_src)

from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.panel import Panel
from rich.columns import Columns
from rich.layout import Layout
from rich.text import Text

DB_V5 = "alpha_btcusd_v5.db"
DB_V6 = "alpha_btcusd_v6.db"
DB_NEWS = "news_events.db"
SYMBOL = "BTCUSD"
BASE_URL = "https://api.india.delta.exchange"
CONTRACT_VALUE = 0.001
INR_TO_USD = 0.012

console = Console()

def fetch_ticker() -> dict | None:
    try:
        response = requests.get(f"{BASE_URL}/v2/tickers/{SYMBOL}", timeout=5)
        if response.status_code == 200:
            data = response.json()
            if data and data.get("success"):
                return data["result"]
    except Exception:
        pass
    return None

def get_db_data(db_path, query, params=()):
    if not os.path.exists(db_path):
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        conn.close()
        return row
    except Exception:
        return None

def get_db_rows(db_path, query, params=()):
    if not os.path.exists(db_path):
        return []
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

def get_bot_status(db_path, mark_price):
    # Get latest account state
    acc = get_db_data(db_path, "SELECT * FROM account_state ORDER BY id DESC LIMIT 1")
    
    # Get active position
    pos = get_db_data(db_path, "SELECT * FROM trades WHERE status = 'open' ORDER BY id DESC LIMIT 1")
    
    # Get pending position (if no active position)
    pending = None
    if not pos:
        pending = get_db_data(db_path, "SELECT * FROM trades WHERE status = 'pending' ORDER BY id DESC LIMIT 1")
        
    return acc, pos, pending

def make_bot_table(title, acc, pos, pending, mark_price):
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("K", style="cyan", width=15)
    table.add_column("V", style="white", width=15)
    
    if not acc:
        table.add_row("Status", "NOT INITIALIZED")
        return Panel(table, title=title, border_style="red")
        
    # Account progress
    wr = (acc["winning_trades"] / acc["total_trades"] * 100) if acc["total_trades"] > 0 else 0.0
    pnl_color = "green" if acc["total_pnl_usd"] >= 0 else "red"
    
    table.add_row("Capital USD", f"${acc['capital_usd']:,.2f}")
    table.add_row("Capital INR", f"Rs {acc['capital_inr']:,.2f}")
    table.add_row("Total PnL", f"[{pnl_color}]${acc['total_pnl_usd']:+,.2f}[/{pnl_color}]")
    table.add_row("Drawdown", f"{acc['drawdown_pct']:.2f}%")
    table.add_row("Trades (W/L)", f"{acc['total_trades']} (W:{acc['winning_trades']} L:{acc['losing_trades']})")
    table.add_row("Win Rate", f"{wr:.1f}%")
    
    table.add_row("-" * 15, "-" * 15)
    
    if pos:
        side = pos["side"].upper()
        side_color = "green" if side == "BUY" else "red"
        entry = pos["entry_price"]
        lots = pos["lots"]
        # Calculate unrealized pnl
        if side == "BUY":
            upnl = (mark_price - entry) * lots * CONTRACT_VALUE
        else:
            upnl = (entry - mark_price) * lots * CONTRACT_VALUE
        upnl_color = "green" if upnl >= 0 else "red"
        
        table.add_row("Position", f"[{side_color}]ACTIVE {side}[/{side_color}]")
        table.add_row("Entry Price", f"${entry:,.2f}")
        table.add_row("SL / TP Target", f"${pos['sl_price']:,.2f} / ${pos['tp_price']:,.2f}")
        table.add_row("Lots / Size", f"{lots} ({lots*CONTRACT_VALUE:.3f} BTC)")
        table.add_row("Unrealized PnL", f"[{upnl_color}]${upnl:+,.2f}[/{upnl_color}]")
    elif pending:
        side = pending["side"].upper()
        side_color = "yellow"
        table.add_row("Position", f"[{side_color}]PENDING {side}[/{side_color}]")
        table.add_row("Trigger Price", f"${pending['pending_price']:,.2f}")
        table.add_row("SL / TP Target", f"${pending['sl_price']:,.2f} / ${pending['tp_price']:,.2f}")
        table.add_row("Lots", str(pending["lots"]))
    else:
        table.add_row("Position", "NO ACTIVE ORDERS")
        table.add_row("Unrealized PnL", "$0.00")
        
    return Panel(table, title=title, border_style="cyan")

def make_options_panel(db_path):
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("K", style="cyan", width=20)
    table.add_column("V", style="white", width=15)
    
    opt = get_db_data(db_path, "SELECT * FROM options_metrics ORDER BY id DESC LIMIT 1")
    if not opt:
        table.add_row("Status", "NO METRICS")
        return Panel(table, title="OPTION DATA ANALYSIS", border_style="magenta")
        
    pcr = opt["pcr_oi"]
    pcr_color = "green" if pcr < 0.7 else "red" if pcr > 1.0 else "yellow"
    pcr_state = "BULLISH" if pcr < 0.7 else "BEARISH" if pcr > 1.0 else "NEUTRAL"
    
    table.add_row("Put/Call Ratio", f"[{pcr_color}]{pcr:.3f} ({pcr_state})[/{pcr_color}]")
    table.add_row("Call OI Contracts", f"{opt['total_call_oi']:,.0f}")
    table.add_row("Put OI Contracts", f"{opt['total_put_oi']:,.0f}")
    table.add_row("Avg Call/Put Delta", f"{opt['avg_call_delta']:.3f} / {opt['avg_put_delta']:.3f}")
    table.add_row("Avg Call/Put Gamma", f"{opt['avg_call_gamma']:.4f} / {opt['avg_put_gamma']:.4f}")
    table.add_row("Avg Call/Put Vega", f"{opt['avg_call_vega']:.1f} / {opt['avg_put_vega']:.1f}")
    table.add_row("Avg Call/Put Theta", f"{opt['avg_call_theta']:.1f} / {opt['avg_put_theta']:.1f}")
    
    return Panel(table, title="OPTION DATA ANALYSIS", border_style="magenta")

def make_news_panel(mark_price):
    table = Table(show_header=False, box=None, padding=(0, 1))
    table.add_column("K", style="cyan", width=18)
    table.add_column("V", style="white", width=42)
    
    # 1. Fetch latest articles for sentiment score
    articles = get_db_rows(DB_NEWS, "SELECT sentiment_score FROM articles ORDER BY timestamp DESC LIMIT 10")
    avg_score = sum(r["sentiment_score"] for r in articles) / len(articles) if articles else 0.0
    
    # 2. Fetch latest Reddit sentiment
    reddit = get_db_data(DB_NEWS, "SELECT sentiment_score, sentiment_label, posts_analyzed FROM reddit_sentiment ORDER BY id DESC LIMIT 1")
    
    # 3. Combined Sentiment Index (40% News, 60% Reddit)
    r_score = reddit["sentiment_score"] if reddit else 0.0
    unified_score = (avg_score * 0.4) + (r_score * 0.6) if reddit else avg_score
    
    if unified_score < -0.15:
        unified_color = "red"
        unified_state = "BEARISH"
    elif unified_score > 0.15:
        unified_color = "green"
        unified_state = "BULLISH"
    else:
        unified_color = "yellow"
        unified_state = "NEUTRAL"
        
    table.add_row("Unified Score", f"[{unified_color}]{unified_score:+.3f} ({unified_state})[/{unified_color}]")
    table.add_row("News Sentiment", f"{avg_score:+.3f}")
    table.add_row("Reddit Sentiment", f"{r_score:+.3f}" if reddit else "N/A")
    
    # 4. Fetch upcoming macro event
    now_str = datetime.now(timezone.utc).isoformat()
    next_macro = get_db_data(DB_NEWS, "SELECT event_name, timestamp FROM macro_events WHERE status = 'pending' AND timestamp > ? ORDER BY timestamp ASC LIMIT 1", (now_str,))
    if next_macro:
        try:
            event_dt = datetime.fromisoformat(next_macro["timestamp"])
            now_dt = datetime.now(timezone.utc)
            hours_left = (event_dt - now_dt).total_seconds() / 3600
            table.add_row("Next Macro Event", f"{next_macro['event_name']}")
            table.add_row("Macro Time Left", f"{hours_left:.1f} hours")
        except Exception:
            table.add_row("Next Macro Event", next_macro["event_name"])
    else:
        table.add_row("Next Macro Event", "NONE PENDING")
        
    table.add_row("-" * 18, "-" * 42)
    
    # 5. Live Market Prediction
    prediction = "NEUTRAL / SIDEWAYS"
    pred_color = "yellow"
    pred_details = "Indicators are ranging, no major macro announcements in next 24h. Recommend normal entry limits."
    
    # If unified sentiment is bearish or upcoming macro event is close
    if unified_score < -0.15:
        prediction = "BEARISH BIAS"
        pred_color = "red"
        pred_details = "Confluence indicates heavy sell pressure & negative social sentiment. Bias: Down. Tighten trailing SL on active longs."
    elif unified_score > 0.15:
        prediction = "BULLISH BIAS"
        pred_color = "green"
        pred_details = "Confluence reports buying pressure & positive social sentiment. Bias: Up. Normal long/short setups validated."
        
    if next_macro:
        try:
            event_dt = datetime.fromisoformat(next_macro["timestamp"])
            hours_left = (event_dt - datetime.now(timezone.utc)).total_seconds() / 3600
            if hours_left <= 24.0:
                prediction = "HIGH VOLATILITY ALERT"
                pred_color = "bold red"
                pred_details = f"Macro {next_macro['event_name']} is in {hours_left:.1f} hours! Expect violent whipsaw action. Recommended: HALT TRADES."
        except Exception:
            pass
            
    table.add_row("Market Bias", f"[{pred_color}]{prediction}[/{pred_color}]")
    table.add_row("Action Plan", pred_details)
    
    return Panel(table, title="NEWS IMPACT & MARKET PREDICTIONS", border_style="yellow")

def build_layout(ticker, mark_price, v5_data, v6_data):
    # Fetch data
    v5_acc, v5_pos, v5_pending = v5_data
    v6_acc, v6_pos, v6_pending = v6_data
    
    # Header Details
    chg_24h = float((ticker or {}).get("ltp_change_24h", 0) or 0)
    chg_col = "green" if chg_24h >= 0 else "red"
    
    header_text = Text()
    header_text.append("=== ALPHA BOT SYSTEM CONSOLIDATED DASHBOARD ===\n", style="bold white")
    header_text.append(f"BTCUSD Mark Price: ${mark_price:,.2f} | ", style="yellow")
    header_text.append(f"24h Change: {chg_24h:+.4f}% | ", style=chg_col)
    header_text.append(f"System Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} (Local)", style="cyan")
    
    # Panels
    v5_panel = make_bot_table("ALPHA BOT V5 (15-MIN TIMEFRAME)", v5_acc, v5_pos, v5_pending, mark_price)
    v6_panel = make_bot_table("ALPHA BOT V6 (4-HOUR TIMEFRAME)", v6_acc, v6_pos, v6_pending, mark_price)
    options_panel = make_options_panel(DB_V5)
    news_panel = make_news_panel(mark_price)
    
    # Arrange Panels side by side or stacked
    left_side = Columns([v5_panel, v6_panel])
    right_side = Columns([options_panel, news_panel])
    
    # Construct final display
    return Panel(
        Columns([
            Panel(left_side, title="BOTS PROGRESSION & POSITION TRACKING", border_style="bright_blue", width=72),
            Panel(right_side, title="MARKET CONFLUENCE & OPTIONS ALERTS", border_style="bright_magenta", width=72)
        ]),
        title=header_text,
        border_style="bold green"
    )

def main():
    console.print("[green]Initializing Consolidated Dashboard View...[/green]")
    
    # Test databases exist
    if not os.path.exists(DB_V5) or not os.path.exists(DB_V6):
        console.print("[red][ERROR] Bot databases not found. Ensure both bots have run once to initialize databases.[/red]")
        return
        
    with Live(console=console, refresh_per_second=0.2, screen=True) as live:
        while True:
            try:
                ticker = fetch_ticker()
                if not ticker:
                    time.sleep(3)
                    continue
                
                mark_price = float(ticker.get("mark_price", 0) or 0)
                
                # Retrieve states
                v5_data = get_bot_status(DB_V5, mark_price)
                v6_data = get_bot_status(DB_V6, mark_price)
                
                # Render
                live.update(build_layout(ticker, mark_price, v5_data, v6_data))
                
                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                console.print(f"[red]Dashboard Error: {e}[/red]")
                time.sleep(5)

if __name__ == "__main__":
    main()
