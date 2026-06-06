import sys
import os
import json

# Add tradingview-mcp/src to Python path
mcp_src = os.path.join(os.path.dirname(__file__), "tradingview-mcp", "src")
sys.path.append(mcp_src)

from tradingview_mcp.core.services import backtest_service, yahoo_finance_service

def run_demo():
    print("==================================================")
    print("TradingView MCP Programmatic Demo")
    print("==================================================")

    # 1. Fetch real-time price info from Yahoo Finance
    symbol = "BTC-USD"
    print(f"\n1. Fetching real-time price info for {symbol}...")
    price_info = yahoo_finance_service.get_price(symbol)
    print(json.dumps(price_info, indent=2))

    # 2. Run historical strategy backtest
    strategy = "supertrend"
    period = "1y"
    print(f"\n2. Running backtest for {symbol} using strategy: {strategy} (Period: {period})...")
    backtest_result = backtest_service.run_backtest(
        symbol=symbol,
        strategy=strategy,
        period=period,
        initial_capital=10000.0,
        commission_pct=0.1,
        slippage_pct=0.05,
        interval="1d"
    )
    
    # Print high-level metrics
    metrics = {
        "symbol": backtest_result.get("symbol"),
        "strategy": backtest_result.get("strategy_label"),
        "total_trades": backtest_result.get("total_trades"),
        "win_rate_pct": backtest_result.get("win_rate_pct"),
        "total_return_pct": backtest_result.get("total_return_pct"),
        "buy_and_hold_return_pct": backtest_result.get("buy_and_hold_return_pct"),
        "vs_buy_and_hold_pct": backtest_result.get("vs_buy_and_hold_pct"),
        "sharpe_ratio": backtest_result.get("sharpe_ratio"),
        "max_drawdown_pct": backtest_result.get("max_drawdown_pct")
    }
    print(json.dumps(metrics, indent=2))

    # 3. Compare multiple strategies
    print(f"\n3. Comparing all strategies for {symbol}...")
    comparison = backtest_service.compare_strategies(symbol, period=period)
    print("\nLeaderboard (Ranked by Total Return):")
    for r in comparison.get("ranking", []):
        print(f"Rank #{r['rank']}: {r['strategy_label']} ({r['strategy']})")
        print(f"  Return: {r['total_return_pct']}% | Win Rate: {r['win_rate_pct']}% | Sharpe: {r['sharpe_ratio']} | Max DD: {r['max_drawdown_pct']}%")

if __name__ == "__main__":
    run_demo()
