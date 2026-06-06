import sqlite3
from datetime import datetime, timezone
import os

DB_FILE = "news_events.db"

def analyze_sentiment_trends():
    if not os.path.exists(DB_FILE):
        print("Database does not exist. Run news_collector.py first.")
        return
        
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # 1. Fetch recent articles
    cursor.execute("SELECT timestamp, source, title, sentiment_score, classification FROM articles ORDER BY timestamp DESC LIMIT 20")
    articles = cursor.fetchall()
    
    # 2. Fetch upcoming macro events
    now_str = datetime.now(timezone.utc).isoformat()
    cursor.execute("SELECT timestamp, event_name, impact_level, status FROM macro_events WHERE status = 'pending' AND timestamp > ? ORDER BY timestamp ASC LIMIT 1", (now_str,))
    next_macro = cursor.fetchone()
    
    # 3. Fetch latest Reddit sentiment
    cursor.execute("""
        SELECT timestamp, sentiment_score, sentiment_label, posts_analyzed, 
               bullish_count, bearish_count, neutral_count 
        FROM reddit_sentiment 
        ORDER BY timestamp DESC LIMIT 1
    """)
    reddit_row = cursor.fetchone()
    
    # 4. Fetch top Reddit posts
    cursor.execute("""
        SELECT subreddit, upvotes, comments, sentiment, title 
        FROM reddit_posts 
        ORDER BY id DESC LIMIT 5
    """)
    reddit_posts = cursor.fetchall()
    
    conn.close()
    
    if not articles:
        print("No articles collected yet. Run news_collector.py to fetch data.")
        return
        
    print("==================================================")
    print("           BTC CONFLUENCE SENTIMENT ANALYSIS      ")
    print("==================================================")
    
    print("\n[RECENT HEADLINES (RSS NEWS)]")
    scores = []
    bearish_count = 0
    bullish_count = 0
    neutral_count = 0
    
    for ts, src, title, score, classification in articles[:10]:
        scores.append(score)
        if classification == "BEARISH":
            bearish_count += 1
            color = "[BEARISH]"
        elif classification == "BULLISH":
            bullish_count += 1
            color = "[BULLISH]"
        else:
            neutral_count += 1
            color = "[NEUTRAL]"
            
        # Clean title for console printing
        clean_title = title.encode('ascii', 'ignore').decode('ascii')
        print(f"- {ts[:16].replace('T', ' ')} | {src} | {color} ({score:+.2f}) - {clean_title[:80]}...")
        
    avg_article_score = sum(scores) / len(scores) if scores else 0.0
    
    # Reddit Social Panel
    print("\n[REDDIT SOCIAL INTELLIGENCE PANEL]")
    if reddit_row:
        r_ts, r_score, r_label, r_posts, r_bull, r_bear, r_neut = reddit_row
        print(f"Timestamp: {r_ts[:16].replace('T', ' ')} UTC")
        print(f"Reddit Sentiment: {r_label} ({r_score:+.3f}) based on {r_posts} posts")
        print(f"Distribution: Bullish={r_bull} | Bearish={r_bear} | Neutral={r_neut}")
        
        if reddit_posts:
            print("Top Trending Social Threads:")
            for sub, up, comm, sent, title in reddit_posts:
                clean_t = title.encode('ascii', 'ignore').decode('ascii')
                print(f"  * {sub} | Up:{up} Comm:{comm} | [{sent.upper()}] - {clean_t[:70]}...")
    else:
        r_score = 0.0
        print("No Reddit sentiment records found. Run news_collector.py first.")

    # Combined Sentiment Calculation
    # We weight News headlines at 40% and Reddit social sentiment at 60%
    weight_news = 0.4
    weight_social = 0.6
    
    if reddit_row:
        unified_score = (avg_article_score * weight_news) + (r_score * weight_social)
    else:
        unified_score = avg_article_score
        
    print("\n[CONFLUENCE SENTIMENT SUMMARY]")
    print(f"News Avg Sentiment Score:  {avg_article_score:+.4f}")
    print(f"Reddit Avg Sentiment Score: {r_score:+.4f}")
    print(f"UNIFIED CONFLUENCE INDEX:  {unified_score:+.4f} (Scale: -1.0 to +1.0)")
    
    # Classify unified state
    if unified_score < -0.4:
        unified_status = "EXTREME_BEARISH"
    elif unified_score < -0.15:
        unified_status = "BEARISH"
    elif unified_score > 0.4:
        unified_status = "EXTREME_BULLISH"
    elif unified_score > 0.15:
        unified_status = "BULLISH"
    else:
        unified_status = "NEUTRAL"
        
    print(f"Unified Sentiment State:   {unified_status}")
    
    print("\n[MACRO CALENDAR OUTLOOK]")
    if next_macro:
        ts_macro, name, impact, status = next_macro
        print(f"Next high-impact announcement: {name}")
        print(f"Scheduled Time: {ts_macro.replace('T', ' ')}")
        
        try:
            event_dt = datetime.fromisoformat(ts_macro)
            now_dt = datetime.now(timezone.utc)
            delta = event_dt - now_dt
            hours_left = delta.total_seconds() / 3600
            print(f"Time remaining: {hours_left:.1f} hours")
        except Exception:
            hours_left = 999.0
    else:
        print("No pending high-impact macro announcements found in calendar.")
        hours_left = 999.0
        
    print("\n[BOT WARNING STATUS & RECOMMENDATIONS]")
    
    warnings = []
    
    # Condition 1: Unified Sentiment State
    if unified_status == "EXTREME_BEARISH":
        warnings.append("[ALERT] EXTREME BEARISH CONFLUENCE DETECTED!")
        warnings.append("  - Financial news and social forums are aligned in panic/selling.")
        warnings.append("  - RECOMMENDATION: Pause all buying operations. Enable halts on pending buy orders.")
    elif unified_status == "BEARISH":
        warnings.append("[WARNING] BEARISH MARKET BIAS DETECTED.")
        warnings.append("  - Combined sentiment has turned negative. Capital outflows / fear dominant.")
        warnings.append("  - RECOMMENDATION: Tighten trailing stop-losses on active longs to lock in profits.")
    elif unified_status == "BULLISH":
        warnings.append("[INFO] BULLISH MARKET BIAS DETECTED.")
        warnings.append("  - News and social indicators show positive momentum and inflows.")
        warnings.append("  - RECOMMENDATION: Normal buy/sell setups permitted.")
    elif unified_status == "EXTREME_BULLISH":
        warnings.append("[INFO] EXTREME BULLISH MARKET MOMENTUM.")
        warnings.append("  - News and forums reporting FOMO and heavy accumulation.")
        warnings.append("  - RECOMMENDATION: Ensure stop-losses are actively trailing in case of volatility pullback.")
        
    # Condition 2: Imminent High-Impact Macro Announcement
    if hours_left <= 24.0:
        warnings.append(f"[ALERT] IMMINENT HIGH-IMPACT MACRO EVENT: {name}")
        warnings.append(f"  - Announcement is within {hours_left:.1f} hours.")
        warnings.append("  - Whipsaw price action and high slippage expected.")
        warnings.append("  - RECOMMENDATION: Temporary halt bot pending orders (set signals['halted'] = True) until 30m after the print.")
    elif hours_left <= 48.0:
        warnings.append(f"[WARNING] UPCOMING HIGH-IMPACT MACRO EVENT: {name}")
        warnings.append(f"  - Announcement is in {hours_left:.1f} hours.")
        warnings.append("  - Monitor open positions closely. Consider reducing RISK_PER_TRADE_PCT by half.")
        
    if not warnings:
        print("STATUS: GREEN. Market confluence is stable. Normal trading operations permitted.")
    else:
        for w in warnings:
            print(w)
                
    print("==================================================")

if __name__ == "__main__":
    analyze_sentiment_trends()
