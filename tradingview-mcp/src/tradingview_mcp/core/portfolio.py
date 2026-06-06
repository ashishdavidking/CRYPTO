import sqlite3
import os
from typing import Dict, List, Optional, Any
from datetime import datetime

# Store the DB in the user's home directory or current directory
DB_DIR = os.path.expanduser("~/.tradingview_mcp_data")
DB_PATH = os.path.join(DB_DIR, "portfolio.db")

def init_db():
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Table for users and their paper trading balance
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        balance REAL NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Table for active positions
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS positions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quantity REAL NOT NULL,
        average_price REAL NOT NULL,
        side TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users (user_id)
    )
    ''')
    
    # Table for trade history/logs
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS trade_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        quantity REAL NOT NULL,
        price REAL NOT NULL,
        side TEXT NOT NULL,  -- 'BUY' or 'SELL'
        realized_pnl REAL DEFAULT 0,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    conn.commit()
    conn.close()

def get_or_create_user(user_id: str, initial_balance: float = 10000.0) -> float:
    """Returns the current balance of the user. Creates the user with 10k if they don't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT balance FROM users WHERE user_id = ?", (user_id,))
    row = cursor.fetchone()
    
    if row is None:
        cursor.execute("INSERT INTO users (user_id, balance) VALUES (?, ?)", (user_id, initial_balance))
        conn.commit()
        balance = initial_balance
    else:
        balance = row[0]
        
    conn.close()
    return balance

def execute_trade(user_id: str, symbol: str, quantity: float, current_price: float, side: str) -> Dict[str, Any]:
    """Execute a simulated trade (BUY or SELL) for a user."""
    symbol = symbol.upper()
    side = side.upper()
    
    if side not in ['BUY', 'SELL']:
        return {"error": "Side must be 'BUY' or 'SELL'"}
        
    if quantity <= 0:
        return {"error": "Quantity must be greater than 0"}

    # Initialize user if they don't exist
    balance = get_or_create_user(user_id)
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        if side == 'BUY':
            cost = quantity * current_price
            if balance < cost:
                return {"error": f"Insufficient balance. Required: ${cost:.2f}, Available: ${balance:.2f}"}
            
            # Deduct balance
            new_balance = balance - cost
            cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
            
            # Check if position exists to average down, else create new
            cursor.execute("SELECT id, quantity, average_price FROM positions WHERE user_id = ? AND symbol = ?", (user_id, symbol))
            pos = cursor.fetchone()
            
            if pos:
                pos_id, existing_qty, existing_avg_price = pos
                new_qty = existing_qty + quantity
                new_avg_price = ((existing_qty * existing_avg_price) + (quantity * current_price)) / new_qty
                cursor.execute("UPDATE positions SET quantity = ?, average_price = ? WHERE id = ?", (new_qty, new_avg_price, pos_id))
            else:
                cursor.execute("INSERT INTO positions (user_id, symbol, quantity, average_price, side) VALUES (?, ?, ?, ?, ?)", 
                               (user_id, symbol, quantity, current_price, "LONG"))
                
            # Log history
            cursor.execute("INSERT INTO trade_history (user_id, symbol, quantity, price, side) VALUES (?, ?, ?, ?, ?)",
                           (user_id, symbol, quantity, current_price, 'BUY'))
            
            conn.commit()
            return {
                "status": "success", 
                "action": "BUY", 
                "symbol": symbol,
                "quantity": quantity,
                "price": current_price,
                "total_cost": cost,
                "remaining_balance": new_balance
            }
            
        elif side == 'SELL':
            # Check position
            cursor.execute("SELECT id, quantity, average_price FROM positions WHERE user_id = ? AND symbol = ?", (user_id, symbol))
            pos = cursor.fetchone()
            
            if not pos:
                return {"error": f"You do not own any {symbol}"}
                
            pos_id, existing_qty, existing_avg_price = pos
            
            if quantity > existing_qty:
                return {"error": f"Cannot sell {quantity} of {symbol}. You only own {existing_qty}."}
                
            revenue = quantity * current_price
            realized_pnl = (current_price - existing_avg_price) * quantity
            
            # Add to balance
            new_balance = balance + revenue
            cursor.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
            
            # Update or remove position
            new_qty = existing_qty - quantity
            if new_qty <= 0.00001:  # Floating point safety
                cursor.execute("DELETE FROM positions WHERE id = ?", (pos_id,))
            else:
                cursor.execute("UPDATE positions SET quantity = ? WHERE id = ?", (new_qty, pos_id))
                
            # Log history
            cursor.execute("INSERT INTO trade_history (user_id, symbol, quantity, price, side, realized_pnl) VALUES (?, ?, ?, ?, ?, ?)",
                           (user_id, symbol, quantity, current_price, 'SELL', realized_pnl))
            
            conn.commit()
            return {
                "status": "success", 
                "action": "SELL", 
                "symbol": symbol,
                "quantity": quantity,
                "price": current_price,
                "revenue": revenue,
                "realized_pnl": realized_pnl,
                "new_balance": new_balance
            }

    except Exception as e:
        conn.rollback()
        return {"error": f"Database error during trade: {str(e)}"}
    finally:
        conn.close()

def get_portfolio(user_id: str) -> Dict[str, Any]:
    """Retrieve the user's current portfolio (balance and open positions)."""
    balance = get_or_create_user(user_id)
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    cursor.execute("SELECT symbol, quantity, average_price FROM positions WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()
    
    positions = []
    for row in rows:
        positions.append({
            "symbol": row["symbol"],
            "quantity": row["quantity"],
            "average_price": row["average_price"]
        })
        
    conn.close()
    
    return {
        "user_id": user_id,
        "balance": balance,
        "positions": positions
    }

# Initialize DB when module is imported
init_db()
