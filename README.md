# Alpaca MCP Split Server

**Two MCP servers for Alpaca trading — split by purpose to work within Claude's tool limits.**

## The Problem

Claude Desktop (and claude.ai) has a **hard limit on how many MCP tools it will load** per conversation. When a single MCP server exposes 40+ tools, Claude silently drops many of them. You end up with a powerful server that only partially works — often missing the exact tools you need most (like order placement or options chains).

There's no error message. Tools just don't appear. You might have `get_account_info`, `get_positions`, `place_stock_order`, `get_option_chain`, and 35 other tools in your server — but Claude only loads ~12-15 of them, seemingly at random.

## The Solution

Split one large server into two smaller ones organized by purpose:

| Server | Tools | Purpose |
|--------|-------|---------|
| **alpaca-trading-data** | 30 | All read operations: quotes, positions, options chains, news, watchlists, market data |
| **alpaca-trading-orders** | 9 | All execution: place orders, cancel orders, close positions, exercise options |

Claude loads tools from **all connected MCP servers** into one flat pool. Two servers with 30 and 9 tools each both stay under the limit, giving you access to all 39 tools in every conversation.

Both servers connect to the **same Alpaca account** using the same API keys. There's no state isolation — they're just two entry points to the same brokerage account.

## What Was Removed

All **crypto trading tools** (9 tools) were removed from the original monolith to further reduce tool count. If you need crypto, you can add them back to the data server from the original source.

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/Schmoll86/alpaca-mcp-split.git
cd alpaca-mcp-split

# Create a shared virtual environment
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure API keys

Copy the example env file into **both** server directories:

```bash
cp alpaca-trading-data/.env.example alpaca-trading-data/.env
cp alpaca-trading-orders/.env.example alpaca-trading-orders/.env
```

Edit both `.env` files with your Alpaca API credentials. You can get these from [Alpaca Dashboard](https://app.alpaca.markets/paper/dashboard/overview).

### 3. Configure Claude Desktop

Add both servers to your `claude_desktop_config.json`:

**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`  
**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "alpaca-trading-data": {
      "command": "/path/to/alpaca-mcp-split/venv/bin/python",
      "args": [
        "/path/to/alpaca-mcp-split/alpaca-trading-data/alpaca_trading_data.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    },
    "alpaca-trading-orders": {
      "command": "/path/to/alpaca-mcp-split/venv/bin/python",
      "args": [
        "/path/to/alpaca-mcp-split/alpaca-trading-orders/alpaca_trading_orders.py"
      ],
      "env": {
        "PYTHONUNBUFFERED": "1"
      }
    }
  }
}
```

Replace `/path/to/alpaca-mcp-split/` with your actual clone path.

### 4. Restart Claude Desktop

Quit and relaunch. Both servers will start automatically. In a new conversation, you should have access to all 39 tools.

## Tool Inventory

### Data Server (30 tools)

**Account & Positions:** `get_account_info`, `get_positions`, `get_open_position`

**Stock Data:** `get_stock_quote`, `get_stock_bars`, `get_stock_trades`, `get_stock_latest_trade`, `get_stock_latest_bar`, `get_stock_snapshot`, `get_stock_quotes`

**Options Data:** `get_option_contracts`, `get_option_latest_quote`, `get_option_snapshot`, `get_option_chain`

**News:** `get_news`, `get_latest_news`

**Market:** `get_market_clock`, `get_market_calendar`

**Watchlists:** `create_watchlist`, `get_watchlists`, `update_watchlist`, `get_watchlist_by_id`, `add_asset_to_watchlist_by_id`, `remove_asset_from_watchlist_by_id`, `delete_watchlist_by_id`

**Other:** `get_asset_info`, `get_all_assets`, `get_portfolio_history`, `get_corporate_announcements`, `get_session_state`

### Orders Server (9 tools)

**Stock Orders:** `place_stock_order`, `get_orders`, `cancel_all_orders`, `cancel_order_by_id`

**Position Management:** `close_position`, `close_all_positions`, `exercise_options_position`

**Options Orders:** `place_option_market_order`

**Session:** `record_decision`

## How It Works

```
┌─────────────────────────────────────────────┐
│              Claude Desktop                  │
│                                              │
│  Tools from ALL connected MCPs appear as     │
│  one flat pool. Claude picks the right       │
│  tool regardless of which server owns it.    │
└──────────┬──────────────┬────────────────────┘
           │              │
    ┌──────▼──────┐ ┌─────▼───────┐
    │  Data MCP   │ │ Orders MCP  │
    │  (30 tools) │ │  (9 tools)  │
    └──────┬──────┘ └─────┬───────┘
           │              │
           └──────┬───────┘
                  │
         ┌────────▼────────┐
         │  Alpaca API     │
         │  (Same account, │
         │   same keys)    │
         └─────────────────┘
```

## Adapting This Pattern

This same split strategy works for any MCP server hitting Claude's tool limit:

1. **Count your tools** — if you have 30+, you're probably getting silently truncated
2. **Split by read vs. write** — safest separation, no functional dependencies
3. **Share the venv** — symlink or use the same virtual environment for both
4. **Same .env** — both servers use identical credentials

## License

MIT
