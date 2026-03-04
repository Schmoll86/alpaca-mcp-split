import os
import re
import sys
import time
import argparse
from datetime import datetime, timedelta, date
from typing import Dict, Any, List, Optional, Union

from dotenv import load_dotenv

from alpaca.common.enums import SupportedCurrencies
from alpaca.common.exceptions import APIError
from alpaca.data.enums import DataFeed, OptionsFeed, CorporateActionsType, CryptoFeed
from alpaca.data.historical.option import OptionHistoricalDataClient
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.historical.corporate_actions import CorporateActionsClient
from alpaca.data.historical.crypto import CryptoHistoricalDataClient
from alpaca.data.historical.news import NewsClient
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import (
    OptionLatestQuoteRequest,
    OptionSnapshotRequest,
    Sort,
    StockBarsRequest,
    StockLatestBarRequest,
    StockLatestQuoteRequest,
    StockLatestTradeRequest,
    StockSnapshotRequest,
    StockTradesRequest,
    StockQuotesRequest,
    OptionChainRequest,
    CorporateActionsRequest,
    CryptoBarsRequest,
    CryptoQuoteRequest,
    CryptoLatestQuoteRequest,
    CryptoLatestBarRequest,
    CryptoLatestTradeRequest,
    CryptoSnapshotRequest,
    CryptoLatestOrderbookRequest,
    CryptoTradesRequest,
    NewsRequest
)

from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import (
    AssetStatus,
    ContractType,
    OrderClass,
    OrderSide,
    OrderType,
    PositionIntent,
    QueryOrderStatus,
    TimeInForce,
)
from alpaca.trading.models import Order
from alpaca.trading.requests import (
    ClosePositionRequest,
    CreateWatchlistRequest,
    GetAssetsRequest,
    GetCalendarRequest,
    GetOptionContractsRequest,
    GetOrdersRequest,
    GetPortfolioHistoryRequest,
    LimitOrderRequest,
    MarketOrderRequest,
    OptionLegRequest,
    StopLimitOrderRequest,
    StopOrderRequest,
    TrailingStopOrderRequest,
    UpdateWatchlistRequest,
)

from mcp.server.fastmcp import FastMCP
import sqlite3
from contextlib import contextmanager

# Configure Python path for local imports
current_dir = os.path.dirname(os.path.abspath(__file__))
github_core_path = os.path.join(current_dir, '.github', 'core')
if github_core_path not in sys.path:
    sys.path.insert(0, github_core_path)
# Import the UserAgentMixin
from user_agent_mixin import UserAgentMixin
# Define new classes using the mixin
class TradingClientSigned(UserAgentMixin, TradingClient): pass
class StockHistoricalDataClientSigned(UserAgentMixin, StockHistoricalDataClient): pass
class OptionHistoricalDataClientSigned(UserAgentMixin, OptionHistoricalDataClient): pass
class CorporateActionsClientSigned(UserAgentMixin, CorporateActionsClient): pass
class CryptoHistoricalDataClientSigned(UserAgentMixin, CryptoHistoricalDataClient): pass

def detect_pycharm_environment():
    """
    Detect if we're running in PyCharm using environment variable.
    Set MCP_CLIENT=pycharm in your PyCharm MCP configuration.
    """
    mcp_client = os.getenv("MCP_CLIENT", "").lower()
    return mcp_client == "pycharm"

def parse_arguments():
    """Parse command line arguments for transport configuration."""
    parser = argparse.ArgumentParser(description="Alpaca MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http", "sse"],
        default="stdio",
        help="Transport method to use (default: stdio). Note: WebSocket not supported, use HTTP for remote connections"
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server to for HTTP/SSE transport (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind the server to for HTTP/SSE transport (default: 8000)"
    )
    return parser.parse_args()

def setup_transport_config(args):
    """Setup transport configuration based on command line arguments."""
    if args.transport == "http":
        return {
            "transport": "http",
            "host": args.host,
            "port": args.port
        }
    elif args.transport == "sse":
        print(f"Warning: SSE transport is deprecated. Consider using HTTP transport instead.")
        return {
            "transport": "sse",
            "host": args.host,
            "port": args.port
        }
    else:
        return {
            "transport": "stdio"
        }

# Default args for when module is imported (not run directly)
class DefaultArgs:
    def __init__(self):
        self.transport = "stdio"
        # host and port are only set when needed (via argument parsing)

# Only parse arguments when running as main script, use defaults when imported
args = DefaultArgs()

# Initialize Alpaca clients using environment variables
# Import our .env file within the same directory
load_dotenv()

TRADE_API_KEY = os.getenv("ALPACA_API_KEY")
TRADE_API_SECRET = os.getenv("ALPACA_SECRET_KEY")
ALPACA_PAPER_TRADE = os.getenv("ALPACA_PAPER_TRADE", "True")
TRADE_API_URL = os.getenv("TRADE_API_URL")
TRDE_API_WSS = os.getenv("TRDE_API_WSS")
DATA_API_URL = os.getenv("DATA_API_URL")
STREAM_DATA_WSS = os.getenv("STREAM_DATA_WSS")
DEBUG = os.getenv("DEBUG", "False")

# Initialize FastMCP server with intelligent log level detection
is_pycharm = detect_pycharm_environment()
log_level = "ERROR" if is_pycharm else "INFO"
log_level = "DEBUG" if DEBUG.lower() == "true" else log_level

# Optional: Print detection result for debugging (only in non-stdio transports)
# Only print when running as main script to avoid noise when imported
# Never print when using stdio transport as it corrupts JSON-RPC communication
if not is_pycharm and __name__ == "__main__" and args.transport != "stdio":
    print(f"MCP Server starting with transport={args.transport}, log_level={log_level} (PyCharm detected: {is_pycharm})")

mcp = FastMCP("alpaca-trading-data", log_level=log_level)

# ----------------------------------------------------------------------------
# Session State Database
# ----------------------------------------------------------------------------
DB_PATH = os.path.join(current_dir, "trading_session.db")

@contextmanager
def get_db():
    """Database connection context manager with WAL mode for concurrent access."""
    conn = sqlite3.connect(DB_PATH, timeout=10.0)
    conn.execute('PRAGMA journal_mode=WAL')  # Allow concurrent reads
    conn.execute('PRAGMA busy_timeout=5000')  # Wait 5 seconds on locks
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

def init_session_db():
    """Initialize the session state database with required tables."""
    with get_db() as conn:
        # Decisions table: Store trading thesis and conclusions
        conn.execute('''
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                decision_type TEXT NOT NULL,
                decision TEXT NOT NULL,
                metadata TEXT,
                timestamp TEXT NOT NULL,
                session_date TEXT NOT NULL,
                UNIQUE(symbol, session_date, decision_type)
            )
        ''')

        # Create index for faster queries
        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_decisions_date_type
            ON decisions(session_date, decision_type)
        ''')

        conn.execute('''
            CREATE INDEX IF NOT EXISTS idx_decisions_symbol
            ON decisions(symbol)
        ''')

# Check if keys are available
if not TRADE_API_KEY or not TRADE_API_SECRET:
    raise ValueError("Alpaca API credentials not found in environment variables.")

# Convert string to boolean
ALPACA_PAPER_TRADE_BOOL = ALPACA_PAPER_TRADE.lower() not in ['false', '0', 'no', 'off']

# Initialize clients
# For trading
trade_client = TradingClientSigned(TRADE_API_KEY, TRADE_API_SECRET, paper=ALPACA_PAPER_TRADE_BOOL)
# For historical market data
stock_historical_data_client = StockHistoricalDataClientSigned(TRADE_API_KEY, TRADE_API_SECRET)
# For streaming market data
stock_data_stream_client = StockDataStream(TRADE_API_KEY, TRADE_API_SECRET, url_override=STREAM_DATA_WSS)
# For option historical data
option_historical_data_client = OptionHistoricalDataClientSigned(api_key=TRADE_API_KEY, secret_key=TRADE_API_SECRET)
# For corporate actions data
corporate_actions_client = CorporateActionsClientSigned(api_key=TRADE_API_KEY, secret_key=TRADE_API_SECRET)
# For crypto historical data
crypto_historical_data_client = CryptoHistoricalDataClientSigned(api_key=TRADE_API_KEY, secret_key=TRADE_API_SECRET)
# For news data (Benzinga via Alpaca)
news_client = NewsClient(api_key=TRADE_API_KEY, secret_key=TRADE_API_SECRET)

# ----------------------------------------------------------------------------
# Centralized date parsing helpers
# ----------------------------------------------------------------------------
def _parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-like datetime string into a datetime.

    Supports:
      - Strings ending with 'Z' by converting to '+00:00'
      - Date-only strings 'YYYY-MM-DD' by assuming midnight
    Returns:
      - datetime when a non-empty valid string is provided
      - None when value is falsy or empty
    Raises:
      - ValueError if a non-empty string is provided but cannot be parsed
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None
    # Allow pure dates
    if re.match(r'^\d{4}-\d{2}-\d{2}$', s):
        s = s + 'T00:00:00'
    s = s.replace('Z', '+00:00')
    try:
        return datetime.fromisoformat(s)
    except Exception as e:
        raise ValueError(f"Invalid ISO datetime: {value}") from e


def _parse_date_ymd(value: str) -> date:
    """Parse 'YYYY-MM-DD' into a date object.
    Raises ValueError on invalid input."""
    return datetime.strptime(value, '%Y-%m-%d').date()


def _month_name_to_number(name: str) -> int:
    """Convert month name to month number. Accepts full and abbreviated names."""
    try:
        return datetime.strptime(name.title(), '%B').month
    except ValueError:
        return datetime.strptime(name.title(), '%b').month

# ============================================================================
# Account Information Tools
# ============================================================================

@mcp.tool()
async def get_account_info() -> str:
    """
    Retrieves and formats the current account information including balances and status.
    
    Returns:
        str: Formatted string containing account details including:
            - Account ID
            - Status
            - Currency
            - Buying Power
            - Cash Balance
            - Portfolio Value
            - Equity
            - Market Values
            - Pattern Day Trader Status
            - Day Trades Remaining
    """
    account = trade_client.get_account()
    
    info = f"""
            Account Information:
            -------------------
            Account ID: {account.id}
            Status: {account.status}
            Currency: {account.currency}
            Buying Power: ${float(account.buying_power):.2f}
            Cash: ${float(account.cash):.2f}
            Portfolio Value: ${float(account.portfolio_value):.2f}
            Equity: ${float(account.equity):.2f}
            Long Market Value: ${float(account.long_market_value):.2f}
            Short Market Value: ${float(account.short_market_value):.2f}
            Pattern Day Trader: {'Yes' if account.pattern_day_trader else 'No'}
            Day Trades Remaining: {account.daytrade_count if hasattr(account, 'daytrade_count') else 'Unknown'}
            """
    return info

@mcp.tool()
async def get_positions() -> str:
    """
    Retrieves and formats all current positions in the portfolio.
    
    Returns:
        str: Formatted string containing details of all open positions including:
            - Symbol
            - Quantity
            - Market Value
            - Average Entry Price
            - Current Price
            - Unrealized P/L
    """
    positions = trade_client.get_all_positions()
    
    if not positions:
        return "No open positions found."
    
    result = "Current Positions:\n-------------------\n"
    for position in positions:
        result += f"""
                    Symbol: {position.symbol}
                    Quantity: {position.qty} shares
                    Market Value: ${float(position.market_value):.2f}
                    Average Entry Price: ${float(position.avg_entry_price):.2f}
                    Current Price: ${float(position.current_price):.2f}
                    Unrealized P/L: ${float(position.unrealized_pl):.2f} ({float(position.unrealized_plpc) * 100:.2f}%)
                    -------------------
                    """
    return result

@mcp.tool()
async def get_open_position(symbol: str) -> str:
    """
    Retrieves and formats details for a specific open position.
    
    Args:
        symbol (str): The symbol name of the asset to get position for (e.g., 'AAPL', 'MSFT')
    
    Returns:
        str: Formatted string containing the position details or an error message
    """
    try:
        position = trade_client.get_open_position(symbol)
        
        # Check if it's an options position by looking for the options symbol pattern
        is_option = len(symbol) > 6 and any(c in symbol for c in ['C', 'P'])
        
        # Format quantity based on asset type
        quantity_text = f"{position.qty} contracts" if is_option else f"{position.qty}"

        return f"""
                Position Details for {symbol}:
                ---------------------------
                Quantity: {quantity_text}
                Market Value: ${float(position.market_value):.2f}
                Average Entry Price: ${float(position.avg_entry_price):.2f}
                Current Price: ${float(position.current_price):.2f}
                Unrealized P/L: ${float(position.unrealized_pl):.2f}
                """ 
    except Exception as e:
        return f"Error fetching position: {str(e)}"

# ============================================================================
# Stock Market Data Tools
# ============================================================================

@mcp.tool()
async def get_stock_quote(symbol: str, feed: DataFeed = DataFeed.SIP) -> str:
    """
    Retrieves and formats the latest quote for a stock.

    Args:
        symbol (str): Stock ticker symbol (e.g., AAPL, MSFT)
        feed (DataFeed): Data feed source. Default: DataFeed.SIP (100% market coverage with Algo Trader Plus)

    Returns:
        str: Formatted string containing:
            - Ask Price
            - Bid Price
            - Ask Size
            - Bid Size
            - Timestamp
    """
    try:
        request_params = StockLatestQuoteRequest(symbol_or_symbols=symbol, feed=feed)
        quotes = stock_historical_data_client.get_stock_latest_quote(request_params)
        
        if symbol in quotes:
            quote = quotes[symbol]
            return f"""
                    Latest Quote for {symbol}:
                    ------------------------
                    Ask Price: ${quote.ask_price:.2f}
                    Bid Price: ${quote.bid_price:.2f}
                    Ask Size: {quote.ask_size}
                    Bid Size: {quote.bid_size}
                    Timestamp: {quote.timestamp}
                    """ 
        else:
            return f"No quote data found for {symbol}."
    except Exception as e:
        return f"Error fetching quote for {symbol}: {str(e)}"

@mcp.tool()
async def get_stock_bars(
    symbol: str,
    days: int = 5,
    timeframe: str = "1Day",
    feed: DataFeed = DataFeed.SIP,
    limit: Optional[int] = None,
    start: Optional[str] = None,
    end: Optional[str] = None
) -> str:
    """
    Retrieves and formats historical price bars for a stock with configurable timeframe and time range.

    Args:
        symbol (str): Stock ticker symbol (e.g., AAPL, MSFT)
        days (int): Number of days to look back (default: 5, ignored if start/end provided)
        timeframe (str): Bar timeframe - supports flexible Alpaca formats:
            - Minutes: "1Min", "2Min", "3Min", "4Min", "5Min", "15Min", "30Min", etc.
            - Hours: "1Hour", "2Hour", "3Hour", "4Hour", "6Hour", etc.
            - Days: "1Day", "2Day", "3Day", etc.
            - Weeks: "1Week", "2Week", etc.
            - Months: "1Month", "2Month", etc.
            (default: "1Day")
        limit (Optional[int]): Maximum number of bars to return (optional)
        start (Optional[str]): Start time in ISO format (e.g., "2023-01-01T09:30:00" or "2023-01-01")
        end (Optional[str]): End time in ISO format (e.g., "2023-01-01T16:00:00" or "2023-01-01")
    
    Returns:
        str: Formatted string containing historical price data with timestamps, OHLCV data
    """
    try:
        # Parse timeframe string to TimeFrame object
        timeframe_obj = parse_timeframe_with_enums(timeframe)
        if timeframe_obj is None:
            return f"Error: Invalid timeframe '{timeframe}'. Supported formats: 1Min, 2Min, 4Min, 5Min, 15Min, 30Min, 1Hour, 2Hour, 4Hour, 1Day, 1Week, 1Month, etc."
        
        # Parse start/end times or calculate from days
        start_time = None
        end_time = None
        
        if start:
            try:
                start_time = _parse_iso_datetime(start)
            except ValueError:
                return f"Error: Invalid start time format '{start}'. Use ISO format like '2023-01-01T09:30:00' or '2023-01-01'"
                
        if end:
            try:
                end_time = _parse_iso_datetime(end)
            except ValueError:
                return f"Error: Invalid end time format '{end}'. Use ISO format like '2023-01-01T16:00:00' or '2023-01-01'"
        
        # If no start/end provided, calculate from days parameter OR limit+timeframe
        if not start_time:
            if limit and timeframe_obj.unit_value in [TimeFrameUnit.Minute, TimeFrameUnit.Hour]:
                # Calculate based on limit and timeframe for intraday data
                if timeframe_obj.unit_value == TimeFrameUnit.Minute:
                    minutes_back = limit * timeframe_obj.amount
                    start_time = datetime.now() - timedelta(minutes=minutes_back)
                elif timeframe_obj.unit_value == TimeFrameUnit.Hour:
                    hours_back = limit * timeframe_obj.amount
                    start_time = datetime.now() - timedelta(hours=hours_back)
            else:
                # Fall back to days parameter for daily+ timeframes
                start_time = datetime.now() - timedelta(days=days)
        if not end_time:
            end_time = datetime.now()
        
        request_params = StockBarsRequest(
            symbol_or_symbols=symbol,
            timeframe=timeframe_obj,
            start=start_time,
            end=end_time,
            limit=limit,
            feed=feed
        )
        
        bars = stock_historical_data_client.get_stock_bars(request_params)
        
        if bars[symbol]:
            time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {end_time.strftime('%Y-%m-%d %H:%M')}"
            result = f"Historical Data for {symbol} ({timeframe} bars, {time_range}):\n"
            result += "---------------------------------------------------\n"
            
            for bar in bars[symbol]:
                # Format timestamp based on timeframe unit
                if timeframe_obj.unit_value in [TimeFrameUnit.Minute, TimeFrameUnit.Hour]:
                    time_str = bar.timestamp.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    time_str = bar.timestamp.date()
                
                result += f"Time: {time_str}, Open: ${bar.open:.2f}, High: ${bar.high:.2f}, Low: ${bar.low:.2f}, Close: ${bar.close:.2f}, Volume: {bar.volume}\n"
            
            return result
        else:
            return f"No historical data found for {symbol} with {timeframe} timeframe in the specified time range."
    except Exception as e:
        return f"Error fetching historical data for {symbol}: {str(e)}"

@mcp.tool()
async def get_stock_trades(
    symbol: str,
    days: int = 5,
    limit: Optional[int] = None,
    sort: Optional[Sort] = Sort.ASC,
    feed: Optional[DataFeed] = None,
    currency: Optional[SupportedCurrencies] = None,
    asof: Optional[str] = None
) -> str:
    """
    Retrieves and formats historical trades for a stock.
    
    Args:
        symbol (str): Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        days (int): Number of days to look back (default: 5)
        limit (Optional[int]): Upper limit of number of data points to return
        sort (Optional[Sort]): Chronological order of response (ASC or DESC)
        feed (Optional[DataFeed]): The stock data feed to retrieve from
        currency (Optional[SupportedCurrencies]): Currency for prices (default: USD)
        asof (Optional[str]): The asof date in YYYY-MM-DD format
    
    Returns:
        str: Formatted string containing trade history or an error message
    """
    try:
        # Calculate start time based on days
        start_time = datetime.now() - timedelta(days=days)
        
        # Create the request object with all available parameters
        request_params = StockTradesRequest(
            symbol_or_symbols=symbol,
            start=start_time,
            end=datetime.now(),
            limit=limit,
            sort=sort,
            feed=feed,
            currency=currency,
            asof=asof
        )
        
        # Get the trades
        trades = stock_historical_data_client.get_stock_trades(request_params)
        
        if symbol in trades:
            result = f"Historical Trades for {symbol} (Last {days} days):\n"
            result += "---------------------------------------------------\n"
            
            for trade in trades[symbol]:
                result += f"""
                    Time: {trade.timestamp}
                    Price: ${float(trade.price):.6f}
                    Size: {trade.size}
                    Exchange: {trade.exchange}
                    ID: {trade.id}
                    Conditions: {trade.conditions}
                    -------------------
                    """
            return result
        else:
            return f"No trade data found for {symbol} in the last {days} days."
    except Exception as e:
        return f"Error fetching trades: {str(e)}"

@mcp.tool()
async def get_stock_latest_trade(
    symbol: str,
    feed: Optional[DataFeed] = None,
    currency: Optional[SupportedCurrencies] = None
) -> str:
    """Get the latest trade for a stock.
    
    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        feed: The stock data feed to retrieve from (optional)
        currency: The currency for prices (optional, defaults to USD)
    
    Returns:
        A formatted string containing the latest trade details or an error message
    """
    try:
        # Create the request object with all available parameters
        request_params = StockLatestTradeRequest(
            symbol_or_symbols=symbol,
            feed=feed,
            currency=currency
        )
        
        # Get the latest trade
        latest_trades = stock_historical_data_client.get_stock_latest_trade(request_params)
        
        if symbol in latest_trades:
            trade = latest_trades[symbol]
            return f"""
                Latest Trade for {symbol}:
                ---------------------------
                Time: {trade.timestamp}
                Price: ${float(trade.price):.6f}
                Size: {trade.size}
                Exchange: {trade.exchange}
                ID: {trade.id}
                Conditions: {trade.conditions}
                """
        else:
            return f"No latest trade data found for {symbol}."
    except Exception as e:
        return f"Error fetching latest trade: {str(e)}"

@mcp.tool()
async def get_stock_latest_bar(
    symbol: str,
    feed: Optional[DataFeed] = None,
    currency: Optional[SupportedCurrencies] = None
) -> str:
    """Get the latest minute bar for a stock.
    
    Args:
        symbol: Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        feed: The stock data feed to retrieve from (optional)
        currency: The currency for prices (optional, defaults to USD)
    
    Returns:
        A formatted string containing the latest bar details or an error message
    """
    try:
        # Create the request object with all available parameters
        request_params = StockLatestBarRequest(
            symbol_or_symbols=symbol,
            feed=feed,
            currency=currency
        )
        
        # Get the latest bar
        latest_bars = stock_historical_data_client.get_stock_latest_bar(request_params)
        
        if symbol in latest_bars:
            bar = latest_bars[symbol]
            return f"""
                Latest Minute Bar for {symbol}:
                ---------------------------
                Time: {bar.timestamp}
                Open: ${float(bar.open):.2f}
                High: ${float(bar.high):.2f}
                Low: ${float(bar.low):.2f}
                Close: ${float(bar.close):.2f}
                Volume: {bar.volume}
                """
        else:
            return f"No latest bar data found for {symbol}."
    except Exception as e:
        return f"Error fetching latest bar: {str(e)}"

# ============================================================================
# Stock Market Data Tools - Stock Snapshot Data with Helper Functions
# ============================================================================

def _format_ohlcv_bar(bar, bar_type: str, include_time: bool = True) -> str:
    """Helper function to format OHLCV bar data consistently."""
    if not bar:
        return ""
    
    time_format = '%Y-%m-%d %H:%M:%S %Z' if include_time else '%Y-%m-%d'
    time_label = "Timestamp" if include_time else "Date"
    
    return f"""{bar_type}:
  Open: ${bar.open:.2f}, High: ${bar.high:.2f}, Low: ${bar.low:.2f}, Close: ${bar.close:.2f}
  Volume: {bar.volume:,}, {time_label}: {bar.timestamp.strftime(time_format)}

"""

def _format_quote_data(quote) -> str:
    """Helper function to format quote data consistently."""
    if not quote:
        return ""
    
    return f"""Latest Quote:
  Bid: ${quote.bid_price:.2f} x {quote.bid_size}, Ask: ${quote.ask_price:.2f} x {quote.ask_size}
  Timestamp: {quote.timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}

"""

def _format_trade_data(trade) -> str:
    """Helper function to format trade data consistently."""
    if not trade:
        return ""
    
    optional_fields = []
    if hasattr(trade, 'exchange') and trade.exchange:
        optional_fields.append(f"Exchange: {trade.exchange}")
    if hasattr(trade, 'conditions') and trade.conditions:
        optional_fields.append(f"Conditions: {trade.conditions}")
    if hasattr(trade, 'id') and trade.id:
        optional_fields.append(f"ID: {trade.id}")
    
    optional_str = f", {', '.join(optional_fields)}" if optional_fields else ""
    
    return f"""Latest Trade:
  Price: ${trade.price:.2f}, Size: {trade.size}{optional_str}
  Timestamp: {trade.timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}

"""

@mcp.tool()
async def get_stock_snapshot(
    symbol_or_symbols: Union[str, List[str]], 
    feed: Optional[DataFeed] = None,
    currency: Optional[SupportedCurrencies] = None
) -> str:
    """
    Retrieves comprehensive snapshots of stock symbols including latest trade, quote, minute bar, daily bar, and previous daily bar.
    
    Args:
        symbol_or_symbols: Single stock symbol or list of stock symbols (e.g., 'AAPL' or ['AAPL', 'MSFT'])
        feed: The stock data feed to retrieve from (optional)
        currency: The currency the data should be returned in (default: USD)
    
    Returns:
        Formatted string with comprehensive snapshots including:
        - latest_quote: Current bid/ask prices and sizes
        - latest_trade: Most recent trade price, size, and exchange
        - minute_bar: Latest minute OHLCV bar
        - daily_bar: Current day's OHLCV bar  
        - previous_daily_bar: Previous trading day's OHLCV bar
    """
    try:
        # Create and execute request
        request = StockSnapshotRequest(symbol_or_symbols=symbol_or_symbols, feed=feed, currency=currency)
        snapshots = stock_historical_data_client.get_stock_snapshot(request)
        
        # Format response
        symbols = [symbol_or_symbols] if isinstance(symbol_or_symbols, str) else symbol_or_symbols
        results = ["Stock Snapshots:", "=" * 15, ""]
        
        for symbol in symbols:
            snapshot = snapshots.get(symbol)
            if not snapshot:
                results.append(f"No data available for {symbol}\n")
                continue
            
            # Build snapshot data using helper functions
            snapshot_data = [
                f"Symbol: {symbol}",
                "-" * 15,
                _format_quote_data(snapshot.latest_quote),
                _format_trade_data(snapshot.latest_trade),
                _format_ohlcv_bar(snapshot.minute_bar, "Latest Minute Bar", True),
                _format_ohlcv_bar(snapshot.daily_bar, "Latest Daily Bar", False),
                _format_ohlcv_bar(snapshot.previous_daily_bar, "Previous Daily Bar", False),
            ]
            
            results.extend(filter(None, snapshot_data))  # Filter out empty strings
        
        return "\n".join(results)
        
    except APIError as api_error:
        error_message = str(api_error)
        # Handle specific data feed subscription errors
        if "subscription" in error_message.lower() and ("sip" in error_message.lower() or "premium" in error_message.lower()):
            return f"""
                    Error: Premium data feed subscription required.

                    The requested data feed requires a premium subscription. Available data feeds:

                    • IEX (Default): Investor's Exchange data feed - Free with basic account
                    • SIP: Securities Information Processor feed - Requires premium subscription
                    • DELAYED_SIP: SIP data with 15-minute delay - Requires premium subscription  
                    • OTC: Over the counter feed - Requires premium subscription

                    Most users can access comprehensive market data using the default IEX feed.
                    To use premium feeds (SIP, DELAYED_SIP, OTC), please upgrade your subscription.

                    Original error: {error_message}
                    """
        else:
            return f"API Error retrieving stock snapshots: {error_message}"
            
    except Exception as e:
        return f"Error retrieving stock snapshots: {str(e)}"
# ============================================================================
# ============================================================================
# Asset Information Tools
# ============================================================================

@mcp.tool()
async def get_asset_info(symbol: str) -> str:
    """
    Retrieves and formats detailed information about a specific asset.
    
    Args:
        symbol (str): The symbol of the asset to get information for
    
    Returns:
        str: Formatted string containing asset details including:
            - Name
            - Exchange
            - Class
            - Status
            - Trading Properties
    """
    try:
        asset = trade_client.get_asset(symbol)
        return f"""
                Asset Information for {symbol}:
                ----------------------------
                Name: {asset.name}
                Exchange: {asset.exchange}
                Class: {asset.asset_class}
                Status: {asset.status}
                Tradable: {'Yes' if asset.tradable else 'No'}
                Marginable: {'Yes' if asset.marginable else 'No'}
                Shortable: {'Yes' if asset.shortable else 'No'}
                Easy to Borrow: {'Yes' if asset.easy_to_borrow else 'No'}
                Fractionable: {'Yes' if asset.fractionable else 'No'}
                """
    except Exception as e:
        return f"Error fetching asset information: {str(e)}"

@mcp.tool()
async def get_all_assets(
    status: Optional[str] = None,
    asset_class: Optional[str] = None,
    exchange: Optional[str] = None,
    attributes: Optional[str] = None
) -> str:
    """
    Get all available assets with optional filtering.
    
    Args:
        status: Filter by asset status (e.g., 'active', 'inactive')
        asset_class: Filter by asset class (e.g., 'us_equity', 'crypto')
        exchange: Filter by exchange (e.g., 'NYSE', 'NASDAQ')
        attributes: Comma-separated values to query for multiple attributes
    """
    try:
        # Create filter if any parameters are provided
        filter_params = None
        if any([status, asset_class, exchange, attributes]):
            filter_params = GetAssetsRequest(
                status=status,
                asset_class=asset_class,
                exchange=exchange,
                attributes=attributes
            )
        
        # Get all assets
        assets = trade_client.get_all_assets(filter_params)
        
        if not assets:
            return "No assets found matching the criteria."
        
        # Format the response
        response_parts = ["Available Assets:"]
        response_parts.append("-" * 30)
        
        for asset in assets:
            response_parts.append(f"Symbol: {asset.symbol}")
            response_parts.append(f"Name: {asset.name}")
            response_parts.append(f"Exchange: {asset.exchange}")
            response_parts.append(f"Class: {asset.asset_class}")
            response_parts.append(f"Status: {asset.status}")
            response_parts.append(f"Tradable: {'Yes' if asset.tradable else 'No'}")
            response_parts.append("-" * 30)
        
        return "\n".join(response_parts)
        
    except Exception as e:
        return f"Error fetching assets: {str(e)}"

# ============================================================================
# Watchlist Management Tools
# ============================================================================

@mcp.tool()
async def create_watchlist(name: str, symbols: List[str]) -> str:
    """
    Creates a new watchlist with specified symbols.
    
    Args:
        name (str): Name of the watchlist
        symbols (List[str]): List of symbols to include in the watchlist
    
    Returns:
        str: Confirmation message with watchlist creation status
    """
    try:
        watchlist_data = CreateWatchlistRequest(name=name, symbols=symbols)
        watchlist = trade_client.create_watchlist(watchlist_data)
        return f"Watchlist '{name}' created successfully with {len(symbols)} symbols."
    except Exception as e:
        return f"Error creating watchlist: {str(e)}"

@mcp.tool()
async def get_watchlists() -> str:
    """
    Get all watchlists for the account with their symbols.

    Returns:
        str: Formatted list of watchlists with their symbols and metadata
    """
    try:
        watchlists = trade_client.get_watchlists()

        if not watchlists:
            return "No watchlists found. Create one with create_watchlist()."

        result = "Watchlists:\n" + "="*50 + "\n"

        for wl in watchlists:
            # Fetch full watchlist details to get assets/symbols
            full_wl = trade_client.get_watchlist_by_id(wl.id)

            result += f"\n📋 {full_wl.name}\n"
            result += f"   ID: {full_wl.id}\n"
            result += f"   Created: {full_wl.created_at.strftime('%Y-%m-%d %H:%M') if full_wl.created_at else 'N/A'}\n"
            result += f"   Updated: {full_wl.updated_at.strftime('%Y-%m-%d %H:%M') if full_wl.updated_at else 'N/A'}\n"

            # Extract symbols from assets
            if full_wl.assets:
                symbols = [asset.symbol for asset in full_wl.assets]
                result += f"   Symbols ({len(symbols)}): {', '.join(symbols)}\n"
            else:
                result += f"   Symbols: (empty)\n"

            result += "\n"

        return result
    except Exception as e:
        return f"Error fetching watchlists: {str(e)}"

@mcp.tool()
async def update_watchlist(watchlist_id: str, name: str = None, symbols: List[str] = None) -> str:
    """Update an existing watchlist."""
    try:
        update_request = UpdateWatchlistRequest(name=name, symbols=symbols)
        watchlist = trade_client.update_watchlist_by_id(watchlist_id, update_request)
        return f"Watchlist updated successfully: {watchlist.name}"
    except Exception as e:
        return f"Error updating watchlist: {str(e)}"

@mcp.tool()
async def get_watchlist_by_id(watchlist_id: str) -> str:
    """
    Get a specific watchlist by its ID with full details including symbols.
    
    Args:
        watchlist_id (str): The UUID of the watchlist
    
    Returns:
        str: Formatted string containing watchlist details and symbols
    """
    try:
        watchlist = trade_client.get_watchlist_by_id(watchlist_id)
        
        result = f"Watchlist: {watchlist.name}\n"
        result += "=" * 50 + "\n"
        result += f"ID: {watchlist.id}\n"
        result += f"Account ID: {watchlist.account_id}\n"
        result += f"Created: {watchlist.created_at.strftime('%Y-%m-%d %H:%M') if watchlist.created_at else 'N/A'}\n"
        result += f"Updated: {watchlist.updated_at.strftime('%Y-%m-%d %H:%M') if watchlist.updated_at else 'N/A'}\n\n"
        
        if watchlist.assets:
            result += f"Symbols ({len(watchlist.assets)}):\n"
            result += "-" * 30 + "\n"
            for asset in watchlist.assets:
                result += f"  {asset.symbol} - {asset.name}\n"
                result += f"    Exchange: {asset.exchange}, Class: {asset.asset_class}\n"
                result += f"    Tradable: {asset.tradable}, Shortable: {asset.shortable}\n"
        else:
            result += "Symbols: (empty)\n"
        
        return result
    except Exception as e:
        return f"Error fetching watchlist: {str(e)}"

@mcp.tool()
async def add_asset_to_watchlist_by_id(watchlist_id: str, symbol: str) -> str:
    """
    Add an asset to a watchlist by the watchlist ID.
    
    Args:
        watchlist_id (str): The UUID of the watchlist
        symbol (str): The symbol to add (e.g., 'AAPL', 'MSFT')
    
    Returns:
        str: Confirmation message with updated watchlist
    """
    try:
        watchlist = trade_client.add_asset_to_watchlist_by_id(watchlist_id, symbol)
        
        symbols = [asset.symbol for asset in watchlist.assets] if watchlist.assets else []
        
        return f"✓ Added {symbol} to watchlist '{watchlist.name}'\n\nCurrent symbols ({len(symbols)}): {', '.join(symbols)}"
    except Exception as e:
        return f"Error adding {symbol} to watchlist: {str(e)}"

@mcp.tool()
async def remove_asset_from_watchlist_by_id(watchlist_id: str, symbol: str) -> str:
    """
    Remove an asset from a watchlist by the watchlist ID.
    
    Args:
        watchlist_id (str): The UUID of the watchlist
        symbol (str): The symbol to remove (e.g., 'AAPL', 'MSFT')
    
    Returns:
        str: Confirmation message with updated watchlist
    """
    try:
        watchlist = trade_client.remove_asset_from_watchlist_by_id(watchlist_id, symbol)
        
        symbols = [asset.symbol for asset in watchlist.assets] if watchlist.assets else []
        
        return f"✓ Removed {symbol} from watchlist '{watchlist.name}'\n\nRemaining symbols ({len(symbols)}): {', '.join(symbols) if symbols else '(empty)'}"
    except Exception as e:
        return f"Error removing {symbol} from watchlist: {str(e)}"

@mcp.tool()
async def delete_watchlist_by_id(watchlist_id: str) -> str:
    """
    Delete a watchlist by its ID.
    
    WARNING: This action is permanent and cannot be undone.
    
    Args:
        watchlist_id (str): The UUID of the watchlist to delete
    
    Returns:
        str: Confirmation message
    """
    try:
        # First get watchlist name for confirmation message
        watchlist = trade_client.get_watchlist_by_id(watchlist_id)
        name = watchlist.name
        
        # Delete the watchlist
        trade_client.delete_watchlist_by_id(watchlist_id)
        
        return f"✓ Deleted watchlist '{name}' (ID: {watchlist_id})"
    except Exception as e:
        return f"Error deleting watchlist: {str(e)}"

# ============================================================================
# Market Information Tools
# ============================================================================

@mcp.tool()
async def get_market_clock() -> str:
    """
    Retrieves and formats current market status and next open/close times.
    
    Returns:
        str: Formatted string containing:
            - Current Time
            - Market Open Status
            - Next Open Time
            - Next Close Time
    """
    try:
        clock = trade_client.get_clock()
        return f"""
                Market Status:
                -------------
                Current Time: {clock.timestamp}
                Is Open: {'Yes' if clock.is_open else 'No'}
                Next Open: {clock.next_open}
                Next Close: {clock.next_close}
                """
    except Exception as e:
        return f"Error fetching market clock: {str(e)}"

@mcp.tool()
async def get_market_calendar(start_date: str, end_date: str) -> str:
    """
    Retrieves and formats market calendar for specified date range.
    
    Args:
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
    
    Returns:
        str: Formatted string containing market calendar information
    """
    try:
        # Convert string dates to date objects
        start_dt = _parse_date_ymd(start_date)
        end_dt = _parse_date_ymd(end_date)
        
        # Create the request object with the correct parameters
        calendar_request = GetCalendarRequest(start=start_dt, end=end_dt)
        calendar = trade_client.get_calendar(calendar_request)
        
        result = f"Market Calendar ({start_date} to {end_date}):\n----------------------------\n"
        for day in calendar:
            result += f"Date: {day.date}, Open: {day.open}, Close: {day.close}\n"
        return result
    except Exception as e:
        return f"Error fetching market calendar: {str(e)}"

# ============================================================================
# Portfolio Tools
# ============================================================================

@mcp.tool()
async def get_portfolio_history(
    period: Optional[str] = None,
    timeframe: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    intraday_reporting: Optional[str] = None,
    pnl_reset: Optional[str] = None,
    date_end: Optional[str] = None,
    extended_hours: Optional[bool] = None
) -> str:
    """
    Retrieves historical portfolio equity and profit/loss data over time.
    
    Args:
        period (Optional[str]): Time period for the data. Valid values: 
            '1D', '1W', '1M', '3M', '6M', '1A', 'all', 'intraday'
        timeframe (Optional[str]): Resolution of time window. Valid values:
            '1Min', '5Min', '15Min', '1H', '1D'
        start (Optional[str]): Start date in YYYY-MM-DD format
        end (Optional[str]): End date in YYYY-MM-DD format
        intraday_reporting (Optional[str]): 'market_hours' or 'extended_hours' or 'continuous'
        pnl_reset (Optional[str]): 'per_day' or 'no_reset' for profit_loss calculation
        date_end (Optional[str]): End of trading day (only for period='intraday')
        extended_hours (Optional[bool]): Include extended hours data
    
    Returns:
        str: Formatted string containing portfolio history with equity, P/L values
    
    Examples:
        get_portfolio_history(period="1M", timeframe="1D")  # Last month, daily bars
        get_portfolio_history(period="1W", timeframe="1H")  # Last week, hourly bars
    """
    try:
        # Parse start/end dates if provided
        start_dt = None
        end_dt = None
        date_end_dt = None
        
        if start:
            try:
                start_dt = _parse_iso_datetime(start)
            except ValueError:
                return f"Error: Invalid start date format '{start}'. Use YYYY-MM-DD or ISO format."
        
        if end:
            try:
                end_dt = _parse_iso_datetime(end)
            except ValueError:
                return f"Error: Invalid end date format '{end}'. Use YYYY-MM-DD or ISO format."
        
        if date_end:
            try:
                date_end_dt = _parse_iso_datetime(date_end)
            except ValueError:
                return f"Error: Invalid date_end format '{date_end}'. Use YYYY-MM-DD or ISO format."
        
        # Build request
        request_kwargs = {}
        if period:
            request_kwargs['period'] = period
        if timeframe:
            request_kwargs['timeframe'] = timeframe
        if start_dt:
            request_kwargs['start'] = start_dt
        if end_dt:
            request_kwargs['end'] = end_dt
        if intraday_reporting:
            request_kwargs['intraday_reporting'] = intraday_reporting
        if pnl_reset:
            request_kwargs['pnl_reset'] = pnl_reset
        if date_end_dt:
            request_kwargs['date_end'] = date_end_dt
        if extended_hours is not None:
            request_kwargs['extended_hours'] = extended_hours
        
        request = GetPortfolioHistoryRequest(**request_kwargs)
        history = trade_client.get_portfolio_history(request)
        
        if not history or not history.timestamp:
            return "No portfolio history data found for the specified criteria."
        
        # Format output
        result = "Portfolio History:\n" + "=" * 60 + "\n"
        if period:
            result += f"Period: {period}\n"
        if timeframe:
            result += f"Timeframe: {timeframe}\n"
        if history.base_value:
            result += f"Base Value: ${float(history.base_value):.2f}\n"
        result += "\n"
        
        result += f"{'Timestamp':<25} {'Equity':>15} {'P/L':>15} {'P/L %':>10}\n"
        result += "-" * 70 + "\n"
        
        timestamps = history.timestamp
        equity = history.equity
        profit_loss = history.profit_loss
        profit_loss_pct = history.profit_loss_pct
        
        for i in range(len(timestamps)):
            ts = datetime.fromtimestamp(timestamps[i]).strftime('%Y-%m-%d %H:%M')
            eq = f"${equity[i]:,.2f}" if equity and i < len(equity) and equity[i] else "N/A"
            pl = f"${profit_loss[i]:,.2f}" if profit_loss and i < len(profit_loss) and profit_loss[i] else "N/A"
            pl_pct = f"{profit_loss_pct[i]*100:.2f}%" if profit_loss_pct and i < len(profit_loss_pct) and profit_loss_pct[i] else "N/A"
            result += f"{ts:<25} {eq:>15} {pl:>15} {pl_pct:>10}\n"
        
        result += "\n" + "=" * 60 + f"\nTotal Data Points: {len(timestamps)}\n"
        
        if equity and len(equity) > 1:
            start_eq = equity[0] if equity[0] else 0
            end_eq = equity[-1] if equity[-1] else 0
            change = end_eq - start_eq
            change_pct = (change / start_eq * 100) if start_eq else 0
            result += f"Equity Change: ${change:,.2f} ({change_pct:.2f}%)\n"
        
        return result
    except Exception as e:
        return f"Error fetching portfolio history: {str(e)}"

# ============================================================================
# Corporate Actions Tools
# ============================================================================

@mcp.tool()
async def get_corporate_announcements(
    ca_types: Optional[List[CorporateActionsType]] = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    symbols: Optional[List[str]] = None,
    cusips: Optional[List[str]] = None,
    ids: Optional[List[str]] = None,
    limit: Optional[int] = 1000,
    sort: Optional[str] = "asc"
) -> str:
    """
    Retrieves and formats corporate action announcements.
    
    Args:
        ca_types (Optional[List[CorporateActionsType]]): List of corporate action types to filter by (default: all types)
            Available types from https://alpaca.markets/sdks/python/api_reference/data/enums.html#corporateactionstype:
            - CorporateActionsType.REVERSE_SPLIT: Reverse split
            - CorporateActionsType.FORWARD_SPLIT: Forward split  
            - CorporateActionsType.UNIT_SPLIT: Unit split
            - CorporateActionsType.CASH_DIVIDEND: Cash dividend
            - CorporateActionsType.STOCK_DIVIDEND: Stock dividend
            - CorporateActionsType.SPIN_OFF: Spin off
            - CorporateActionsType.CASH_MERGER: Cash merger
            - CorporateActionsType.STOCK_MERGER: Stock merger
            - CorporateActionsType.STOCK_AND_CASH_MERGER: Stock and cash merger
            - CorporateActionsType.REDEMPTION: Redemption
            - CorporateActionsType.NAME_CHANGE: Name change
            - CorporateActionsType.WORTHLESS_REMOVAL: Worthless removal
            - CorporateActionsType.RIGHTS_DISTRIBUTION: Rights distribution
        start (Optional[date]): Start date for the announcements (default: current day)
        end (Optional[date]): End date for the announcements (default: current day)
        symbols (Optional[List[str]]): Optional list of stock symbols to filter by
        cusips (Optional[List[str]]): Optional list of CUSIPs to filter by
        ids (Optional[List[str]]): Optional list of corporate action IDs (mutually exclusive with other filters)
        limit (Optional[int]): Maximum number of results to return (default: 1000)
        sort (Optional[str]): Sort order (asc or desc, default: asc)
    
    Returns:
        str: Formatted string containing corporate announcement details
        
    References:
        - API Documentation: https://docs.alpaca.markets/reference/corporateactions-1
        - CorporateActionsType Enum: https://alpaca.markets/sdks/python/api_reference/data/enums.html#corporateactionstype
        - CorporateActionsRequest: https://alpaca.markets/sdks/python/api_reference/data/corporate_actions/requests.html#corporateactionsrequest
    """
    try:
        request = CorporateActionsRequest(
            symbols=symbols,
            cusips=cusips,
            types=ca_types,
            start=start,
            end=end,
            ids=ids,
            limit=limit,
            sort=sort
        )
        announcements = corporate_actions_client.get_corporate_actions(request)
        
        if not announcements or not announcements.data:
            return "No corporate announcements found for the specified criteria."
        
        result = "Corporate Announcements:\n----------------------\n"
        
        # The response.data contains action types as keys (e.g., 'cash_dividends', 'forward_splits')
        # Each value is a list of corporate actions
        for action_type, actions_list in announcements.data.items():
            if not actions_list:
                continue
                
            result += f"\n{action_type.replace('_', ' ').title()}:\n"
            result += "=" * 30 + "\n"
            
            for action in actions_list:
                # Group by symbol for better organization
                symbol = getattr(action, 'symbol', 'Unknown')
                result += f"\nSymbol: {symbol}\n"
                result += "-" * 15 + "\n"
                
                # Display action details based on available attributes
                if hasattr(action, 'corporate_action_type'):
                    result += f"Type: {action.corporate_action_type}\n"
                
                if hasattr(action, 'ex_date') and action.ex_date:
                    result += f"Ex Date: {action.ex_date}\n"
                    
                if hasattr(action, 'record_date') and action.record_date:
                    result += f"Record Date: {action.record_date}\n"
                    
                if hasattr(action, 'payable_date') and action.payable_date:
                    result += f"Payable Date: {action.payable_date}\n"
                    
                if hasattr(action, 'process_date') and action.process_date:
                    result += f"Process Date: {action.process_date}\n"
                
                # Cash dividend specific fields
                if hasattr(action, 'rate') and action.rate:
                    result += f"Rate: ${action.rate:.6f}\n"
                    
                if hasattr(action, 'foreign') and hasattr(action, 'special'):
                    result += f"Foreign: {action.foreign}, Special: {action.special}\n"
                
                # Split specific fields
                if hasattr(action, 'old_rate') and action.old_rate:
                    result += f"Old Rate: {action.old_rate}\n"
                    
                if hasattr(action, 'new_rate') and action.new_rate:
                    result += f"New Rate: {action.new_rate}\n"
                
                # Due bill dates
                if hasattr(action, 'due_bill_on_date') and action.due_bill_on_date:
                    result += f"Due Bill On Date: {action.due_bill_on_date}\n"
                    
                if hasattr(action, 'due_bill_off_date') and action.due_bill_off_date:
                    result += f"Due Bill Off Date: {action.due_bill_off_date}\n"
                
                result += "\n"
        return result
    except Exception as e:
        return f"Error fetching corporate announcements: {str(e)}"


# ============================================================================
# Options Trading Helper Functions
# ============================================================================

def _parse_expiration_expression(expression: str) -> Dict[str, Any]:
    """
    Parse natural language expiration expressions into date parameters.
    
    Args:
        expression (str): Natural language expression like "week of September 7, 2025"
    
    Returns:
        Dict[str, Any]: Parsed parameters or error message
    """
    import re
    from datetime import datetime, timedelta
    
    expression = expression.strip().lower()
    
    # Pattern for "week of [date]"
    week_pattern = r'week\s+of\s+(\w+)\s+(\d{1,2}),?\s+(\d{4})'
    week_match = re.search(week_pattern, expression)
    
    if week_match:
        month_name, day_str, year_str = week_match.groups()
        try:
            # Parse the date
            month_num = _month_name_to_number(month_name)
            day = int(day_str)
            year = int(year_str)
            
            # Create the anchor date
            anchor_date = datetime(year, month_num, day).date()
            
            # Calculate the week range (Monday to Friday trading days)
            # Find the Monday of the week containing the anchor date
            days_since_monday = anchor_date.weekday()  # Monday=0, Sunday=6
            week_start = anchor_date - timedelta(days=days_since_monday)  # Go to Monday
            week_end = week_start + timedelta(days=4)  # Friday
            
            return {
                'expiration_date_gte': week_start,
                'expiration_date_lte': week_end,
                'description': f"week of {month_name.title()} {day}, {year}"
            }
            
        except (ValueError, AttributeError) as e:
            return {'error': f"Invalid date in expression: {str(e)}"}
    
    # Pattern for "month of [month] [year]"
    month_pattern = r'month\s+of\s+(\w+)\s+(\d{4})'
    month_match = re.search(month_pattern, expression)
    
    if month_match:
        month_name, year_str = month_match.groups()
        try:
            month_num = _month_name_to_number(month_name)
            year = int(year_str)
            
            start_date = datetime(year, month_num, 1).date()
            if month_num == 12:
                end_date = datetime(year + 1, 1, 1).date() - timedelta(days=1)
            else:
                end_date = datetime(year, month_num + 1, 1).date() - timedelta(days=1)

            return {
                'expiration_date_gte': start_date,
                'expiration_date_lte': end_date,
                'description': f"month of {month_name.title()} {year}"
            }
            
        except (ValueError, AttributeError) as e:
            return {'error': f"Invalid month/year in expression: {str(e)}"}
    
    # Pattern for specific date like "September 7, 2025"
    date_pattern = r'(\w+)\s+(\d{1,2}),?\s+(\d{4})'
    date_match = re.search(date_pattern, expression)
    
    if date_match:
        month_name, day_str, year_str = date_match.groups()
        try:
            month_num = _month_name_to_number(month_name)
            day = int(day_str)
            year = int(year_str)
            
            specific_date = datetime(year, month_num, day).date()
            
            return {
                'expiration_date': specific_date,
                'description': f"{month_name.title()} {day}, {year}"
            }
            
        except (ValueError, AttributeError) as e:
            return {'error': f"Invalid date in expression: {str(e)}"}
    
    return {'error': f"Unable to parse expression '{expression}'. Supported formats: 'week of September 7, 2025', 'month of December 2025', 'September 7, 2025'"}

# ============================================================================
# Options Trading Tools
# ============================================================================

@mcp.tool()
async def get_option_contracts(
    underlying_symbol: str,
    expiration_date: Optional[date] = None,
    expiration_date_gte: Optional[date] = None,
    expiration_date_lte: Optional[date] = None,
    expiration_expression: Optional[str] = None,
    strike_price_gte: Optional[str] = None,
    strike_price_lte: Optional[str] = None,
    type: Optional[ContractType] = None,
    status: Optional[AssetStatus] = None,
    root_symbol: Optional[str] = None,
    limit: Optional[int] = None
) -> str:
    """
    Retrieves option contracts - direct mapping to GetOptionContractsRequest.
    
    Args:
        underlying_symbol (str): Underlying asset symbol (e.g., 'SPY', 'AAPL')
        expiration_date (Optional[date]): Specific expiration date
        expiration_date_gte (Optional[date]): Expiration date greater than or equal to
        expiration_date_lte (Optional[date]): Expiration date less than or equal to
        expiration_expression (Optional[str]): Natural language (e.g., "week of September 2, 2025")
        strike_price_gte/lte (Optional[str]): Strike price range
        type (Optional[ContractType]): "call" or "put"
        status (Optional[AssetStatus]): "active" (default)
        root_symbol (Optional[str]): Root symbol filter
        limit (Optional[int]): Maximum number of contracts to return
    
    Examples:
        get_option_contracts("NVDA", expiration_expression="week of September 2, 2025")
        get_option_contracts("SPY", expiration_date_gte=date(2025,9,1), expiration_date_lte=date(2025,9,5))
    """
    try:
        # Handle natural language expression
        if expiration_expression:
            parsed = _parse_expiration_expression(expiration_expression)
            if parsed.get('error'):
                return f"Error: {parsed['error']}"
            
            # Map parsed results directly to API parameters
            if 'expiration_date' in parsed:
                expiration_date = parsed['expiration_date']
            elif 'expiration_date_gte' in parsed:
                expiration_date_gte = parsed['expiration_date_gte']
                expiration_date_lte = parsed['expiration_date_lte']
        
        # Create API request - direct mapping like your baseline example
        request = GetOptionContractsRequest(
            underlying_symbols=[underlying_symbol],
            expiration_date=expiration_date,
            expiration_date_gte=expiration_date_gte,
            expiration_date_lte=expiration_date_lte,
            strike_price_gte=strike_price_gte,
            strike_price_lte=strike_price_lte,
            type=type,
            status=status,
            root_symbol=root_symbol,
            limit=limit
        )
        
        # Execute API call
        response = trade_client.get_option_contracts(request)
        
        if not response or not response.option_contracts:
            return f"No option contracts found for {underlying_symbol}."
        
        # Format results
        contracts = response.option_contracts
        result = [f"Option Contracts for {underlying_symbol}:", "=" * 50]

        for contract in contracts:  # Show ALL contracts returned by API
            contract_type = "Call" if contract.type == ContractType.CALL else "Put"
            result.extend([
                f"ID: {contract.id}",
                f"Symbol: {contract.symbol}",
                f"  Name: {contract.name}",
                f"  Type: {contract_type}",
                f"  Strike: ${contract.strike_price}",
                f"  Expiration: {contract.expiration_date}",
                f"  Style: {contract.style}",
                f"  Contract Size: {contract.size}",
                f"  Open Interest: {contract.open_interest or 'N/A'}",
                f"  Open Interest Date: {contract.open_interest_date or 'N/A'}",
                f"  Close Price: ${contract.close_price or 'N/A'}",
                f"  Close Price Date: {contract.close_price_date or 'N/A'}",
                f"  Tradable: {contract.tradable}",
                f"  Status: {contract.status}",
                f"  Root Symbol: {contract.root_symbol}",
                f"  Underlying Asset ID: {contract.underlying_asset_id}",
                f"  Underlying Symbol: {contract.underlying_symbol}",
                "-" * 40
            ])

        result.append(f"\nTotal: {len(contracts)} contracts")
        return "\n".join(result)
        
    except Exception as e:
        return f"Error: {str(e)}"

@mcp.tool()
async def get_option_latest_quote(
    symbol: str,
    feed: OptionsFeed = OptionsFeed.OPRA
) -> str:
    """
    Retrieves and formats the latest quote for an option contract. This endpoint returns real-time
    pricing and market data, including bid/ask prices, sizes, and exchange information.

    Args:
        symbol (str): The option contract symbol (e.g., 'AAPL230616C00150000')
        feed (OptionsFeed): The source feed of the data.
            Default: OptionsFeed.OPRA (real-time with Algo Trader Plus subscription)
    
    Returns:
        str: Formatted string containing the latest quote information including:
            - Ask Price and Ask Size
            - Bid Price and Bid Size
            - Ask Exchange and Bid Exchange
            - Trade Conditions
            - Tape Information
            - Timestamp (in UTC)
    
    Note:
        This endpoint returns real-time market data. For contract specifications and static data,
        use get_option_contracts instead.
    """
    try:
        # Create the request object
        request = OptionLatestQuoteRequest(
            symbol_or_symbols=symbol,
            feed=feed
        )
        
        # Get the latest quote
        quotes = option_historical_data_client.get_option_latest_quote(request)
        
        if symbol in quotes:
            quote = quotes[symbol]
            return f"""
                Latest Quote for {symbol}:
                ------------------------
                Ask Price: ${float(quote.ask_price):.2f}
                Ask Size: {quote.ask_size}
                Ask Exchange: {quote.ask_exchange}
                Bid Price: ${float(quote.bid_price):.2f}
                Bid Size: {quote.bid_size}
                Bid Exchange: {quote.bid_exchange}
                Conditions: {quote.conditions}
                Tape: {quote.tape}
                Timestamp: {quote.timestamp}
                """
        else:
            return f"No quote data found for {symbol}."
            
    except Exception as e:
        return f"Error fetching option quote: {str(e)}"


@mcp.tool()
async def get_option_snapshot(symbol_or_symbols: Union[str, List[str]], feed: OptionsFeed = OptionsFeed.OPRA) -> str:
    """
    Retrieves comprehensive snapshots of option contracts including latest trade, quote, implied volatility, and Greeks.
    This endpoint provides a complete view of an option's current market state and theoretical values.
    
    Args:
        symbol_or_symbols (Union[str, List[str]]): Single option symbol or list of option symbols
            (e.g., 'AAPL250613P00205000')
        feed (Optional[OptionsFeed]): The source feed of the data (opra or indicative).
            Default: opra if the user has the options subscription, indicative otherwise.
    
    Returns:
        str: Formatted string containing a comprehensive snapshot including:
            - Symbol Information
            - Latest Quote:
                * Bid/Ask Prices and Sizes
                * Exchange Information
                * Trade Conditions
                * Tape Information
                * Timestamp (UTC)
            - Latest Trade:
                * Price and Size
                * Exchange and Conditions
                * Trade ID
                * Timestamp (UTC)
            - Implied Volatility (as percentage)
            - Greeks:
                * Delta (directional risk)
                * Gamma (delta sensitivity)
                * Rho (interest rate sensitivity)
                * Theta (time decay)
                * Vega (volatility sensitivity)
    """
    try:
        # Create snapshot request
        request = OptionSnapshotRequest(
            symbol_or_symbols=symbol_or_symbols,
            feed=feed
        )
        
        # Get snapshots
        snapshots = option_historical_data_client.get_option_snapshot(request)
        
        # Format the response
        result = "Option Snapshots:\n"
        result += "================\n\n"
        
        # Handle both single symbol and list of symbols
        symbols = [symbol_or_symbols] if isinstance(symbol_or_symbols, str) else symbol_or_symbols
        
        for symbol in symbols:
            snapshot = snapshots.get(symbol)
            if snapshot is None:
                result += f"No data available for {symbol}\n"
                continue
                
            result += f"Symbol: {symbol}\n"
            result += "-----------------\n"
            
            # Latest Quote
            if snapshot.latest_quote:
                quote = snapshot.latest_quote
                result += f"Latest Quote:\n"
                result += f"  Bid Price: ${quote.bid_price:.6f}\n"
                result += f"  Bid Size: {quote.bid_size}\n"
                result += f"  Bid Exchange: {quote.bid_exchange}\n"
                result += f"  Ask Price: ${quote.ask_price:.6f}\n"
                result += f"  Ask Size: {quote.ask_size}\n"
                result += f"  Ask Exchange: {quote.ask_exchange}\n"
                if quote.conditions:
                    result += f"  Conditions: {quote.conditions}\n"
                if quote.tape:
                    result += f"  Tape: {quote.tape}\n"
                result += f"  Timestamp: {quote.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}\n"
            
            # Latest Trade
            if snapshot.latest_trade:
                trade = snapshot.latest_trade
                result += f"Latest Trade:\n"
                result += f"  Price: ${trade.price:.6f}\n"
                result += f"  Size: {trade.size}\n"
                if trade.exchange:
                    result += f"  Exchange: {trade.exchange}\n"
                if trade.conditions:
                    result += f"  Conditions: {trade.conditions}\n"
                if trade.tape:
                    result += f"  Tape: {trade.tape}\n"
                if trade.id:
                    result += f"  Trade ID: {trade.id}\n"
                result += f"  Timestamp: {trade.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f %Z')}\n"
            
            # Implied Volatility
            if snapshot.implied_volatility is not None:
                result += f"Implied Volatility: {snapshot.implied_volatility:.2%}\n"
            
            # Greeks
            if snapshot.greeks:
                greeks = snapshot.greeks
                result += f"Greeks:\n"
                result += f"  Delta: {greeks.delta:.4f}\n"
                result += f"  Gamma: {greeks.gamma:.4f}\n"
                result += f"  Rho: {greeks.rho:.4f}\n"
                result += f"  Theta: {greeks.theta:.4f}\n"
                result += f"  Vega: {greeks.vega:.4f}\n"
            
            result += "\n"
        
        return result
        
    except Exception as e:
        return f"Error retrieving option snapshots: {str(e)}"


@mcp.tool()
async def get_option_chain(
    underlying_symbol: str,
    expiration_date_gte: Optional[str] = None,
    expiration_date_lte: Optional[str] = None,
    strike_price_gte: Optional[float] = None,
    strike_price_lte: Optional[float] = None,
    option_type: Optional[str] = None,
    feed: str = "indicative"
) -> str:
    """
    Retrieves complete option chain with Greeks, implied volatility, and pricing for an underlying symbol.

    Returns full chain data including:
    - Latest quote (bid/ask/size)
    - Latest trade (price/size)
    - Greeks (delta, gamma, theta, vega, rho)
    - Implied Volatility

    IMPORTANT: Always use filters to avoid overwhelming results. A full SPY chain has 1000+ contracts.

    Args:
        underlying_symbol (str): Underlying stock symbol (e.g., 'SPY', 'AAPL', 'NVDA')
        expiration_date_gte (Optional[str]): Minimum expiration date in YYYY-MM-DD format
        expiration_date_lte (Optional[str]): Maximum expiration date in YYYY-MM-DD format
        strike_price_gte (Optional[float]): Minimum strike price (e.g., 550.0)
        strike_price_lte (Optional[float]): Maximum strike price (e.g., 560.0)
        option_type (Optional[str]): Option type: 'call' or 'put'
        feed (str): Data feed - use 'indicative' for delayed (15min) data or 'opra' for real-time
                   (requires Algo Trader Plus subscription). Default: 'indicative'

    Examples:
        get_option_chain("SPY", expiration_date_gte="2025-10-10", expiration_date_lte="2025-10-17",
                        strike_price_gte=550.0, strike_price_lte=560.0, option_type="call")
        get_option_chain("NVDA", expiration_date_gte="2025-10-15", strike_price_gte=130.0,
                        strike_price_lte=140.0, option_type="put", feed="opra")

    Returns:
        str: Formatted string containing complete option chain with all market data
    """
    try:
        # Convert feed string to enum
        feed_enum = OptionsFeed.OPRA if feed.lower() == "opra" else OptionsFeed.INDICATIVE

        # Convert option_type string to enum if provided
        type_enum = None
        if option_type:
            type_enum = ContractType.CALL if option_type.lower() == "call" else ContractType.PUT

        # Build request with filters
        request = OptionChainRequest(
            underlying_symbol=underlying_symbol,
            feed=feed_enum
        )

        # Add optional filters
        if expiration_date_gte:
            request.expiration_date_gte = datetime.strptime(expiration_date_gte, '%Y-%m-%d').date()
        if expiration_date_lte:
            request.expiration_date_lte = datetime.strptime(expiration_date_lte, '%Y-%m-%d').date()
        if strike_price_gte is not None:
            request.strike_price_gte = strike_price_gte
        if strike_price_lte is not None:
            request.strike_price_lte = strike_price_lte
        if type_enum:
            request.type = type_enum

        # Execute request using global client
        chain_data = option_historical_data_client.get_option_chain(request)

        # Format output
        output = []
        output.append(f"Option Chain for {underlying_symbol}")
        output.append(f"Feed: {feed_enum.value}")
        output.append(f"Contracts Retrieved: {len(chain_data)}")

        if expiration_date_gte or expiration_date_lte:
            output.append(f"Expiration Filter: {expiration_date_gte or 'N/A'} to {expiration_date_lte or 'N/A'}")
        if strike_price_gte or strike_price_lte:
            output.append(f"Strike Filter: ${strike_price_gte or 'N/A'} to ${strike_price_lte or 'N/A'}")
        if option_type:
            output.append(f"Type: {option_type.upper()}S")

        output.append("\n" + "="*80 + "\n")

        if len(chain_data) == 0:
            output.append("No contracts found matching filters.")
            return "\n".join(output)

        # Sort by expiration then strike
        # Just sort by symbol name (OCC symbols sort naturally by expiration+strike)
        sorted_contracts = sorted(chain_data.items(), key=lambda x: x[0])

        # Format each contract
        for symbol, snapshot in sorted_contracts:
            output.append(f"\nSymbol: {symbol}")

            # Parse contract details from OCC symbol
            try:
                exp_str = symbol[len(underlying_symbol):len(underlying_symbol)+6]
                exp_date = datetime.strptime(exp_str, '%y%m%d').strftime('%Y-%m-%d')
                output.append(f"  Expiration: {exp_date}")

                cp_index = symbol.index('C') if 'C' in symbol[len(underlying_symbol)+6:] else symbol.index('P')
                strike_raw = symbol[cp_index+1:cp_index+9]
                strike = float(strike_raw) / 1000
                output.append(f"  Strike: ${strike:.2f}")

                opt_type = "CALL" if 'C' in symbol[len(underlying_symbol)+6:] else "PUT"
                output.append(f"  Type: {opt_type}")
            except:
                pass

            # Latest Quote
            if snapshot.latest_quote:
                q = snapshot.latest_quote
                output.append(f"  Bid: ${q.bid_price:.2f} x {q.bid_size}")
                output.append(f"  Ask: ${q.ask_price:.2f} x {q.ask_size}")
                mid = (q.bid_price + q.ask_price) / 2
                output.append(f"  Mid: ${mid:.2f}")
                spread = q.ask_price - q.bid_price
                spread_pct = (spread / mid * 100) if mid > 0 else 0
                output.append(f"  Spread: ${spread:.2f} ({spread_pct:.2f}%)")

            # Latest Trade
            if snapshot.latest_trade:
                t = snapshot.latest_trade
                output.append(f"  Last: ${t.price:.2f} ({t.size} contracts)")

            # Greeks
            if snapshot.greeks:
                g = snapshot.greeks
                output.append(f"  Delta: {g.delta:.4f}")
                output.append(f"  Gamma: {g.gamma:.4f}")
                output.append(f"  Theta: {g.theta:.4f}")
                output.append(f"  Vega: {g.vega:.4f}")
                output.append(f"  Rho: {g.rho:.4f}")

            # Implied Volatility
            if snapshot.implied_volatility:
                iv_pct = snapshot.implied_volatility * 100
                output.append(f"  IV: {iv_pct:.2f}%")

            output.append("")

        # Summary statistics
        output.append("="*80)
        output.append("\nChain Summary:")

        if len(chain_data) > 0:
            ivs = [s.implied_volatility for s in chain_data.values() if s.implied_volatility]
            if ivs:
                avg_iv = sum(ivs) / len(ivs)
                output.append(f"  Average IV: {avg_iv*100:.2f}%")

        return "\n".join(output)

    except APIError as e:
        error_msg = str(e)
        # Detect subscription/permission issues
        if "subscription" in error_msg.lower() or "permission" in error_msg.lower() or "forbidden" in error_msg.lower():
            return f"❌ Subscription Error: {error_msg}\n\n💡 Solution:\n" \
                   f"- You're trying to use feed='{feed}' which requires Algo Trader Plus\n" \
                   f"- Use feed='indicative' for delayed (15min) options data\n" \
                   f"- Or upgrade at: https://alpaca.markets/pricing\n\n" \
                   f"Example: get_option_chain(\"{underlying_symbol}\", ..., feed=\"indicative\")"
        else:
            return f"❌ API Error: {error_msg}\n\nTroubleshooting:\n" \
                   f"- Verify symbol '{underlying_symbol}' exists and has options\n" \
                   f"- Check date format is YYYY-MM-DD\n" \
                   f"- Ensure strike prices are numeric\n" \
                   f"- Try feed='indicative' for delayed data"
    except Exception as e:
        return f"❌ Error retrieving option chain: {str(e)}\n\nPlease check your parameters and try again."

# ============================================================================
# News Data Tools (Benzinga via Alpaca)
# ============================================================================

@mcp.tool()
async def get_news(
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 5,
    sort: str = "desc",
    include_content: bool = False
) -> str:
    """
    Retrieves Benzinga news articles for specified symbols.

    Available with Algo Trader Plus subscription. Provides:
    - Headlines and summaries
    - Author and publication time
    - Related symbols
    - Article URLs

    CONTEXT MANAGEMENT: Default limit is 5 articles to avoid flooding. Increase only when needed.

    Args:
        symbols (Optional[List[str]]): List of stock symbols (e.g., ['AAPL', 'MSFT']). Leave empty for all news.
        start (Optional[str]): Start date in YYYY-MM-DD format or ISO datetime
        end (Optional[str]): End date in YYYY-MM-DD format or ISO datetime
        limit (int): Number of articles to return (default: 5, max: 50)
        sort (str): Sort order by publication time (desc = newest first, asc = oldest first)
        include_content (bool): Include full article content. WARNING: Significantly increases context usage.

    Use Cases:
        - Check recent catalyst: get_news(symbols=["NVDA"], limit=3)
        - Pre-market scan: get_news(symbols=["SPY", "QQQ"], limit=5)
        - Event research: get_news(symbols=["TSLA"], start="2025-10-01", end="2025-10-02", limit=10)

    Returns:
        str: Formatted string containing news articles with headlines, summaries, and URLs
    """
    try:
        # Convert date strings to datetime if provided
        start_dt = None
        end_dt = None
        if start:
            try:
                start_dt = datetime.strptime(start, '%Y-%m-%d')
            except ValueError:
                start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))

        if end:
            try:
                end_dt = datetime.strptime(end, '%Y-%m-%d')
            except ValueError:
                end_dt = datetime.fromisoformat(end.replace('Z', '+00:00'))

        # Build request - symbols must be comma-separated string, not list
        symbols_str = ','.join(symbols) if symbols else None

        request = NewsRequest(
            symbols=symbols_str,
            start=start_dt,
            end=end_dt,
            limit=limit,
            sort=sort
        )

        # Fetch news using global client
        news_data = news_client.get_news(request)

        # Convert NewsSet to list - NewsSet.data is Dict[str, List[News]]
        articles = list(news_data.data.get('news', [])) if hasattr(news_data, 'data') else []

        # Format output
        output = []
        output.append("Benzinga News Articles")
        output.append("="*80)

        if symbols:
            output.append(f"Symbols: {', '.join(symbols)}")
        if start:
            output.append(f"Date Range: {start} to {end or 'now'}")
        output.append(f"Results: {len(articles)} articles (limit: {limit})")
        output.append("")

        if len(articles) == 0:
            output.append("No news articles found matching criteria.")
            return "\n".join(output)

        # Format each article
        for i, article in enumerate(articles, 1):
            output.append(f"\n[{i}] {article.headline}")
            output.append(f"    Author: {article.author}")
            output.append(f"    Published: {article.created_at}")
            output.append(f"    Updated: {article.updated_at}")

            if article.symbols:
                output.append(f"    Symbols: {', '.join(article.symbols)}")

            # Summary (always include - usually 1-2 sentences)
            if article.summary:
                summary = article.summary[:300] + "..." if len(article.summary) > 300 else article.summary
                output.append(f"    Summary: {summary}")

            # Full content (only if explicitly requested)
            if include_content and article.content:
                clean_content = re.sub('<[^<]+?>', '', article.content)
                clean_content = clean_content[:500] + "..." if len(clean_content) > 500 else clean_content
                output.append(f"    Content: {clean_content}")

            # URL for full article
            output.append(f"    URL: {article.url}")
            output.append("")

        # Warning if limit was reached
        if len(articles) == limit:
            output.append(f"\n⚠️  Result limit ({limit}) reached. More articles may be available.")
            output.append("Consider narrowing date range or increasing limit (max: 50).")

        return "\n".join(output)

    except Exception as e:
        return f"Error retrieving news: {str(e)}\n\nTroubleshooting:\n- Check date format (YYYY-MM-DD)\n- Verify symbol format\n- Ensure limit is between 1-50"


@mcp.tool()
async def get_latest_news(symbol: str) -> str:
    """
    Quick lookup for the most recent news article for a symbol.
    Optimized for fast catalyst checks without flooding context.

    Returns single most recent article with:
    - Headline
    - Summary
    - Publication time
    - URL

    Perfect for:
    - "What's the latest on NVDA?"
    - "Any recent news on SPY?"
    - Quick pre-trade catalyst check

    Args:
        symbol (str): Stock symbol (e.g., 'AAPL')

    Returns:
        str: Formatted string containing the most recent news article
    """
    try:
        # symbols must be comma-separated string, not list
        request = NewsRequest(
            symbols=symbol,
            limit=1,
            sort="desc"
        )

        news_data = news_client.get_news(request)

        # Convert NewsSet to list
        articles = list(news_data.data.get('news', [])) if hasattr(news_data, 'data') else []

        if len(articles) == 0:
            return f"No recent news found for {symbol}"

        article = articles[0]

        output = []
        output.append(f"Latest News for {symbol}")
        output.append("="*80)
        output.append(f"Headline: {article.headline}")
        output.append(f"Published: {article.created_at}")
        output.append(f"Author: {article.author}")

        if article.summary:
            output.append(f"\nSummary: {article.summary}")

        output.append(f"\nRead more: {article.url}")

        return "\n".join(output)

    except Exception as e:
        return f"Error retrieving latest news for {symbol}: {str(e)}"

# Helper Functions and Utilities
# ============================================================================
# The following functions are internal helper functions used by the MCP tools
# for data parsing, validation, formatting, and other utility operations.
# ============================================================================

def parse_timeframe_with_enums(timeframe_str: str) -> Optional[TimeFrame]:
    """
    Parse timeframe string to Alpaca TimeFrame object using proper enumerations.
    Supports standard Alpaca formats and common natural language variations.
    
    Args:
        timeframe_str (str): Timeframe string (e.g., "1Min", "30 mins", "1 hour", "daily")
        
    Returns:
        Optional[TimeFrame]: Parsed TimeFrame object using TimeFrameUnit enums or None if invalid
        
    Reference:
        https://alpaca.markets/sdks/python/api_reference/data/timeframe.html#timeframeunit
    """
    
    try:
        if not timeframe_str or not isinstance(timeframe_str, str):
            return None
            
        timeframe_str = timeframe_str.strip()
        if not timeframe_str:
            return None
        
        # Use predefined TimeFrame objects for common cases (most efficient)
        predefined_timeframes = {
            "1Min": TimeFrame.Minute,
            "1Hour": TimeFrame.Hour, 
            "1Day": TimeFrame.Day,
            "1Week": TimeFrame.Week,
            "1Month": TimeFrame.Month
        }
        
        if timeframe_str in predefined_timeframes:
            return predefined_timeframes[timeframe_str]
        
        # Normalize input for flexible parsing
        normalized = re.sub(r'\s+', ' ', timeframe_str.lower().strip())
        
        # Common expressions that map directly to timeframes
        direct_mappings = {
            'half hour': (30, TimeFrameUnit.Minute),
            'quarter hour': (15, TimeFrameUnit.Minute),
            'hourly': (1, TimeFrameUnit.Hour),
            'daily': (1, TimeFrameUnit.Day),
            'weekly': (1, TimeFrameUnit.Week),
            'monthly': (1, TimeFrameUnit.Month)
        }
        
        if normalized in direct_mappings:
            amount, unit = direct_mappings[normalized]
            return TimeFrame(amount, unit)
        
        # Comprehensive pattern to handle most variations
        # Matches: number + unit (with optional spaces, hyphens, plurals)
        pattern = r'^(\d+)\s*[-\s]*\s*(min|minute|minutes|hr|hour|hours|day|days|week|weeks|month|months)s?$'
        match = re.match(pattern, normalized)
        
        if match:
            amount = int(match.group(1))
            unit_str = match.group(2)
            
            # Map unit strings to TimeFrameUnit enums
            unit_mapping = {
                'min': TimeFrameUnit.Minute, 'minute': TimeFrameUnit.Minute, 'minutes': TimeFrameUnit.Minute,
                'hr': TimeFrameUnit.Hour, 'hour': TimeFrameUnit.Hour, 'hours': TimeFrameUnit.Hour,
                'day': TimeFrameUnit.Day, 'days': TimeFrameUnit.Day,
                'week': TimeFrameUnit.Week, 'weeks': TimeFrameUnit.Week,
                'month': TimeFrameUnit.Month, 'months': TimeFrameUnit.Month
            }
            
            unit = unit_mapping.get(unit_str)
            if unit and _validate_amount(amount, unit):
                return TimeFrame(amount, unit)
        
        # Try case-insensitive standard Alpaca formats
        alpaca_pattern = r'^(\d+)(min|hour|day|week|month)s?$'
        match = re.match(alpaca_pattern, normalized)
        
        if match:
            amount = int(match.group(1))
            unit_str = match.group(2)
            
            unit_mapping = {
                'min': TimeFrameUnit.Minute,
                'hour': TimeFrameUnit.Hour,
                'day': TimeFrameUnit.Day,
                'week': TimeFrameUnit.Week,
                'month': TimeFrameUnit.Month
            }
            
            unit = unit_mapping.get(unit_str)
            if unit and _validate_amount(amount, unit):
                return TimeFrame(amount, unit)
            
        return None
        
    except (ValueError, AttributeError, TypeError):
        return None


def _validate_amount(amount: int, unit: TimeFrameUnit) -> bool:
    """
    Validate that the amount is reasonable for the given unit.
    """
    if amount <= 0:
        return False
        
    if unit == TimeFrameUnit.Minute and amount > 59:
        return False
    elif unit == TimeFrameUnit.Hour and amount > 23:
        return False
    elif unit in [TimeFrameUnit.Day, TimeFrameUnit.Week, TimeFrameUnit.Month] and amount > 365:
        return False
        
    return True


# ----------------------------------------------------------------------------
# Session State Management Tools
# ----------------------------------------------------------------------------

@mcp.tool()
async def get_session_state(
    symbol: Optional[str] = None,
    decision_type: Optional[str] = None,
    days: int = 1
) -> str:
    """
    Retrieve trading decisions with optional filtering.

    Call this at the START of each Claude session to restore your trading
    context, analysis, and plans. This prevents wasting tokens by re-analyzing
    symbols you've already researched.

    Args:
        symbol: Filter by specific ticker (e.g., 'NVDA'). Optional.
        decision_type: Filter by type - 'analysis', 'alert', 'risk', or 'note'. Optional.
        days: Number of days to look back (default: 1 = today only)

    Returns:
        Formatted list of decisions matching the filters

    Example usage:
        - get_session_state() → All decisions from today
        - get_session_state(symbol="NVDA") → All NVDA decisions from today
        - get_session_state(decision_type="analysis", days=7) → All analysis from past week
        - get_session_state(symbol="SPY", decision_type="alert") → SPY alerts from today
    """
    import json

    try:
        with get_db() as conn:
            # Build dynamic query based on filters
            query = '''
                SELECT symbol, decision_type, decision, metadata, timestamp, session_date
                FROM decisions
                WHERE session_date >= date('now', '-' || ? || ' days')
            '''
            params: List[Union[int, str]] = [days - 1]  # -1 because 'now' includes today

            if symbol:
                query += ' AND symbol = ?'
                params.append(symbol.upper().strip())

            if decision_type:
                decision_type_clean = decision_type.lower().strip()
                valid_types = ['analysis', 'alert', 'risk', 'note']
                if decision_type_clean not in valid_types:
                    return f"❌ Invalid decision_type '{decision_type}'. Must be one of: {', '.join(valid_types)}"
                query += ' AND decision_type = ?'
                params.append(decision_type_clean)

            query += ' ORDER BY timestamp DESC'

            rows = conn.execute(query, params).fetchall()

        if not rows:
            filters = []
            if symbol:
                filters.append(f"symbol={symbol}")
            if decision_type:
                filters.append(f"type={decision_type}")
            if days != 1:
                filters.append(f"{days} days")

            filter_str = f" ({', '.join(filters)})" if filters else ""
            return f"📋 No decisions found{filter_str}.\n\nUse record_decision() to save your analysis."

        # Format output
        filter_info = ""
        if symbol or decision_type or days != 1:
            parts = []
            if symbol:
                parts.append(f"Symbol: {symbol}")
            if decision_type:
                parts.append(f"Type: {decision_type}")
            if days != 1:
                parts.append(f"Days: {days}")
            filter_info = f" (Filters: {', '.join(parts)})"

        result = f"📋 Trading Decisions{filter_info}:\n" + "="*50 + "\n"

        for sym, dtype, dec, meta, ts, _ in rows:
            time_str = datetime.fromisoformat(ts).strftime("%Y-%m-%d %H:%M")
            result += f"\n{sym} [{dtype}] @ {time_str}\n  └─ {dec}"

            # Display metadata if present
            if meta:
                try:
                    meta_obj = json.loads(meta)
                    # Handle double-encoded JSON strings (defensive parsing)
                    if isinstance(meta_obj, str):
                        meta_obj = json.loads(meta_obj)

                    # Now format as key=value pairs
                    if isinstance(meta_obj, dict):
                        meta_str = ', '.join([f"{k}={v}" for k, v in meta_obj.items()])
                        result += f"\n  └─ {meta_str}"
                    else:
                        result += f"\n  └─ {meta_obj}"
                except (json.JSONDecodeError, AttributeError, TypeError) as e:
                    # Fallback: just display raw metadata
                    result += f"\n  └─ {meta}"

            result += "\n"

        return result

    except Exception as e:
        return f"❌ Error retrieving session state: {str(e)}"

# ============================================================================
# ADDITIONAL FUNCTIONS - Stock Quotes (Historical)
# ============================================================================

@mcp.tool()
async def get_stock_quotes(
    symbol: str,
    days: int = 1,
    hours: int = 0,
    minutes: int = 15,
    limit: int = 1000,
    sort: Sort = Sort.ASC,
    feed: Optional[DataFeed] = None,
    currency: Optional[SupportedCurrencies] = None,
    asof: Optional[str] = None
) -> str:
    """
    Retrieves historical quote data (bid/ask) for a stock.
    
    Args:
        symbol (str): Stock ticker symbol (e.g., 'AAPL', 'MSFT')
        days (int): Number of days to look back (default: 1)
        hours (int): Additional hours to look back (default: 0)
        minutes (int): Additional minutes to look back (default: 15)
        limit (int): Maximum number of quotes to return (default: 1000)
        sort (Sort): Chronological order of response (ASC or DESC)
        feed (Optional[DataFeed]): The stock data feed to retrieve from
        currency (Optional[SupportedCurrencies]): Currency for prices (default: USD)
        asof (Optional[str]): The asof date in YYYY-MM-DD format
    
    Returns:
        str: Formatted string containing historical quote data with bid/ask prices and sizes
    """
    try:
        start_time = datetime.now() - timedelta(days=days, hours=hours, minutes=minutes)
        
        request_params = StockQuotesRequest(
            symbol_or_symbols=symbol,
            start=start_time,
            end=datetime.now(),
            limit=limit,
            sort=sort,
            feed=feed,
            currency=currency,
            asof=asof
        )
        
        quotes = stock_historical_data_client.get_stock_quotes(request_params)
        
        if symbol in quotes and quotes[symbol]:
            time_range = f"{start_time.strftime('%Y-%m-%d %H:%M')} to {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            result = f"Historical Quotes for {symbol} ({time_range}):\n"
            result += "=" * 80 + "\n"
            result += f"{'Timestamp':<25} {'Bid':>12} {'Bid Size':>10} {'Ask':>12} {'Ask Size':>10}\n"
            result += "-" * 80 + "\n"
            
            for quote in quotes[symbol]:
                ts = quote.timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                result += f"{ts:<25} ${quote.bid_price:>10.4f} {quote.bid_size:>10} ${quote.ask_price:>10.4f} {quote.ask_size:>10}\n"
            
            result += f"\nTotal quotes: {len(quotes[symbol])}"
            return result
        else:
            return f"No historical quote data found for {symbol}."
    except Exception as e:
        return f"Error fetching stock quotes: {str(e)}"


# Run the server
if __name__ == "__main__":
    # Parse command line arguments when running as main script
    args = parse_arguments()

    # Initialize session state database
    init_session_db()

    # Setup transport configuration based on command line arguments
    transport_config = setup_transport_config(args)

    try:
        # Run server with the specified transport
        if args.transport == "http":
            mcp.settings.host = transport_config["host"]
            mcp.settings.port = transport_config["port"]
            mcp.run(transport="streamable-http")
        elif args.transport == "sse":
            mcp.settings.host = transport_config["host"]
            mcp.settings.port = transport_config["port"]
            mcp.run(transport="sse")
        else:
            mcp.run(transport="stdio")
    except Exception as e:
        if args.transport in ["http", "sse"]:
            print(f"Error starting {args.transport} server: {e}")
            print(f"Server was configured to run on {transport_config['host']}:{transport_config['port']}")
            print("Common solutions:")
            print(f"1. Ensure port {transport_config['port']} is available")
            print(f"2. Check if another service is using port {transport_config['port']}")
            print("3. Try using a different port with --port <PORT>")
            print("4. For remote access, consider using SSH tunneling or reverse proxy")
        else:
            print(f"Error starting MCP server: {e}")
        sys.exit(1)
