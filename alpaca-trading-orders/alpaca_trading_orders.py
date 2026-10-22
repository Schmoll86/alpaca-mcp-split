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
    ReplaceOrderRequest,
    StopLimitOrderRequest,
    StopLossRequest,
    StopOrderRequest,
    TakeProfitRequest,
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

mcp = FastMCP("alpaca-trading-orders", log_level=log_level)

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

# ============================================================================
# Order Management Tools
# ============================================================================

@mcp.tool()
async def get_orders(
    status: str = "all", 
    limit: int = 10,
    after: Optional[str] = None,
    until: Optional[str] = None,
    direction: Optional[str] = None,
    nested: Optional[bool] = None,
    side: Optional[str] = None,
    symbols: Optional[List[str]] = None
) -> str:
    """
    Retrieves and formats orders with the specified filters.
    
    Args:
        status (str): Order status to filter by (open, closed, all)
        limit (int): Maximum number of orders to return (default: 10, max 500)
        after (Optional[str]): Include orders submitted after this timestamp (ISO format)
        until (Optional[str]): Include orders submitted until this timestamp (ISO format)
        direction (Optional[str]): Chronological order (asc or desc, default: desc)
        nested (Optional[bool]): Roll up multi-leg orders under legs field if True
        side (Optional[str]): Filter by order side (buy or sell)
        symbols (Optional[List[str]]): List of symbols to filter by
    
    Returns:
        str: Formatted string containing order details including:
            - Symbol
            - ID
            - Type
            - Side
            - Quantity
            - Status
            - Submission Time
            - Fill Details (if applicable)
    """
    try:
        # Convert status string to enum
        if status.lower() == "open":
            query_status = QueryOrderStatus.OPEN
        elif status.lower() == "closed":
            query_status = QueryOrderStatus.CLOSED
        else:
            query_status = QueryOrderStatus.ALL
        
        # Convert direction string to enum if provided
        direction_enum = None
        if direction:
            if direction.lower() == "asc":
                direction_enum = Sort.ASC
            elif direction.lower() == "desc":
                direction_enum = Sort.DESC
            else:
                return f"Invalid direction: {direction}. Must be 'asc' or 'desc'."
        
        # Convert side string to enum if provided
        side_enum = None
        if side:
            if side.lower() == "buy":
                side_enum = OrderSide.BUY
            elif side.lower() == "sell":
                side_enum = OrderSide.SELL
            else:
                return f"Invalid side: {side}. Must be 'buy' or 'sell'."
        
        # Parse datetime strings if provided
        after_dt = None
        until_dt = None
        if after:
            try:
                after_dt = _parse_iso_datetime(after)
            except ValueError:
                return f"Invalid 'after' timestamp format: {after}. Use ISO format like '2023-01-01T09:30:00'"
        if until:
            try:
                until_dt = _parse_iso_datetime(until)
            except ValueError:
                return f"Invalid 'until' timestamp format: {until}. Use ISO format like '2023-01-01T16:00:00'"
            
        request_params = GetOrdersRequest(
            status=query_status,
            limit=limit,
            after=after_dt,
            until=until_dt,
            direction=direction_enum,
            nested=nested,
            side=side_enum,
            symbols=symbols
        )
        
        orders = trade_client.get_orders(request_params)
        
        if not orders:
            return f"No {status} orders found."
        
        result = f"{status.capitalize()} Orders (Last {len(orders)}):\n"
        result += "-----------------------------------\n"
        
        for order in orders:
            result += f"Symbol: {order.symbol}\n"
            result += f"ID: {order.id}\n"
            result += f"Type: {order.type}\n"
            result += f"Side: {order.side}\n"
            result += f"Quantity: {order.qty}\n"
            result += f"Status: {order.status}\n"
            result += f"Asset Class: {order.asset_class}\n"
            result += f"Order Class: {order.order_class}\n"
            result += f"Time In Force: {order.time_in_force}\n"
            result += f"Extended Hours: {order.extended_hours}\n"
            result += f"Submitted At: {order.submitted_at}\n"
            result += f"Created At: {order.created_at}\n"
            result += f"Updated At: {order.updated_at}\n"
            
            # Additional core fields (these are optional)
            if hasattr(order, 'asset_id') and order.asset_id:
                result += f"Asset ID: {order.asset_id}\n"
            if hasattr(order, 'order_type') and order.order_type:
                result += f"Order Type: {order.order_type}\n"
            if hasattr(order, 'ratio_qty') and order.ratio_qty:
                result += f"Ratio Quantity: {order.ratio_qty}\n"
            
            # Optional fields that may not always be present
            if hasattr(order, 'filled_at') and order.filled_at:
                result += f"Filled At: {order.filled_at}\n"
            if hasattr(order, 'filled_avg_price') and order.filled_avg_price:
                result += f"Filled Price: ${float(order.filled_avg_price):.2f}\n"
            if hasattr(order, 'filled_qty') and order.filled_qty:
                result += f"Filled Quantity: {order.filled_qty}\n"
            if hasattr(order, 'limit_price') and order.limit_price:
                result += f"Limit Price: ${float(order.limit_price):.2f}\n"
            if hasattr(order, 'stop_price') and order.stop_price:
                result += f"Stop Price: ${float(order.stop_price):.2f}\n"
            if hasattr(order, 'trail_price') and order.trail_price:
                result += f"Trail Price: ${float(order.trail_price):.2f}\n"
            if hasattr(order, 'trail_percent') and order.trail_percent:
                result += f"Trail Percent: {order.trail_percent}%\n"
            if hasattr(order, 'notional') and order.notional:
                result += f"Notional: ${float(order.notional):.2f}\n"
            if hasattr(order, 'position_intent') and order.position_intent:
                result += f"Position Intent: {order.position_intent}\n"
            if hasattr(order, 'client_order_id') and order.client_order_id:
                result += f"Client Order ID: {order.client_order_id}\n"
            if hasattr(order, 'canceled_at') and order.canceled_at:
                result += f"Canceled At: {order.canceled_at}\n"
            if hasattr(order, 'expired_at') and order.expired_at:
                result += f"Expired At: {order.expired_at}\n"
            if hasattr(order, 'expires_at') and order.expires_at:
                result += f"Expires At: {order.expires_at}\n"
            if hasattr(order, 'failed_at') and order.failed_at:
                result += f"Failed At: {order.failed_at}\n"
            if hasattr(order, 'replaced_at') and order.replaced_at:
                result += f"Replaced At: {order.replaced_at}\n"
            if hasattr(order, 'replaced_by') and order.replaced_by:
                result += f"Replaced By: {order.replaced_by}\n"
            if hasattr(order, 'replaces') and order.replaces:
                result += f"Replaces: {order.replaces}\n"
            if hasattr(order, 'legs') and order.legs:
                result += f"Legs: {order.legs}\n"
            if hasattr(order, 'hwm') and order.hwm:
                result += f"HWM: {order.hwm}\n"
                
            result += "-----------------------------------\n"
            
        return result
    except Exception as e:
        return f"Error fetching orders: {str(e)}"

@mcp.tool()
async def place_stock_order(
    symbol: str,
    side: str,
    quantity: float,
    order_type: str = "market",
    time_in_force: str = "day",
    limit_price: float = None,
    stop_price: float = None,
    trail_price: float = None,
    trail_percent: float = None,
    extended_hours: bool = False,
    client_order_id: str = None,
    stop_loss_price: float = None,
    take_profit_price: float = None
) -> str:
    """
    Places an order of any supported type (MARKET, LIMIT, STOP, STOP_LIMIT, TRAILING_STOP) using the correct Alpaca request class.

    Optionally attaches a protective stop and/or profit target atomically (the stop/target live at the
    broker the instant the entry fills — no supervision needed). On a MARKET or LIMIT entry:
      - stop_loss_price only            -> OrderClass.OTO  (entry triggers a protective stop)
      - take_profit_price only          -> OrderClass.OTO  (entry triggers a take-profit limit)
      - both stop_loss_price + take_profit_price -> OrderClass.BRACKET (entry + OCO stop & target)
    Note: advanced (OTO/BRACKET) orders require whole-share quantities and TIF of DAY or GTC.

    Args:
        symbol (str): Stock ticker symbol (e.g., AAPL, MSFT)
        side (str): Order side (buy or sell)
        quantity (float): Number of shares to buy or sell
        order_type (str): Order type (MARKET, LIMIT, STOP, STOP_LIMIT, TRAILING_STOP). Default is MARKET.
        time_in_force (str): Time in force for the order. Valid options for equity trading:
            DAY, GTC, OPG, CLS, IOC, FOK (default: DAY)
        limit_price (float): Limit price (required for LIMIT, STOP_LIMIT)
        stop_price (float): Stop price (required for STOP, STOP_LIMIT)
        trail_price (float): Trail price (for TRAILING_STOP)
        trail_percent (float): Trail percent (for TRAILING_STOP)
        extended_hours (bool): Allow execution during extended hours (default: False)
        client_order_id (str): Optional custom identifier for the order
        stop_loss_price (float): Optional attached protective stop price (MARKET/LIMIT entries only).
        take_profit_price (float): Optional attached take-profit limit price (MARKET/LIMIT entries only).

    Returns:
        str: Formatted string containing order details or error message.
    """
    try:
        # Validate side
        if side.lower() == "buy":
            order_side = OrderSide.BUY
        elif side.lower() == "sell":
            order_side = OrderSide.SELL
        else:
            return f"Invalid order side: {side}. Must be 'buy' or 'sell'."

        # Validate and convert time_in_force to enum
        tif_enum = None
        if isinstance(time_in_force, TimeInForce):
            tif_enum = time_in_force
        elif isinstance(time_in_force, str):
            # Convert string to TimeInForce enum
            time_in_force_upper = time_in_force.upper()
            if time_in_force_upper == "DAY":
                tif_enum = TimeInForce.DAY
            elif time_in_force_upper == "GTC":
                tif_enum = TimeInForce.GTC
            elif time_in_force_upper == "OPG":
                tif_enum = TimeInForce.OPG
            elif time_in_force_upper == "CLS":
                tif_enum = TimeInForce.CLS
            elif time_in_force_upper == "IOC":
                tif_enum = TimeInForce.IOC
            elif time_in_force_upper == "FOK":
                tif_enum = TimeInForce.FOK
            else:
                return f"Invalid time_in_force: {time_in_force}. Valid options are: DAY, GTC, OPG, CLS, IOC, FOK"
        else:
            return f"Invalid time_in_force type: {type(time_in_force)}. Must be string or TimeInForce enum."

        # Validate order_type
        order_type_upper = order_type.upper()

        # Optional atomic protective legs (OTO / BRACKET). Only valid on MARKET/LIMIT entries.
        adv_kwargs = {}
        if stop_loss_price is not None or take_profit_price is not None:
            if order_type_upper not in ("MARKET", "LIMIT"):
                return ("stop_loss_price/take_profit_price are only supported on MARKET or LIMIT "
                        "entries (they build an OTO/BRACKET). Got order_type=" + order_type + ".")
            if stop_loss_price is not None and take_profit_price is not None:
                adv_kwargs["order_class"] = OrderClass.BRACKET
                adv_kwargs["stop_loss"] = StopLossRequest(stop_price=stop_loss_price)
                adv_kwargs["take_profit"] = TakeProfitRequest(limit_price=take_profit_price)
            elif stop_loss_price is not None:
                adv_kwargs["order_class"] = OrderClass.OTO
                adv_kwargs["stop_loss"] = StopLossRequest(stop_price=stop_loss_price)
            else:
                adv_kwargs["order_class"] = OrderClass.OTO
                adv_kwargs["take_profit"] = TakeProfitRequest(limit_price=take_profit_price)

        if order_type_upper == "MARKET":
            order_data = MarketOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                type=OrderType.MARKET,
                time_in_force=tif_enum,
                extended_hours=extended_hours,
                client_order_id=client_order_id or f"order_{int(time.time())}",
                **adv_kwargs
            )
        elif order_type_upper == "LIMIT":
            if limit_price is None:
                return "limit_price is required for LIMIT orders."
            order_data = LimitOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                type=OrderType.LIMIT,
                time_in_force=tif_enum,
                limit_price=limit_price,
                extended_hours=extended_hours,
                client_order_id=client_order_id or f"order_{int(time.time())}",
                **adv_kwargs
            )
        elif order_type_upper == "STOP":
            if stop_price is None:
                return "stop_price is required for STOP orders."
            order_data = StopOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                type=OrderType.STOP,
                time_in_force=tif_enum,
                stop_price=stop_price,
                extended_hours=extended_hours,
                client_order_id=client_order_id or f"order_{int(time.time())}"
            )
        elif order_type_upper == "STOP_LIMIT":
            if stop_price is None or limit_price is None:
                return "Both stop_price and limit_price are required for STOP_LIMIT orders."
            order_data = StopLimitOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                type=OrderType.STOP_LIMIT,
                time_in_force=tif_enum,
                stop_price=stop_price,
                limit_price=limit_price,
                extended_hours=extended_hours,
                client_order_id=client_order_id or f"order_{int(time.time())}"
            )
        elif order_type_upper == "TRAILING_STOP":
            if trail_price is None and trail_percent is None:
                return "Either trail_price or trail_percent is required for TRAILING_STOP orders."
            order_data = TrailingStopOrderRequest(
                symbol=symbol,
                qty=quantity,
                side=order_side,
                type=OrderType.TRAILING_STOP,
                time_in_force=tif_enum,
                trail_price=trail_price,
                trail_percent=trail_percent,
                extended_hours=extended_hours,
                client_order_id=client_order_id or f"order_{int(time.time())}"
            )
        else:
            return f"Invalid order type: {order_type}. Must be one of: MARKET, LIMIT, STOP, STOP_LIMIT, TRAILING_STOP."

        # Submit order
        order = trade_client.submit_order(order_data)
        return f"""
                Stock Order Placed Successfully:
                --------------------------------
                asset_class: {order.asset_class}
                asset_id: {order.asset_id}
                canceled_at: {order.canceled_at}
                client_order_id: {order.client_order_id}
                created_at: {order.created_at}
                expired_at: {order.expired_at}
                expires_at: {order.expires_at}
                extended_hours: {order.extended_hours}
                failed_at: {order.failed_at}
                filled_at: {order.filled_at}
                filled_avg_price: {order.filled_avg_price}
                filled_qty: {order.filled_qty}
                hwm: {order.hwm}
                id: {order.id}
                legs: {order.legs}
                limit_price: {order.limit_price}
                notional: {order.notional}
                order_class: {order.order_class}
                order_type: {order.order_type}
                position_intent: {order.position_intent}
                qty: {order.qty}
                ratio_qty: {order.ratio_qty}
                replaced_at: {order.replaced_at}
                replaced_by: {order.replaced_by}
                replaces: {order.replaces}
                side: {order.side}
                status: {order.status}
                stop_price: {order.stop_price}
                submitted_at: {order.submitted_at}
                symbol: {order.symbol}
                time_in_force: {order.time_in_force}
                trail_percent: {order.trail_percent}
                trail_price: {order.trail_price}
                type: {order.type}
                updated_at: {order.updated_at}
                """
    except Exception as e:
        return f"Error placing order: {str(e)}"
@mcp.tool()
async def cancel_all_orders() -> str:
    """
    Cancel all open orders.
    
    Returns:
        A formatted string containing the status of each cancelled order.
    """
    try:
        # Cancel all orders
        cancel_responses = trade_client.cancel_orders()
        
        if not cancel_responses:
            return "No orders were found to cancel."
        
        # Format the response
        response_parts = ["Order Cancellation Results:"]
        response_parts.append("-" * 30)
        
        for response in cancel_responses:
            status = "Success" if response.status == 200 else "Failed"
            response_parts.append(f"Order ID: {response.id}")
            response_parts.append(f"Status: {status}")
            if response.body:
                response_parts.append(f"Details: {response.body}")
            response_parts.append("-" * 30)
        
        return "\n".join(response_parts)
        
    except Exception as e:
        return f"Error cancelling orders: {str(e)}"

@mcp.tool()
async def cancel_order_by_id(order_id: str) -> str:
    """
    Cancel a specific order by its ID.
    
    Args:
        order_id: The UUID of the order to cancel
        
    Returns:
        A formatted string containing the status of the cancelled order.
    """
    try:
        # Cancel the specific order
        response = trade_client.cancel_order_by_id(order_id)
        
        # Format the response
        status = "Success" if response.status == 200 else "Failed"
        result = f"""
        Order Cancellation Result:
        ------------------------
        Order ID: {response.id}
        Status: {status}
        """
        
        if response.body:
            result += f"Details: {response.body}\n"
            
        return result
        
    except Exception as e:
        return f"Error cancelling order {order_id}: {str(e)}"

@mcp.tool()
async def replace_order_by_id(
    order_id: str,
    qty: Optional[float] = None,
    limit_price: Optional[float] = None,
    stop_price: Optional[float] = None,
    trail: Optional[float] = None,
    time_in_force: Optional[str] = None,
    client_order_id: Optional[str] = None,
) -> str:
    """
    Modify an existing open order in place (re-ladder a resting limit, tighten a stop,
    or change quantity) without cancelling and re-submitting.

    Only the fields you supply are changed; everything you leave as None is preserved
    from the original order. Alpaca implements this as an atomic replace: it cancels the
    old order and creates a successor, returning the NEW order (with a new id). The
    successor's `replaces` field points back at the original order_id.

    Args:
        order_id: The UUID of the open order to modify.
        qty: New share quantity. Leave None to keep the original quantity.
        limit_price: New limit price (for LIMIT / STOP_LIMIT orders).
        stop_price: New stop price (for STOP / STOP_LIMIT orders).
        trail: New trail amount (for TRAILING_STOP orders).
        time_in_force: New time in force (DAY, GTC, OPG, CLS, IOC, FOK). None keeps original.
        client_order_id: Optional custom identifier for the replacement order.

    Returns:
        str: Formatted details of the replacement order, or an error message.
    """
    try:
        # Translate time_in_force string to the enum, only if the caller is changing it.
        tif_enum = None
        if time_in_force is not None:
            if isinstance(time_in_force, TimeInForce):
                tif_enum = time_in_force
            elif isinstance(time_in_force, str):
                tif_map = {
                    "DAY": TimeInForce.DAY,
                    "GTC": TimeInForce.GTC,
                    "OPG": TimeInForce.OPG,
                    "CLS": TimeInForce.CLS,
                    "IOC": TimeInForce.IOC,
                    "FOK": TimeInForce.FOK,
                }
                tif_enum = tif_map.get(time_in_force.upper())
                if tif_enum is None:
                    return f"Invalid time_in_force: {time_in_force}. Valid options are: DAY, GTC, OPG, CLS, IOC, FOK"
            else:
                return f"Invalid time_in_force type: {type(time_in_force)}. Must be string or TimeInForce enum."

        # Build the request with only the fields the caller actually wants to change, so
        # unspecified attributes are inherited from the original order rather than nulled.
        replace_kwargs: Dict[str, Any] = {}
        if qty is not None:
            replace_kwargs["qty"] = qty
        if limit_price is not None:
            replace_kwargs["limit_price"] = limit_price
        if stop_price is not None:
            replace_kwargs["stop_price"] = stop_price
        if trail is not None:
            replace_kwargs["trail"] = trail
        if tif_enum is not None:
            replace_kwargs["time_in_force"] = tif_enum
        if client_order_id is not None:
            replace_kwargs["client_order_id"] = client_order_id

        if not replace_kwargs:
            return "No changes supplied. Provide at least one of: qty, limit_price, stop_price, trail, time_in_force."

        order = trade_client.replace_order_by_id(order_id, ReplaceOrderRequest(**replace_kwargs))
        return f"""
                Order Replaced Successfully:
                ----------------------------
                replaces (original id): {order.replaces}
                id (new order): {order.id}
                client_order_id: {order.client_order_id}
                symbol: {order.symbol}
                side: {order.side}
                order_type: {order.order_type}
                qty: {order.qty}
                limit_price: {order.limit_price}
                stop_price: {order.stop_price}
                trail_percent: {order.trail_percent}
                trail_price: {order.trail_price}
                time_in_force: {order.time_in_force}
                status: {order.status}
                submitted_at: {order.submitted_at}
                updated_at: {order.updated_at}
                """
    except Exception as e:
        return f"Error replacing order {order_id}: {str(e)}"

# =======================================================================================
# Position Management Tools
# Ref: https://alpaca.markets/sdks/python/api_reference/trading/positions.html#positions
# =======================================================================================

@mcp.tool()
async def close_position(symbol: str, qty: Optional[str] = None, percentage: Optional[str] = None) -> str:
    """
    Closes a specific position for a single symbol. 
    This method will throw an error if the position does not exist!
    
    Args:
        symbol (str): The symbol of the position to close
        qty (Optional[str]): Optional number of shares to liquidate
        percentage (Optional[str]): Optional percentage of shares to liquidate (must result in at least 1 share)
    
    Returns:
        str: Formatted string containing position closure details or error message
    """
    try:
        # Create close position request if options are provided
        close_options = None
        if qty or percentage:
            close_options = ClosePositionRequest(
                qty=qty,
                percentage=percentage
            )
        
        # Close the position
        order = trade_client.close_position(symbol, close_options)
        
        return f"""
                Position Closed Successfully:
                ----------------------------
                Symbol: {symbol}
                Order ID: {order.id}
                Status: {order.status}
                """
                
    except APIError as api_error:
        error_message = str(api_error)
        if "42210000" in error_message and "would result in order size of zero" in error_message:
            return """
            Error: Invalid position closure request.
            
            The requested percentage would result in less than 1 share.
            Please either:
            1. Use a higher percentage
            2. Close the entire position (100%)
            3. Specify an exact quantity using the qty parameter
            """
        else:
            return f"Error closing position: {error_message}"
            
    except Exception as e:
        return f"Error closing position: {str(e)}"
    
@mcp.tool()
async def close_all_positions(cancel_orders: bool = False) -> str:
    """
    Closes all open positions.
    
    Args:
        cancel_orders (bool): If True, cancels all open orders before liquidating positions
    
    Returns:
        str: Formatted string containing position closure results
    """
    try:
        # Close all positions
        close_responses = trade_client.close_all_positions(cancel_orders=cancel_orders)
        
        if not close_responses:
            return "No positions were found to close."
        
        # Format the response
        response_parts = ["Position Closure Results:"]
        response_parts.append("-" * 30)
        
        for response in close_responses:
            response_parts.append(f"Symbol: {response.symbol}")
            response_parts.append(f"Status: {response.status}")
            if response.order_id:
                response_parts.append(f"Order ID: {response.order_id}")
            response_parts.append("-" * 30)
        
        return "\n".join(response_parts)
        
    except Exception as e:
        return f"Error closing positions: {str(e)}"

# Position Management Tools (Options)
@mcp.tool()
async def exercise_options_position(symbol_or_contract_id: str) -> str:
    """
    Exercises a held option contract, converting it into the underlying asset.
    
    Args:
        symbol_or_contract_id (str): Option contract symbol (e.g., 'NVDA250919C001680') or contract ID
    
    Returns:
        str: Success message or error details
    """
    try:
        trade_client.exercise_options_position(symbol_or_contract_id=symbol_or_contract_id)
        return f"Successfully submitted exercise request for option contract: {symbol_or_contract_id}"
    except Exception as e:
        return f"Error exercising option contract '{symbol_or_contract_id}': {str(e)}"

# ============================================================================
# Options Trading Helper Functions
# ============================================================================

def _validate_option_order_inputs(legs: List[Dict[str, Any]], quantity: int, time_in_force: Union[str, TimeInForce]) -> Optional[str]:
    """Validate inputs for option order placement."""
    if not legs:
        return "Error: No option legs provided"
    if len(legs) > 4:
        return "Error: Maximum of 4 legs allowed for option orders"
    if quantity <= 0:
        return "Error: Quantity must be positive"
    
    # Handle both string and enum inputs
    if isinstance(time_in_force, str):
        if time_in_force.lower() != "day":
            return "Error: Only 'day' time_in_force is supported for options trading"
    elif isinstance(time_in_force, TimeInForce):
        if time_in_force != TimeInForce.DAY:
            return "Error: Only DAY time_in_force is supported for options trading"
    else:
        return f"Error: Invalid time_in_force type: {type(time_in_force)}. Must be string or TimeInForce enum."
    
    return None

def _convert_order_class_string(order_class: Optional[Union[str, OrderClass]]) -> Union[OrderClass, str]:
    """Convert order class string to enum if needed."""
    if order_class is None:
        return order_class
    if isinstance(order_class, OrderClass):
        return order_class
    if isinstance(order_class, str):
        order_class_upper = order_class.upper()
        class_mapping = {
            'SIMPLE': OrderClass.SIMPLE,
            'BRACKET': OrderClass.BRACKET,
            'OCO': OrderClass.OCO,
            'OTO': OrderClass.OTO,
            'MLEG': OrderClass.MLEG
        }
        if order_class_upper in class_mapping:
            return class_mapping[order_class_upper]
        else:
            return f"Invalid order class: {order_class}. Must be one of: simple, bracket, oco, oto, mleg"
    else:
        return f"Invalid order class type: {type(order_class)}. Must be string or OrderClass enum."

def _process_option_legs(legs: List[Dict[str, Any]]) -> Union[List[OptionLegRequest], str]:
    """Convert leg dictionaries to OptionLegRequest objects."""
    order_legs = []
    for leg in legs:
        # Validate ratio_qty
        if not isinstance(leg['ratio_qty'], int) or leg['ratio_qty'] <= 0:
            return f"Error: Invalid ratio_qty for leg {leg['symbol']}. Must be positive integer."
        
        # Convert side string to enum
        if leg['side'].lower() == "buy":
            order_side = OrderSide.BUY
        elif leg['side'].lower() == "sell":
            order_side = OrderSide.SELL
        else:
            return f"Invalid order side: {leg['side']}. Must be 'buy' or 'sell'."
        
        order_legs.append(OptionLegRequest(
            symbol=leg['symbol'],
            side=order_side,
            ratio_qty=leg['ratio_qty']
        ))
    return order_legs

def _create_option_market_order_request(
    order_legs: List[OptionLegRequest], 
    order_class: OrderClass, 
    quantity: int,
    time_in_force: TimeInForce,
    extended_hours: bool
) -> MarketOrderRequest:
    """Create the appropriate MarketOrderRequest based on order class."""
    if order_class == OrderClass.MLEG:
        return MarketOrderRequest(
            qty=quantity,
            order_class=order_class,
            time_in_force=time_in_force,
            extended_hours=extended_hours,
            client_order_id=f"mcp_opt_{int(time.time())}",
            type=OrderType.MARKET,
            legs=order_legs
        )
    else:
        # For single-leg orders
        return MarketOrderRequest(
            symbol=order_legs[0].symbol,
            qty=quantity,
            side=order_legs[0].side,
            order_class=order_class,
            time_in_force=time_in_force,
            extended_hours=extended_hours,
            client_order_id=f"mcp_opt_{int(time.time())}",
            type=OrderType.MARKET
        )

def _format_option_order_response(order: Order, order_class: OrderClass, order_legs: List[OptionLegRequest]) -> str:
    """Format the successful order response."""
    result = f"""
            Option Market Order Placed Successfully:
            --------------------------------------
            Order ID: {order.id}
            Client Order ID: {order.client_order_id}
            Order Class: {order.order_class}
            Order Type: {order.type}
            Time In Force: {order.time_in_force}
            Status: {order.status}
            Quantity: {order.qty}
            Created At: {order.created_at}
            Updated At: {order.updated_at}
            """
    
    if order_class == OrderClass.MLEG and order.legs:
        result += "\nLegs:\n"
        for leg in order.legs:
            result += f"""
                    Symbol: {leg.symbol}
                    Side: {leg.side}
                    Ratio Quantity: {leg.ratio_qty}
                    Status: {leg.status}
                    Asset Class: {leg.asset_class}
                    Created At: {leg.created_at}
                    Updated At: {leg.updated_at}
                    Filled Price: {leg.filled_avg_price if hasattr(leg, 'filled_avg_price') else 'Not filled'}
                    Filled Time: {leg.filled_at if hasattr(leg, 'filled_at') else 'Not filled'}
                    -------------------------
                    """
    else:
        result += f"""
                Symbol: {order.symbol}
                Side: {order_legs[0].side}
                Filled Price: {order.filled_avg_price if hasattr(order, 'filled_avg_price') else 'Not filled'}
                Filled Time: {order.filled_at if hasattr(order, 'filled_at') else 'Not filled'}
                -------------------------
                """
    
    return result

def _analyze_option_strategy_type(order_legs: List[OptionLegRequest], order_class: OrderClass) -> tuple[bool, bool, bool]:
    """Analyze the option strategy type for error handling."""
    is_short_straddle = False
    is_short_strangle = False
    is_short_calendar = False
    
    if order_class == OrderClass.MLEG and len(order_legs) == 2:
        both_short = order_legs[0].side == OrderSide.SELL and order_legs[1].side == OrderSide.SELL
        
        if both_short:
            # Check for short straddle (same strike, same expiration, both short)
            if (order_legs[0].symbol.split('C')[0] == order_legs[1].symbol.split('P')[0]):
                is_short_straddle = True
            else:
                is_short_strangle = True
                
            # Check for short calendar spread (same strike, different expirations, both short)
            leg1_type = 'C' if 'C' in order_legs[0].symbol else 'P'
            leg2_type = 'C' if 'C' in order_legs[1].symbol else 'P'
            
            if leg1_type == 'C' and leg2_type == 'C':
                leg1_exp = order_legs[0].symbol.split(leg1_type)[1][:6]
                leg2_exp = order_legs[1].symbol.split(leg2_type)[1][:6]
                if leg1_exp != leg2_exp:
                    is_short_calendar = True
                    is_short_strangle = False  # Override strangle detection
    
    return is_short_straddle, is_short_strangle, is_short_calendar

def _get_short_straddle_error_message() -> str:
    """Get error message for short straddle permission issues."""
    return """
    Error: Account not eligible to trade short straddles.
    
    This error occurs because short straddles require Level 4 options trading permission.
    A short straddle involves:
    - Selling a call option
    - Selling a put option
    - Both options have the same strike price and expiration
    
    Required Account Level:
    - Level 4 options trading permission is required
    - Please contact your broker to upgrade your account level if needed
    
    Alternative Strategies:
    - Consider using a long straddle instead
    - Use a debit spread strategy
    - Implement a covered call or cash-secured put
    """

def _get_short_strangle_error_message() -> str:
    """Get error message for short strangle permission issues."""
    return """
    Error: Account not eligible to trade short strangles.
    
    This error occurs because short strangles require Level 4 options trading permission.
    A short strangle involves:
    - Selling an out-of-the-money call option
    - Selling an out-of-the-money put option
    - Both options have the same expiration
    
    Required Account Level:
    - Level 4 options trading permission is required
    - Please contact your broker to upgrade your account level if needed
    
    Alternative Strategies:
    - Consider using a long strangle instead
    - Use a debit spread strategy
    - Implement a covered call or cash-secured put
    """

def _get_short_calendar_error_message() -> str:
    """Get error message for short calendar spread permission issues."""
    return """
    Error: Account not eligible to trade short calendar spreads.
    
    This error occurs because short calendar spreads require Level 4 options trading permission.
    A short calendar spread involves:
    - Selling a longer-term option
    - Selling a shorter-term option
    - Both options have the same strike price
    
    Required Account Level:
    - Level 4 options trading permission is required
    - Please contact your broker to upgrade your account level if needed
    
    Alternative Strategies:
    - Consider using a long calendar spread instead
    - Use a debit spread strategy
    - Implement a covered call or cash-secured put
    """

def _get_uncovered_options_error_message() -> str:
    """Get error message for uncovered options permission issues."""
    return """
    Error: Account not eligible to trade uncovered option contracts.
    
    This error occurs when attempting to place an order that could result in an uncovered position.
    Common scenarios include:
    1. Selling naked calls
    2. Calendar spreads where the short leg expires after the long leg
    3. Other strategies that could leave uncovered positions
    
    Required Account Level:
    - Level 4 options trading permission is required for uncovered options
    - Please contact your broker to upgrade your account level if needed
    
    Alternative Strategies:
    - Consider using covered calls instead of naked calls
    - Use debit spreads instead of calendar spreads
    - Ensure all positions are properly hedged
    """

def _handle_option_api_error(error_message: str, order_legs: List[OptionLegRequest], order_class: OrderClass) -> str:
    """Handle API errors with specific option strategy analysis."""
    if "40310000" in error_message and "not eligible to trade uncovered option contracts" in error_message:
        is_short_straddle, is_short_strangle, is_short_calendar = _analyze_option_strategy_type(order_legs, order_class)
        
        if is_short_straddle:
            return _get_short_straddle_error_message()
        elif is_short_strangle:
            return _get_short_strangle_error_message()
        elif is_short_calendar:
            return _get_short_calendar_error_message()
        else:
            return _get_uncovered_options_error_message()
    elif "403" in error_message:
        return f"""
        Error: Permission denied for option trading.
        
        Possible reasons:
        1. Insufficient account level for the requested strategy
        2. Account restrictions on option trading
        3. Missing required permissions
        
        Please check:
        1. Your account's option trading level
        2. Any specific restrictions on your account
        3. Required permissions for the strategy you're trying to implement
        
        Original error: {error_message}
        """
    else:
        return f"""
        Error placing option order: {error_message}
        
        Please check:
        1. All option symbols are valid
        2. Your account has sufficient buying power
        3. The market is open for trading
        4. Your account has the required permissions
        """

# ============================================================================
# Options Trading Tool
# ============================================================================

@mcp.tool()
async def place_option_market_order(
    legs: List[Dict[str, Any]],
    order_class: Optional[Union[str, OrderClass]] = None,
    quantity: int = 1,
    time_in_force: Union[str, TimeInForce] = "day",
    extended_hours: bool = False
) -> str:
    """
    Places a market order for options (single or multi-leg) and returns the order details.
    Supports up to 4 legs for multi-leg orders.

    Single vs Multi-Leg Orders:
    - Single-leg: One option contract (buy/sell call or put). Uses "simple" order class.
    - Multi-leg: Multiple option contracts executed together as one strategy (spreads, straddles, etc.). Uses "mleg" order class.
    
    API Processing:
    - Single-leg orders: Sent as standard MarketOrderRequest with symbol and side
    - Multi-leg orders: Sent as MarketOrderRequest with legs array for atomic execution
    
    Args:
        legs (List[Dict[str, Any]]): List of option legs, where each leg is a dictionary containing:
            - symbol (str): Option contract symbol (e.g., 'AAPL230616C00150000')
            - side (str): 'buy' or 'sell'
            - ratio_qty (int): Quantity ratio for the leg (1-4)
        order_class (Optional[Union[str, OrderClass]]): Order class ('simple', 'bracket', 'oco', 'oto', 'mleg' or OrderClass enum)
            Defaults to 'simple' for single leg, 'mleg' for multi-leg
        quantity (int): Base quantity for the order (default: 1)
        time_in_force (Union[str, TimeInForce]): Time in force for the order. For options trading, 
            only 'day' is supported (default: 'day')
        extended_hours (bool): Whether to allow execution during extended hours (default: False)
    
    Returns:
        str: Formatted string containing order details or error message

    Examples:
        # Single-leg: Buy 1 call option
        legs = [{"symbol": "AAPL230616C00150000", "side": "buy", "ratio_qty": 1}]
        
        # Multi-leg: Bull call spread (executed atomically)
        legs = [
            {"symbol": "AAPL230616C00150000", "side": "buy", "ratio_qty": 1},
            {"symbol": "AAPL230616C00160000", "side": "sell", "ratio_qty": 1}
        ]
    
    Note:
        Some option strategies may require specific account permissions:
        - Level 1: Covered calls, Covered puts, Cash-Secured put, etc.
        - Level 2: Long calls, Long puts, cash-secured puts, etc.
        - Level 3: Spreads and combinations: Butterfly Spreads, Straddles, Strangles, Calendar Spreads (except for short call calendar spread, short strangles, short straddles)
        - Level 4: Uncovered options (naked calls/puts), Short Strangles, Short Straddles, Short Call Calendar Spread, etc.
        If you receive a permission error, please check your account's option trading level.
    """
    # Initialize variables that might be used in exception handlers
    order_legs: List[OptionLegRequest] = []
    
    try:
        # Validate inputs
        validation_error = _validate_option_order_inputs(legs, quantity, time_in_force)
        if validation_error:
            return validation_error
        
        # Convert time_in_force to enum (handle both string and enum inputs)
        if isinstance(time_in_force, str):
            time_in_force_enum = TimeInForce.DAY  # Only DAY is supported for options
        elif isinstance(time_in_force, TimeInForce):
            time_in_force_enum = time_in_force
        else:
            return f"Error: Invalid time_in_force type: {type(time_in_force)}. Must be string or TimeInForce enum."
        
        # Convert order class string to enum if needed
        converted_order_class = _convert_order_class_string(order_class)
        if isinstance(converted_order_class, OrderClass):
            order_class = converted_order_class
        elif isinstance(converted_order_class, str):  # Error message returned
            return converted_order_class
        
        # Determine order class if not provided
        if order_class is None:
            order_class = OrderClass.MLEG if len(legs) > 1 else OrderClass.SIMPLE
        
        # Process legs
        processed_legs = _process_option_legs(legs)
        if isinstance(processed_legs, str):  # Error message returned
            return processed_legs
        order_legs = processed_legs
        
        # Create order request
        order_data = _create_option_market_order_request(
            order_legs, order_class, quantity, time_in_force_enum, extended_hours
        )
        
        # Submit order
        order = trade_client.submit_order(order_data)
        
        # Format and return response
        return _format_option_order_response(order, order_class, order_legs)
        
    except APIError as api_error:
        return _handle_option_api_error(str(api_error), order_legs, order_class)
        
    except Exception as e:
        return f"""
        Unexpected error placing option order: {str(e)}
        
        Please try:
        1. Verifying all input parameters
        2. Checking your account status
        3. Ensuring market is open
        4. Contacting support if the issue persists
        """

# Session State Management Tools
# ----------------------------------------------------------------------------

@mcp.tool()
async def record_decision(
    symbol: str,
    decision_type: str,
    decision: str,
    metadata: Optional[Union[str, Dict[str, Any]]] = None
) -> str:
    """
    Record a trading decision or analysis that persists between Claude sessions.

    Use this to remember your analysis, trading plans, alerts, and conclusions
    so they survive Claude Desktop restarts. This eliminates the need to
    re-analyze the same symbols repeatedly.

    Args:
        symbol: Stock/crypto ticker (e.g., 'NVDA', 'SPY', 'BTC/USD')
        decision_type: Type of decision - 'analysis', 'alert', 'risk', or 'note'
        decision: Your conclusion or trading plan
        metadata: Optional dict or JSON string with additional context
                 Examples: {"target": "135", "stop": "115", "confidence": "high"}
                          '{"entry": "47", "stop": "49"}'

    Returns:
        Confirmation message with the recorded decision

    Example usage:
        - record_decision("NVDA", "analysis", "BUY - RSI oversold", {"target": "135", "stop": "115"})
        - record_decision("SPY", "alert", "Watch for break above $450")
        - record_decision("RGTI", "analysis", "SHORT setup", {"entry": "47", "stop": "49"})
    """
    import json

    try:
        # Validate decision_type
        valid_types = ['analysis', 'alert', 'risk', 'note']
        decision_type_clean = decision_type.lower().strip()
        if decision_type_clean not in valid_types:
            return f"❌ Invalid decision_type '{decision_type}'. Must be one of: {', '.join(valid_types)}"

        # Handle metadata - accept both dict and string
        metadata_str = None
        if metadata:
            if isinstance(metadata, dict):
                # Convert dict to JSON string
                metadata_str = json.dumps(metadata)
            elif isinstance(metadata, str):
                # Validate it's valid JSON
                try:
                    json.loads(metadata)
                    metadata_str = metadata
                except json.JSONDecodeError:
                    return f"❌ Invalid metadata JSON: {metadata}"
            else:
                return f"❌ Metadata must be a dict or JSON string, got {type(metadata).__name__}"

        with get_db() as conn:
            # Use INSERT OR REPLACE to handle duplicates
            conn.execute('''
                INSERT OR REPLACE INTO decisions (symbol, decision_type, decision, metadata, timestamp, session_date)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                symbol.upper().strip(),
                decision_type_clean,
                decision.strip(),
                metadata_str,
                datetime.now().isoformat(),
                date.today().isoformat()
            ))

        meta_display = f"\n  └─ Metadata: {metadata_str}" if metadata_str else ""
        return f"✓ Recorded: {symbol.upper()} [{decision_type_clean}]\n  └─ {decision}{meta_display}"

    except Exception as e:
        return f"❌ Error recording decision: {str(e)}"

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
