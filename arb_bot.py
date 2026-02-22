#!/usr/bin/env python3
"""
Coinbase ↔ Kraken Cross-Exchange Arbitrage Bot
===============================================
Scans for profitable price discrepancies between Coinbase Advanced Trade
and Kraken, accounting for exchange fees, order-book slippage, liquidity
depth, and configurable risk limits.

Default mode: PAPER TRADING — no real funds are touched.
To enable live execution: set  live_mode: true  in config.yaml (read all
risk warnings first and test thoroughly in paper mode).

Required pip installs
─────────────────────
    pip install ccxt pyyaml requests

Sample run
──────────
    python arb_bot.py                         # paper mode, all pairs
    python arb_bot.py --config my.yaml        # custom config file
    python arb_bot.py --pairs BTC/USD ETH/USD # restrict to specific pairs
    python arb_bot.py --dry-run               # one scan then exit (smoke test)

CCXT exchange IDs used
──────────────────────
    Coinbase Advanced Trade → 'coinbaseadvanced'  (ccxt >= 4.x)
    Kraken                  → 'kraken'
    Run: python -c "import ccxt; print(ccxt.exchanges)" to verify.

⚠️  Risk disclaimer
────────────────────
    Cross-exchange arbitrage carries real risks: execution latency, API
    failures, price movements between leg 1 and leg 2, withdrawal delays,
    and regulatory issues. This script is provided for educational and
    paper-trading purposes. Use live mode at your own risk.
"""

# ══════════════════════════════════════════════════════════════════════════════
# IMPORTS
# ══════════════════════════════════════════════════════════════════════════════

import argparse
import json
import logging
import os
import signal
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import ccxt
import yaml

# Requests is used for Telegram alerts; gracefully optional.
try:
    import requests as _requests
    _REQUESTS_AVAILABLE = True
except ImportError:
    _REQUESTS_AVAILABLE = False


# ══════════════════════════════════════════════════════════════════════════════
# DATA MODELS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class OrderBook:
    """Snapshot of top-N bid/ask levels from one exchange."""
    symbol:    str
    exchange:  str
    bids:      list   # [[price, volume], …] sorted best-first
    asks:      list   # [[price, volume], …] sorted best-first
    timestamp: float  # Unix seconds when the snapshot was captured

    @property
    def best_bid(self) -> float:
        return float(self.bids[0][0]) if self.bids else 0.0

    @property
    def best_ask(self) -> float:
        return float(self.asks[0][0]) if self.asks else float("inf")

    @property
    def mid(self) -> float:
        return (self.best_bid + self.best_ask) / 2


@dataclass
class ArbOpportunity:
    """A detected cross-exchange arbitrage opportunity."""
    symbol:                  str
    buy_exchange:            str
    sell_exchange:           str
    buy_price:               float    # top-of-book ask on buy side
    sell_price:              float    # top-of-book bid on sell side
    buy_vwap:                float    # VWAP after walking the book (buy side)
    sell_vwap:               float    # VWAP after walking the book (sell side)
    raw_spread_pct:          float    # (best_bid_sell - best_ask_buy) / best_ask_buy * 100
    fees_pct:                float    # combined taker fees both legs
    slippage_pct:            float    # combined slippage both legs
    net_profit_pct:          float    # raw_spread - fees - slippage
    position_usdc:           float    # USDC size we can fill with available liquidity
    estimated_profit_usdc:   float
    available_liquidity_usdc: float   # depth supporting the position
    detected_at:             float = field(default_factory=time.time)

    @property
    def summary(self) -> str:
        return (
            f"{self.symbol}: BUY {self.buy_exchange} @ {self.buy_price:.6f}"
            f" | SELL {self.sell_exchange} @ {self.sell_price:.6f}"
            f" | net={self.net_profit_pct:+.3f}%"
            f" | est. ${self.estimated_profit_usdc:.4f}"
        )


@dataclass
class Trade:
    """Record of one executed (paper or live) order."""
    id:         str
    arb_id:     str    # links buy + sell sides of the same arb
    symbol:     str
    side:       str    # "buy" | "sell"
    exchange:   str
    price:      float
    amount:     float  # base currency quantity
    usdc_value: float  # USDC equivalent
    fee_usdc:   float
    timestamp:  float
    paper:      bool = True


@dataclass
class PortfolioState:
    """Persistent portfolio snapshot."""
    cash_usdc:           float
    holdings:            dict  = field(default_factory=dict)  # currency → qty
    realized_pnl:        float = 0.0
    daily_pnl:           float = 0.0
    daily_trades:        int   = 0
    total_trades:        int   = 0
    daily_start_balance: float = 0.0
    last_reset_date:     str   = ""


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

class Config:
    """
    Loads config.yaml, merges with sensible defaults, and validates
    credentials when live_mode is enabled.
    """

    DEFAULTS: dict = {
        "live_mode": False,
        "strategy": {
            "scan_interval_seconds":  7,
            "min_profit_pct":         0.5,
            "max_position_usdc":      500.0,
            "max_position_pct":       0.02,
            "min_24h_volume_usdc":    100_000,
            "order_book_depth":       10,
            "triangular":             False,
        },
        "fees": {
            "coinbase": {"maker": 0.004, "taker": 0.006},
            "kraken":   {"maker": 0.0016, "taker": 0.0026},
        },
        "risk": {
            "max_daily_drawdown_pct": 5.0,
            "max_daily_trades":       50,
            "max_volatility_pct":     3.0,
            "min_order_book_age_s":   5.0,
        },
        "paper":   {"starting_balance_usdc": 10_000.0},
        "alerts":  {"telegram": {"enabled": False, "bot_token": "", "chat_id": ""},
                    "min_profit_to_alert_pct": 1.0},
        "logging": {"level": "INFO", "log_file": "arb_bot.log"},
    }

    def __init__(self, path: str):
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        self._cfg = self._deep_merge(self.DEFAULTS, raw)
        self._validate()

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _deep_merge(self, base: dict, override: dict) -> dict:
        result = dict(base)
        for k, v in override.items():
            if isinstance(v, dict) and isinstance(result.get(k), dict):
                result[k] = self._deep_merge(result[k], v)
            else:
                result[k] = v
        return result

    def _validate(self) -> None:
        if self.live_mode:
            for ex in ("coinbase", "kraken"):
                key = self._cfg.get(ex, {}).get("api_key", "")
                if not key or key.startswith("YOUR"):
                    raise ValueError(
                        f"live_mode=true but {ex}.api_key is not set in config.yaml."
                    )

    def get(self, *keys, default=None):
        """Drill into nested config dict with a default fallback."""
        d = self._cfg
        for k in keys:
            if not isinstance(d, dict):
                return default
            d = d.get(k, default)
        return d

    # ── Typed shortcuts ───────────────────────────────────────────────────────

    @property
    def live_mode(self) -> bool:
        return bool(self._cfg.get("live_mode", False))

    @property
    def scan_interval(self) -> float:
        return float(self.get("strategy", "scan_interval_seconds", default=7))

    @property
    def min_profit_pct(self) -> float:
        return float(self.get("strategy", "min_profit_pct", default=0.5))

    @property
    def max_position_usdc(self) -> float:
        return float(self.get("strategy", "max_position_usdc", default=500.0))

    @property
    def order_book_depth(self) -> int:
        return int(self.get("strategy", "order_book_depth", default=10))

    @property
    def starting_balance(self) -> float:
        return float(self.get("paper", "starting_balance_usdc", default=10_000.0))

    def fee(self, exchange: str, side: str = "taker") -> float:
        return float(self.get("fees", exchange, side, default=0.006))


# ══════════════════════════════════════════════════════════════════════════════
# EXCHANGE CLIENT  (CCXT wrapper)
# ══════════════════════════════════════════════════════════════════════════════

class ExchangeClient:
    """
    Wraps a CCXT exchange with:
      - Built-in rate limiting (enableRateLimit=True)
      - Automatic error categorisation (network vs exchange vs unexpected)
      - Kraken pair-name normalisation (XBT → BTC)
      - Lazy market loading (cached after first call)
    """

    # Internal name → CCXT exchange ID
    CCXT_IDS = {
        "coinbase": "coinbaseadvanced",   # ccxt >= 4.x; try 'coinbase' if this fails
        "kraken":   "kraken",
    }

    def __init__(self, name: str, cfg: Config, log: logging.Logger):
        self.name = name
        self._log = log
        self._markets: dict = {}

        ccxt_id = self.CCXT_IDS.get(name, name)
        params: dict = {
            "enableRateLimit": True,   # CCXT honours per-exchange rate limits
            "timeout":         10_000, # 10 s HTTP timeout
        }

        # Attach credentials only in live mode
        if cfg.live_mode:
            api_key    = cfg.get(name, "api_key",    default="")
            api_secret = cfg.get(name, "api_secret", default="")
            if api_key:
                params["apiKey"] = api_key
                params["secret"] = api_secret

        try:
            self._ex = getattr(ccxt, ccxt_id)(params)
        except AttributeError:
            raise ValueError(
                f"Exchange '{ccxt_id}' not found in CCXT. "
                f"Run: python -c \"import ccxt; print(ccxt.exchanges)\""
            )

        # Cap per-request timeouts and rate limits for both exchanges so that
        # a single hung API call doesn't block the scan thread for 10+ seconds.
        self._ex.timeout = 8_000   # 8 s per request max

        if name == "kraken":
            # Kraken's CCXT default rateLimit is 3000 ms (very conservative).
            # Public endpoints allow ~1 req/s; 1000 ms is safe.
            self._ex.rateLimit = 1000
        elif name == "coinbase":
            # Coinbase Advanced Trade allows ~30 req/s public; 200 ms is safe.
            self._ex.rateLimit = 200

    # ── Public API ────────────────────────────────────────────────────────────

    def load_markets(self) -> dict:
        """Fetch all markets (cached after first call)."""
        if not self._markets:
            self._log.debug("[%s] Loading markets…", self.name)
            self._markets = self._ex.load_markets()
            self._log.info("[%s] Loaded %d markets", self.name, len(self._markets))
        return self._markets

    def fetch_order_book(self, symbol: str, limit: int = 10) -> Optional[OrderBook]:
        """Return top-N bid/ask levels, or None on failure."""
        try:
            raw = self._ex.fetch_order_book(symbol, limit)
            ts  = (raw.get("timestamp") or time.time() * 1000) / 1000
            return OrderBook(
                symbol=symbol,
                exchange=self.name,
                bids=raw["bids"][:limit],
                asks=raw["asks"][:limit],
                timestamp=ts,
            )
        except ccxt.NetworkError as e:
            self._log.warning("[%s] Network error %s: %s", self.name, symbol, e)
        except ccxt.ExchangeError as e:
            self._log.warning("[%s] Exchange error %s: %s", self.name, symbol, e)
        except Exception as e:
            self._log.error("[%s] Unexpected error %s: %s", self.name, symbol, e)
        return None

    def fetch_ticker(self, symbol: str) -> Optional[dict]:
        """Return 24h ticker data (last, high, low, quoteVolume)."""
        try:
            return self._ex.fetch_ticker(symbol)
        except Exception as e:
            self._log.debug("[%s] Ticker error %s: %s", self.name, symbol, e)
            return None

    def create_order(self, symbol: str, side: str, amount: float,
                     price: Optional[float] = None) -> Optional[dict]:
        """
        Place a market order (price=None) or limit order (price=float).
        Returns the CCXT order dict on success, None on failure.
        """
        try:
            order_type = "market" if price is None else "limit"
            order = self._ex.create_order(symbol, order_type, side, amount, price)
            self._log.info(
                "[%s] %s %s %s qty=%.6f price=%s",
                self.name, order_type.upper(), side.upper(), symbol, amount,
                f"{price:.6f}" if price else "MARKET",
            )
            return order
        except ccxt.InsufficientFunds as e:
            self._log.error("[%s] Insufficient funds: %s", self.name, e)
        except ccxt.ExchangeError as e:
            self._log.error("[%s] Order error: %s", self.name, e)
        return None

    @property
    def symbols(self) -> set:
        return set(self._markets.keys())


# ══════════════════════════════════════════════════════════════════════════════
# PAIR DISCOVERY
# ══════════════════════════════════════════════════════════════════════════════

class PairDiscovery:
    """
    Identifies the intersection of active trading pairs on both exchanges,
    then filters by minimum 24h quote volume on both sides.

    Handles Kraken's non-standard ticker names:
      XBT → BTC,  XDG → DOGE  (others added as needed)
    """

    # Kraken uses non-ISO currency codes; map them to standard ones.
    KRAKEN_RENAMES = {"XBT": "BTC", "XDG": "DOGE"}

    def __init__(self, cb: ExchangeClient, kr: ExchangeClient,
                 cfg: Config, log: logging.Logger):
        self._cb  = cb
        self._kr  = kr
        self._cfg = cfg
        self._log = log

    def find_common_pairs(self, restrict: Optional[list] = None) -> list:
        """
        Return sorted list of pairs tradeable on both exchanges that
        pass the 24h volume filter.  Optionally restrict to `restrict`.

        Uses fetch_tickers() (one bulk call per exchange) instead of
        individual ticker requests — makes startup ~100× faster.
        """
        cb_markets = self._cb.load_markets()
        kr_markets = self._kr.load_markets()

        # Active spot pairs only (no futures / perpetuals)
        cb_syms = {
            s for s, m in cb_markets.items()
            if m.get("active", True) and "/" in s and m.get("type", "spot") == "spot"
        }
        kr_syms = {
            s for s, m in kr_markets.items()
            if m.get("active", True) and "/" in s
        }

        # Normalise Kraken names so they match Coinbase (XBT → BTC, etc.)
        kr_normalised: dict = {}   # normalised_symbol → original_kraken_symbol
        for sym in kr_syms:
            norm = sym
            for old, new in self.KRAKEN_RENAMES.items():
                norm = norm.replace(old, new)
            kr_normalised[norm] = sym

        common = cb_syms & set(kr_normalised.keys())
        self._log.info(
            "Symbol intersection before volume filter: %d  (CB=%d, KR=%d)",
            len(common), len(cb_syms), len(kr_syms),
        )

        if restrict:
            common = {s for s in common if s in restrict}
            self._log.info("Restricted to %d user-specified pairs", len(common))

        # Bulk-fetch all tickers in a single API call per exchange
        min_vol = float(self._cfg.get("strategy", "min_24h_volume_usdc", default=100_000))
        cb_tickers = self._fetch_all_tickers(self._cb)
        kr_tickers = self._fetch_all_tickers(self._kr)

        qualified = []
        for sym in sorted(common):
            cb_vol = float((cb_tickers.get(sym) or {}).get("quoteVolume") or 0)
            # Kraken tickers use their native symbol name; try both
            kr_sym  = kr_normalised.get(sym, sym)
            kr_tick = kr_tickers.get(kr_sym) or kr_tickers.get(sym) or {}
            kr_vol  = float(kr_tick.get("quoteVolume") or 0)

            if cb_vol >= min_vol and kr_vol >= min_vol:
                qualified.append(sym)
            else:
                self._log.debug(
                    "  Volume skip %s: CB=$%.0f KR=$%.0f (need $%.0f)",
                    sym, cb_vol, kr_vol, min_vol,
                )

        self._log.info(
            "Qualified pairs after volume filter (min $%.0f/24h): %d",
            min_vol, len(qualified),
        )
        return qualified

    def _fetch_all_tickers(self, client: ExchangeClient) -> dict:
        """Fetch all tickers in one bulk call; fall back to empty dict on error."""
        try:
            self._log.info("[%s] Fetching all tickers (bulk)…", client.name)
            tickers = client._ex.fetch_tickers()
            self._log.info("[%s] Got %d tickers", client.name, len(tickers))
            return tickers
        except Exception as e:
            self._log.warning("[%s] fetch_tickers() failed (%s) — skipping volume filter", client.name, e)
            return {}


# ══════════════════════════════════════════════════════════════════════════════
# ORDER BOOK ANALYSER  (VWAP + slippage)
# ══════════════════════════════════════════════════════════════════════════════

class OrderBookAnalyzer:
    """
    Walks order-book levels to compute the volume-weighted average execution
    price (VWAP) for a given position size.  The difference between VWAP and
    the top-of-book price is the estimated slippage.

    All amounts are in USDC (quote currency).
    """

    @staticmethod
    def vwap_buy(book: OrderBook, usdc_amount: float) -> tuple:
        """
        Simulate buying `usdc_amount` worth of the base asset against the ask
        side of the order book.

        Returns (vwap_price, usdc_actually_filled).
        """
        remaining  = usdc_amount
        total_cost = 0.0
        total_qty  = 0.0

        for level in book.asks:
            price, qty = float(level[0]), float(level[1])
            level_cost = price * qty

            if remaining <= level_cost:
                fill_qty    = remaining / price
                total_cost += fill_qty * price
                total_qty  += fill_qty
                remaining   = 0.0
                break
            else:
                total_cost += level_cost
                total_qty  += qty
                remaining  -= level_cost

        if total_qty == 0:
            return float("inf"), 0.0

        vwap   = total_cost / total_qty
        filled = usdc_amount - remaining
        return vwap, filled

    @staticmethod
    def vwap_sell(book: OrderBook, usdc_amount: float) -> tuple:
        """
        Simulate selling assets worth `usdc_amount` against the bid side.

        Returns (vwap_price, usdc_actually_filled).
        """
        remaining      = usdc_amount
        total_proceeds = 0.0
        total_qty      = 0.0

        for level in book.bids:
            price, qty  = float(level[0]), float(level[1])
            level_value = price * qty

            if remaining <= level_value:
                fill_qty        = remaining / price
                total_proceeds += fill_qty * price
                total_qty      += fill_qty
                remaining       = 0.0
                break
            else:
                total_proceeds += level_value
                total_qty      += qty
                remaining      -= level_value

        if total_qty == 0:
            return 0.0, 0.0

        vwap   = total_proceeds / total_qty
        filled = usdc_amount - remaining
        return vwap, filled

    @staticmethod
    def slippage_pct(top_of_book: float, vwap: float) -> float:
        """% deviation of VWAP from the top-of-book price (always positive)."""
        if top_of_book <= 0:
            return 0.0
        return abs(vwap - top_of_book) / top_of_book * 100

    @staticmethod
    def available_liquidity(book: OrderBook, side: str, levels: int = 5) -> float:
        """Total USDC available in the top N levels on 'bid' or 'ask' side."""
        levels_data = book.bids if side == "bid" else book.asks
        return sum(float(lv[0]) * float(lv[1]) for lv in levels_data[:levels])


# ══════════════════════════════════════════════════════════════════════════════
# ARBITRAGE CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════

class ArbCalculator:
    """
    Detects profitable opportunities in two modes:

    Direct arbitrage
    ────────────────
    Buy the asset on the cheaper exchange (ask side),
    simultaneously sell it on the more expensive exchange (bid side).
    Net P&L = sell_vwap - buy_vwap - taker_fees_both_legs - slippage_both_legs

    Triangular arbitrage  (same exchange, optional)
    ────────────────────
    USD → A/USD → A/B → B/USD → USD
    Profit if the product of the three conversion rates > 1 after fees.
    """

    def __init__(self, cfg: Config, log: logging.Logger):
        self._cfg = cfg
        self._log = log
        self._ob  = OrderBookAnalyzer()

    # ── Direct arb ────────────────────────────────────────────────────────────

    def evaluate_direct(
        self,
        symbol:        str,
        book_cb:       OrderBook,
        book_kr:       OrderBook,
        position_usdc: float,
    ) -> list:
        """
        Check both directions (CB→KR and KR→CB).
        Returns list of ArbOpportunity (0 or 1 per direction, filtered by min_profit_pct).
        """
        results = []
        for buy_ex, buy_book, sell_ex, sell_book in [
            ("coinbase", book_cb, "kraken",   book_kr),
            ("kraken",   book_kr, "coinbase", book_cb),
        ]:
            opp = self._check_direction(
                symbol, buy_ex, buy_book, sell_ex, sell_book, position_usdc
            )
            if opp and opp.net_profit_pct >= self._cfg.min_profit_pct:
                results.append(opp)
        return results

    def _check_direction(
        self,
        symbol:        str,
        buy_ex:        str,
        buy_book:      OrderBook,
        sell_ex:       str,
        sell_book:     OrderBook,
        position_usdc: float,
    ) -> Optional[ArbOpportunity]:

        if not buy_book.asks or not sell_book.bids:
            return None

        # ── Stale data guard ──────────────────────────────────────────────────
        max_age = float(self._cfg.get("risk", "min_order_book_age_s", default=5.0))
        now     = time.time()
        if now - buy_book.timestamp  > max_age or now - sell_book.timestamp > max_age:
            self._log.debug("Stale order book for %s — skipping", symbol)
            return None

        top_ask = buy_book.best_ask
        top_bid = sell_book.best_bid

        # Quick pre-filter: raw spread must be positive before we walk the book
        if top_bid <= top_ask:
            return None

        raw_spread_pct = (top_bid - top_ask) / top_ask * 100

        # ── VWAP + slippage ───────────────────────────────────────────────────
        buy_vwap,  buy_filled  = OrderBookAnalyzer.vwap_buy( buy_book,  position_usdc)
        sell_vwap, sell_filled = OrderBookAnalyzer.vwap_sell(sell_book, position_usdc)

        # Only trade what both sides can support
        actual_usdc = min(buy_filled, sell_filled, position_usdc)
        if actual_usdc < 10.0:
            return None   # not enough liquidity even for a minimal trade

        buy_slip  = OrderBookAnalyzer.slippage_pct(top_ask, buy_vwap)
        sell_slip = OrderBookAnalyzer.slippage_pct(top_bid, sell_vwap)
        total_slip_pct = buy_slip + sell_slip

        # ── Fees ──────────────────────────────────────────────────────────────
        # Both legs pay taker fees (we need immediate execution)
        fee_pct = (self._cfg.fee(buy_ex, "taker") + self._cfg.fee(sell_ex, "taker")) * 100

        # ── Net profit ────────────────────────────────────────────────────────
        net_profit_pct    = raw_spread_pct - fee_pct - total_slip_pct
        estimated_profit  = actual_usdc * net_profit_pct / 100

        # Available depth (top 5 levels each side)
        liquidity = min(
            OrderBookAnalyzer.available_liquidity(buy_book,  "ask", levels=5),
            OrderBookAnalyzer.available_liquidity(sell_book, "bid", levels=5),
        )

        return ArbOpportunity(
            symbol=symbol,
            buy_exchange=buy_ex,
            sell_exchange=sell_ex,
            buy_price=top_ask,
            sell_price=top_bid,
            buy_vwap=buy_vwap,
            sell_vwap=sell_vwap,
            raw_spread_pct=raw_spread_pct,
            fees_pct=fee_pct,
            slippage_pct=total_slip_pct,
            net_profit_pct=net_profit_pct,
            position_usdc=actual_usdc,
            estimated_profit_usdc=estimated_profit,
            available_liquidity_usdc=liquidity,
        )

    # ── Triangular arb ────────────────────────────────────────────────────────

    def evaluate_triangular(
        self,
        books: dict,   # {"coinbase": {symbol: OrderBook}, "kraken": {symbol: OrderBook}}
        position_usdc: float,
    ) -> list:
        """
        Same-exchange triangular arb.  For each exchange, find all loops of
        the form:  USD → base/USD → base/mid → mid/USD → USD
        and return those where net profit (after 3 × taker fees) is positive.
        """
        results = []
        for ex_name in ("coinbase", "kraken"):
            ex_books = books.get(ex_name, {})
            results.extend(self._tri_on_exchange(ex_name, ex_books, position_usdc))
        return results

    def _tri_on_exchange(self, ex_name: str, books: dict, position_usdc: float) -> list:
        results  = []
        symbols  = list(books.keys())
        fee      = self._cfg.fee(ex_name, "taker")   # applied three times

        # Start: pairs quoted in USD/USDT
        usd_pairs   = [s for s in symbols if s.endswith("/USD") or s.endswith("/USDT")]
        # Middle: cross pairs (crypto/crypto)
        cross_pairs = [s for s in symbols if "/" in s
                       and not s.endswith("/USD") and not s.endswith("/USDT")]

        for start_pair in usd_pairs:           # e.g. ETH/USD
            base, quote = start_pair.split("/")  # base=ETH

            for mid_pair in cross_pairs:       # e.g. ETH/BTC
                mid_base, mid_quote = mid_pair.split("/")
                if mid_base != base:
                    continue

                close_pair = f"{mid_quote}/{quote}"   # BTC/USD
                if close_pair not in books:
                    continue

                b1 = books[start_pair]   # buy ETH with USD
                b2 = books[mid_pair]     # sell ETH for BTC
                b3 = books[close_pair]   # sell BTC for USD

                if not b1.asks or not b2.bids or not b3.bids:
                    continue

                # Simplified top-of-book calculation (no depth walk for speed)
                start = position_usdc
                after_1 = (start   / b1.best_ask) * (1 - fee)  # ETH qty
                after_2 = (after_1 * b2.best_bid) * (1 - fee)  # BTC qty
                after_3 = (after_2 * b3.best_bid) * (1 - fee)  # USD

                profit_pct = (after_3 - start) / start * 100
                if profit_pct >= self._cfg.min_profit_pct:
                    results.append({
                        "type":                  "triangular",
                        "exchange":              ex_name,
                        "path":                  f"USD→{start_pair}→{mid_pair}→{close_pair}→USD",
                        "net_profit_pct":        round(profit_pct, 4),
                        "position_usdc":         start,
                        "estimated_profit_usdc": round(after_3 - start, 4),
                    })
        return results


# ══════════════════════════════════════════════════════════════════════════════
# RISK MANAGER
# ══════════════════════════════════════════════════════════════════════════════

class RiskManager:
    """
    Acts as a gate before any trade is placed.  Checks:
      1. Daily drawdown limit
      2. Daily trade count limit
      3. 24h volatility of the pair
      4. Sufficient cash for the position
    """

    def __init__(self, cfg: Config, log: logging.Logger):
        self._cfg = cfg
        self._log = log

    def position_size(self, state: PortfolioState) -> float:
        """
        Allowed USDC per trade = min(max_position_usdc, max_position_pct × cash).
        This ensures we never risk more than a set fraction of the portfolio.
        """
        pct_limit = float(self._cfg.get("strategy", "max_position_pct", default=0.02))
        return min(state.cash_usdc * pct_limit, self._cfg.max_position_usdc)

    def check_drawdown(self, state: PortfolioState) -> bool:
        max_dd = float(self._cfg.get("risk", "max_daily_drawdown_pct", default=5.0))
        if state.daily_start_balance <= 0:
            return True
        dd_pct = (state.daily_start_balance - state.cash_usdc) / state.daily_start_balance * 100
        if dd_pct >= max_dd:
            self._log.warning(
                "Daily drawdown limit hit: %.2f%% >= %.2f%% — halting trades",
                dd_pct, max_dd,
            )
            return False
        return True

    def check_trade_limit(self, state: PortfolioState) -> bool:
        max_t = int(self._cfg.get("risk", "max_daily_trades", default=50))
        if state.daily_trades >= max_t:
            self._log.warning("Daily trade limit reached (%d) — halting", max_t)
            return False
        return True

    def check_volatility(self, symbol: str, ticker: Optional[dict]) -> bool:
        """
        Use 24h high/low range as a volatility proxy.
        Skip the pair if (high - low) / low > max_volatility_pct.
        Caution: high volatility increases the risk of leg-1/leg-2 price drift.
        """
        if ticker is None:
            return True   # can't measure → allow (conservative option: return False)
        max_vol = float(self._cfg.get("risk", "max_volatility_pct", default=3.0))
        high = float(ticker.get("high") or 0)
        low  = float(ticker.get("low")  or 1)
        if low <= 0:
            return True
        vol_pct = (high - low) / low * 100
        if vol_pct > max_vol:
            self._log.debug(
                "Skipping %s — 24h volatility %.2f%% > limit %.2f%%",
                symbol, vol_pct, max_vol,
            )
            return False
        return True

    def approve(
        self,
        opp:    ArbOpportunity,
        state:  PortfolioState,
        ticker: Optional[dict] = None,
    ) -> bool:
        """Final approve/reject decision before execution."""
        if not self.check_drawdown(state):    return False
        if not self.check_trade_limit(state): return False
        if not self.check_volatility(opp.symbol, ticker): return False
        if state.cash_usdc < opp.position_usdc:
            self._log.debug("Insufficient cash for %s", opp.symbol)
            return False
        return True


# ══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════

class Portfolio:
    """
    Tracks cash, holdings, and realised P&L.  State is persisted to
    portfolio_state.json so runs survive restarts.

    Paper mode:  apply_arb() is called to instantly simulate both legs.
    Live mode:   record_trade() is called after each confirmed order fill.
    """

    STATE_FILE = "portfolio_state.json"

    def __init__(self, cfg: Config, log: logging.Logger):
        self._cfg    = cfg
        self._log    = log
        self._trades: list = []
        self.state   = self._load_or_init()

    def _load_or_init(self) -> PortfolioState:
        if os.path.exists(self.STATE_FILE):
            try:
                with open(self.STATE_FILE) as f:
                    d = json.load(f)
                self._log.info("Restored portfolio from %s", self.STATE_FILE)
                return PortfolioState(**d)
            except Exception as e:
                self._log.warning("Could not load %s (%s) — starting fresh", self.STATE_FILE, e)

        bal   = self._cfg.starting_balance
        today = date.today().isoformat()
        return PortfolioState(
            cash_usdc=bal,
            holdings={},
            realized_pnl=0.0,
            daily_pnl=0.0,
            daily_trades=0,
            total_trades=0,
            daily_start_balance=bal,
            last_reset_date=today,
        )

    def save(self) -> None:
        try:
            with open(self.STATE_FILE, "w") as f:
                json.dump(self.state.__dict__, f, indent=2)
        except Exception as e:
            self._log.error("Failed to save portfolio: %s", e)

    def maybe_reset_daily(self) -> None:
        """Call once per scan loop to reset daily counters at midnight."""
        today = date.today().isoformat()
        if self.state.last_reset_date != today:
            self._log.info("New trading day — resetting daily P&L and trade counters")
            self.state.daily_pnl          = 0.0
            self.state.daily_trades       = 0
            self.state.daily_start_balance = self.state.cash_usdc
            self.state.last_reset_date    = today
            self.save()

    def apply_arb(self, opp: ArbOpportunity, cfg: Config) -> tuple:
        """
        Simulate both legs of a paper trade and update portfolio state.
        Returns (buy_trade, sell_trade).
        """
        arb_id = str(uuid.uuid4())[:8]
        now    = time.time()

        # Buy leg: spend USDC, receive asset
        usdc_spent = opp.position_usdc
        fee_buy    = usdc_spent * cfg.fee(opp.buy_exchange, "taker")
        asset_qty  = (usdc_spent - fee_buy) / opp.buy_vwap

        buy_trade = Trade(
            id=str(uuid.uuid4())[:8], arb_id=arb_id,
            symbol=opp.symbol, side="buy", exchange=opp.buy_exchange,
            price=opp.buy_vwap, amount=asset_qty,
            usdc_value=usdc_spent, fee_usdc=fee_buy,
            timestamp=now, paper=True,
        )

        # Sell leg: receive USDC, pay fee
        proceeds   = asset_qty * opp.sell_vwap
        fee_sell   = proceeds * cfg.fee(opp.sell_exchange, "taker")
        net_usdc   = proceeds - fee_sell

        sell_trade = Trade(
            id=str(uuid.uuid4())[:8], arb_id=arb_id,
            symbol=opp.symbol, side="sell", exchange=opp.sell_exchange,
            price=opp.sell_vwap, amount=asset_qty,
            usdc_value=net_usdc, fee_usdc=fee_sell,
            timestamp=now, paper=True,
        )

        # Update state
        pnl = net_usdc - usdc_spent
        self.state.cash_usdc    += pnl
        self.state.realized_pnl += pnl
        self.state.daily_pnl    += pnl
        self.state.daily_trades += 1
        self.state.total_trades += 1
        self._trades.extend([buy_trade, sell_trade])
        self.save()

        return buy_trade, sell_trade

    def record_trade(self, trade: Trade) -> None:
        """Called after a confirmed live order fill."""
        self._trades.append(trade)
        self.state.daily_trades += 1
        self.state.total_trades += 1
        self.save()

    def summary(self) -> str:
        s = self.state
        sign = lambda v: "+" if v >= 0 else ""
        return (
            f"Balance: ${s.cash_usdc:,.2f} | "
            f"Realized P&L: {sign(s.realized_pnl)}${s.realized_pnl:.4f} | "
            f"Daily P&L: {sign(s.daily_pnl)}${s.daily_pnl:.4f} | "
            f"Trades today: {s.daily_trades} | Total: {s.total_trades}"
        )


# ══════════════════════════════════════════════════════════════════════════════
# TRADER  (paper + live execution)
# ══════════════════════════════════════════════════════════════════════════════

class Trader:
    """
    Executes the two legs of an arbitrage.

    Paper mode  — instant virtual fill, portfolio updated immediately.
    Live mode   — places real market orders via CCXT as close to
                  simultaneously as possible (sequentially, not truly
                  parallel; true simultaneity requires async or threads).
                  If leg 2 fails, an emergency unwind of leg 1 is attempted.
    """

    def __init__(self, cb: ExchangeClient, kr: ExchangeClient,
                 portfolio: Portfolio, cfg: Config, log: logging.Logger):
        self._cb   = cb
        self._kr   = kr
        self._port = portfolio
        self._cfg  = cfg
        self._log  = log

    def execute(self, opp: ArbOpportunity) -> bool:
        if not self._cfg.live_mode:
            return self._paper_execute(opp)
        return self._live_execute(opp)

    def _paper_execute(self, opp: ArbOpportunity) -> bool:
        buy_t, sell_t = self._port.apply_arb(opp, self._cfg)
        pnl = sell_t.usdc_value - buy_t.usdc_value
        self._log.info(
            "[PAPER] Executed %s | BUY %s @ %.6f | SELL %s @ %.6f | P&L: %+.4f USDC",
            opp.symbol,
            opp.buy_exchange, opp.buy_vwap,
            opp.sell_exchange, opp.sell_vwap,
            pnl,
        )
        return True

    def _live_execute(self, opp: ArbOpportunity) -> bool:
        """
        ⚠️  LIVE — real money.
        Strategy: market orders on both legs (fastest execution, highest fill certainty).
        Retry each leg up to 3 times before aborting.
        """
        self._log.warning("⚠️  LIVE EXECUTION | %s", opp.summary)

        exes     = {"coinbase": self._cb, "kraken": self._kr}
        buy_ex   = exes[opp.buy_exchange]
        sell_ex  = exes[opp.sell_exchange]
        qty      = opp.position_usdc / opp.buy_vwap  # approximate base qty

        # ── Leg 1: Buy ────────────────────────────────────────────────────────
        buy_order = None
        for attempt in range(1, 4):
            buy_order = buy_ex.create_order(opp.symbol, "buy", qty)
            if buy_order:
                break
            self._log.warning("Buy attempt %d/3 failed — retrying…", attempt)
            time.sleep(0.5)

        if not buy_order:
            self._log.error("Buy leg failed after 3 attempts — aborting arb")
            return False

        # ── Leg 2: Sell ───────────────────────────────────────────────────────
        sell_order = None
        for attempt in range(1, 4):
            sell_order = sell_ex.create_order(opp.symbol, "sell", qty)
            if sell_order:
                break
            self._log.warning("Sell attempt %d/3 failed — retrying…", attempt)
            time.sleep(0.5)

        if not sell_order:
            self._log.error(
                "Sell leg failed — EMERGENCY UNWIND: selling %.6f %s on %s",
                qty, opp.symbol.split("/")[0], opp.buy_exchange,
            )
            buy_ex.create_order(opp.symbol, "sell", qty)   # attempt unwind
            return False

        self._log.info("✅ Live arb complete: %s", opp.summary)
        return True


# ══════════════════════════════════════════════════════════════════════════════
# ALERTS
# ══════════════════════════════════════════════════════════════════════════════

class Alerter:
    """
    Sends Telegram notifications for high-profit opportunities and trade results.
    Silently disabled if telegram.enabled=false or requests not installed.
    """

    def __init__(self, cfg: Config, log: logging.Logger):
        self._log     = log
        self._enabled = (
            bool(cfg.get("alerts", "telegram", "enabled", default=False))
            and _REQUESTS_AVAILABLE
        )
        self._token = cfg.get("alerts", "telegram", "bot_token", default="")
        self._chat  = cfg.get("alerts", "telegram", "chat_id",   default="")
        self._min   = float(cfg.get("alerts", "min_profit_to_alert_pct", default=1.0))

    def opportunity(self, opp: ArbOpportunity) -> None:
        if not self._enabled or opp.net_profit_pct < self._min:
            return
        self._send(
            f"🟢 *ARB DETECTED*\n"
            f"`{opp.symbol}`\n"
            f"Buy {opp.buy_exchange} @ `{opp.buy_price:.6f}`\n"
            f"Sell {opp.sell_exchange} @ `{opp.sell_price:.6f}`\n"
            f"Net: *{opp.net_profit_pct:+.3f}%* | Est. ${opp.estimated_profit_usdc:.2f}"
        )

    def trade_result(self, opp: ArbOpportunity, success: bool) -> None:
        if not self._enabled:
            return
        icon = "✅" if success else "❌"
        self._send(f"{icon} *{'EXECUTED' if success else 'FAILED'}*\n{opp.summary}")

    def _send(self, text: str) -> None:
        try:
            _requests.post(
                f"https://api.telegram.org/bot{self._token}/sendMessage",
                json={"chat_id": self._chat, "text": text, "parse_mode": "Markdown"},
                timeout=5,
            )
        except Exception as e:
            self._log.debug("Telegram alert failed: %s", e)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN BOT
# ══════════════════════════════════════════════════════════════════════════════

class ArbBot:
    """
    Orchestrates the full scan → evaluate → risk-check → execute loop.

    Scan cycle
    ──────────
    1. For each qualified pair, fetch order books from both exchanges.
    2. ArbCalculator checks both directions for net-positive opportunities.
    3. RiskManager approves or rejects each opportunity.
    4. Alerter fires Telegram notification if profit exceeds threshold.
    5. Trader executes (paper or live).
    6. Sleep scan_interval seconds, repeat.
    """

    def __init__(self, cfg: Config, log: logging.Logger, args):
        self._cfg    = cfg
        self._log    = log
        self._args   = args
        self._running = False

        # Instantiate components
        self._cb     = ExchangeClient("coinbase", cfg, log)
        self._kr     = ExchangeClient("kraken",   cfg, log)
        self._disc   = PairDiscovery(self._cb, self._kr, cfg, log)
        self._calc   = ArbCalculator(cfg, log)
        self._risk   = RiskManager(cfg, log)
        self._port   = Portfolio(cfg, log)
        self._trader = Trader(self._cb, self._kr, self._port, cfg, log)
        self._alert  = Alerter(cfg, log)

        self._pairs:      list = []
        self._scan_count: int  = 0
        self._opp_count:  int  = 0

    # ── Public entry point ────────────────────────────────────────────────────

    def start(self, dry_run: bool = False) -> None:
        mode = "PAPER" if not self._cfg.live_mode else "⚠️  LIVE"
        self._log.info("═" * 62)
        self._log.info("  Coinbase ↔ Kraken Arbitrage Bot  [%s MODE]", mode)
        self._log.info("═" * 62)

        if self._cfg.live_mode:
            self._log.warning(
                "LIVE MODE ACTIVE — real funds will be used. "
                "You have 5 seconds to abort (Ctrl+C)."
            )
            time.sleep(5)

        # Discover common, liquid pairs
        restrict     = getattr(self._args, "pairs", None)
        self._pairs  = self._disc.find_common_pairs(restrict or None)
        if not self._pairs:
            self._log.error(
                "No pairs passed the filters. "
                "Try lowering min_24h_volume_usdc or specifying --pairs explicitly."
            )
            return

        self._log.info("Scanning %d pairs: %s", len(self._pairs),
                       ", ".join(self._pairs[:15]) + ("…" if len(self._pairs) > 15 else ""))

        self._running = True
        signal.signal(signal.SIGINT,  self._handle_shutdown)
        signal.signal(signal.SIGTERM, self._handle_shutdown)

        try:
            while self._running:
                self._port.maybe_reset_daily()
                self._scan()
                self._scan_count += 1

                if dry_run:
                    self._log.info("Dry-run complete after 1 scan.")
                    break

                self._log.debug(
                    "Scan #%d done — sleeping %.1fs  |  %s",
                    self._scan_count, self._cfg.scan_interval, self._port.summary(),
                )
                time.sleep(self._cfg.scan_interval)

        except KeyboardInterrupt:
            pass
        finally:
            self._log.info("Final portfolio: %s", self._port.summary())
            self._port.save()
            self._log.info("Shutdown complete.")

    # ── Scan loop ─────────────────────────────────────────────────────────────

    def _scan(self) -> None:
        """
        Fetch all order books and evaluate opportunities.

        Each (exchange, symbol) pair is fetched in its own thread so a single
        slow or hung API call cannot block the entire scan.  A per-scan timeout
        of 30 s (or 5 s × number of pairs, whichever is larger) is enforced;
        any futures that haven't resolved by then are skipped.
        """
        depth = self._cfg.order_book_depth
        size  = self._risk.position_size(self._port.state)

        books_cb: dict = {}
        books_kr: dict = {}

        def fetch_one(client: ExchangeClient, sym: str):
            ob = client.fetch_order_book(sym, depth)
            return client.name, sym, ob

        scan_timeout = max(30.0, len(self._pairs) * 5.0)
        futures = {}
        with ThreadPoolExecutor(max_workers=len(self._pairs) * 2 or 2) as pool:
            for sym in self._pairs:
                if not self._running:
                    break
                futures[pool.submit(fetch_one, self._cb, sym)] = ("coinbase", sym)
                futures[pool.submit(fetch_one, self._kr, sym)] = ("kraken",   sym)

            try:
                for fut in as_completed(futures, timeout=scan_timeout):
                    ex_name, sym = futures[fut]
                    try:
                        _, _, ob = fut.result()
                        if ob:
                            if ex_name == "coinbase":
                                books_cb[sym] = ob
                            else:
                                books_kr[sym] = ob
                    except Exception as e:
                        self._log.warning("[%s] Order book error %s: %s", ex_name, sym, e)
            except FutureTimeoutError:
                pending = [v for f, v in futures.items() if not f.done()]
                self._log.warning(
                    "Scan timed out after %.0fs — %d fetch(es) still pending: %s",
                    scan_timeout, len(pending),
                    ", ".join(f"{ex}/{s}" for ex, s in pending),
                )

        self._log.info(
            "Order books: CB=%d/%d  KR=%d/%d",
            len(books_cb), len(self._pairs),
            len(books_kr), len(self._pairs),
        )

        # Evaluate opportunities for symbols with books from both exchanges
        for symbol in self._pairs:
            if not self._running:
                break
            ob_cb = books_cb.get(symbol)
            ob_kr = books_kr.get(symbol)
            if ob_cb is None or ob_kr is None:
                continue

            for opp in self._calc.evaluate_direct(symbol, ob_cb, ob_kr, size):
                self._handle_opportunity(opp)

        # Optional: same-exchange triangular arbitrage
        if self._cfg.get("strategy", "triangular", default=False):
            for tri in self._calc.evaluate_triangular(
                {"coinbase": books_cb, "kraken": books_kr}, size
            ):
                self._log.info(
                    "TRIANGULAR | %s | net=%.3f%% | est. $%.4f",
                    tri["path"], tri["net_profit_pct"], tri["estimated_profit_usdc"],
                )

    def _handle_opportunity(self, opp: ArbOpportunity) -> None:
        self._opp_count += 1
        self._log.info(
            "🔍 Opp #%d | %s | raw=%.3f%% fees=%.3f%% slip=%.3f%% → net=%+.3f%% | est.$%.4f",
            self._opp_count,
            opp.symbol,
            opp.raw_spread_pct,
            opp.fees_pct,
            opp.slippage_pct,
            opp.net_profit_pct,
            opp.estimated_profit_usdc,
        )
        self._log.info("        %s", opp.summary)

        # Fetch ticker for volatility gate
        ticker = self._cb.fetch_ticker(opp.symbol)

        if not self._risk.approve(opp, self._port.state, ticker):
            self._log.info("        ↳ Rejected by risk manager")
            return

        self._alert.opportunity(opp)
        success = self._trader.execute(opp)
        self._alert.trade_result(opp, success)

        if success:
            self._log.info("        ↳ %s", self._port.summary())

    def _handle_shutdown(self, sig, frame) -> None:
        self._log.info("Shutdown signal received — finishing current scan…")
        self._running = False


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING SETUP
# ══════════════════════════════════════════════════════════════════════════════

def setup_logging(cfg: Config) -> logging.Logger:
    level    = getattr(logging, str(cfg.get("logging", "level", default="INFO")), logging.INFO)
    log_file = str(cfg.get("logging", "log_file", default="arb_bot.log"))

    fmt = "%(asctime)s | %(levelname)-8s | %(message)s"
    logging.basicConfig(
        level=level,
        format=fmt,
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    # Quiet noisy third-party loggers
    for noisy in ("ccxt", "urllib3", "requests"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    return logging.getLogger("arb_bot")


# ══════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Coinbase ↔ Kraken cross-exchange arbitrage bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples
  python arb_bot.py                          # paper mode, auto-discover pairs
  python arb_bot.py --pairs BTC/USD ETH/USD  # restrict pairs
  python arb_bot.py --dry-run                # one scan, then exit
  python arb_bot.py --config live.yaml       # use a different config
        """,
    )
    parser.add_argument("--config",  default="config.yaml",
                        help="Path to YAML config (default: config.yaml)")
    parser.add_argument("--pairs",   nargs="+", metavar="PAIR",
                        help="Only scan these pairs, e.g. BTC/USD ETH/USD")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run exactly one scan then exit")
    args = parser.parse_args()

    cfg = Config(args.config)
    log = setup_logging(cfg)

    bot = ArbBot(cfg, log, args)
    bot.start(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
