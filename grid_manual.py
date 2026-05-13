# -*- coding: utf-8 -*-
"""
Grid Manual Mode — Multi-user, Multi-instrument Server Version
- User selects instrument (BTCUSD-PERP, CLUSD-PERP, etc.) and direction (LONG/SHORT/NEUTRAL)
- Unidirectional grid: LONG→BUY below / SHORT→SELL above
- R1 recenter configurable (default 60 min), R3 on-TP recenter
- Direction flip → confirm dialog, post-only close + rebuild opposite
- BTC: Binance 1m klines for sigma auto-spacing; Non-BTC: manual spacing only
- CRO market WS (bid/ask), CRO user WS (fills)
- Flask+SocketIO dashboard
- ENV vars: CRO_API_KEY, CRO_SECRET_KEY, SOURCE_IP, DASHBOARD_PORT, USER_ID
"""
import asyncio
import json
import logging
import math
import os
import signal
import sys
import io
import time
import threading
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiohttp
import websockets

# ========= Windows console ========= #
if sys.platform == "win32":
    try:
        if hasattr(sys.stdout, "buffer"):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", line_buffering=True)
        if hasattr(sys.stderr, "buffer"):
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", line_buffering=True)
    except Exception:
        pass
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ========= Imports from local modules ========= #
from configs import GRID_CONFIG, LOOKBACK, ORDER_QTY

# Server-mode: these come from ENV, not configs.py
SOURCE_IP = os.environ.get("SOURCE_IP", "")
DASHBOARD_PORT = int(os.environ.get("DASHBOARD_PORT", "5560"))
USER_ID = os.environ.get("USER_ID", "DEFAULT")

# ========= Source IP binding ========= #
import socket as _socket
import threading as _threading

_bind_tls = _threading.local()
_bound_ids: set[int] = set()

if SOURCE_IP:
    _orig_connect = _socket.socket.connect
    def _bound_connect(self, address):
        src = getattr(_bind_tls, 'source_ip', None)
        sid = id(self)
        if src and self.family == _socket.AF_INET and sid not in _bound_ids:
            try:
                self.bind((src, 0))
                _bound_ids.add(sid)
            except OSError:
                pass
        return _orig_connect(self, address)
    _socket.socket.connect = _bound_connect

# ========= Per-user data directory ========= #
_USER_DATA_DIR = Path(__file__).parent / "data" / USER_ID
_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

# ========= Logging ========= #
from logging.handlers import RotatingFileHandler
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(str(_USER_DATA_DIR / "grid_manual.log"), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(f"GridManual-{USER_ID}")

from cro_ws_order import CroWSOrderClient
from cro_rest_client import CroRestClient

# ============================================================
# Constants
# ============================================================
DEFAULT_INSTRUMENT = "BTCUSD-PERP"
BINANCE_KLINE_WS = "wss://fstream.binance.com/market/ws/btcusdt@kline_1m"
BINANCE_KLINE_REST = "https://fapi.binance.com/fapi/v1/klines"

TRADING_MINUTES = 960
MINUTES_PER_YEAR = 525600
RETRY_BASE_DELAY = 1.0
RETRY_MAX_DELAY = 30.0

STATE_FILE = _USER_DATA_DIR / "grid_manual_state.json"

# Default configurable parameters (overridable from dashboard)
DEFAULT_LEVELS = 20
DEFAULT_CORR_MULT = 0.75
DEFAULT_TP_MULT = 1.0
DEFAULT_MAX_LOTS = 60
DEFAULT_RECENTER_MINUTES = 60

# ============================================================
# Instrument Config — fetched from CRO API at startup
# ============================================================
_INSTRUMENT_CACHE: dict[str, dict] = {}

def _fetch_instrument_list() -> dict[str, dict]:
    """Fetch tradable PERP instruments from CRO public API. Returns {symbol: config}."""
    import urllib.request, ssl
    result = {}
    try:
        ctx = ssl.create_default_context()
        req = urllib.request.Request("https://api.crypto.com/exchange/v1/public/get-instruments")
        with urllib.request.urlopen(req, context=ctx, timeout=10) as resp:
            data = json.loads(resp.read().decode())
        if data.get("code") != 0:
            logger.error("CRO get-instruments API error: %s", data)
            return result
        for inst in data["result"]["data"]:
            if inst.get("inst_type") != "PERPETUAL_SWAP" or not inst.get("tradable"):
                continue
            sym = inst["symbol"]
            result[sym] = {
                "symbol": sym,
                "base_ccy": inst.get("base_ccy", ""),
                "price_tick": float(inst.get("price_tick_size", "0.1")),
                "qty_tick": float(inst.get("qty_tick_size", "0.0001")),
                "qty_decimals": int(inst.get("quantity_decimals", 4)),
                "quote_decimals": int(inst.get("quote_decimals", 1)),
                "max_leverage": inst.get("max_leverage", "50"),
            }
        logger.info("Fetched %d tradable PERP instruments from CRO", len(result))
    except Exception as e:
        logger.error("Failed to fetch CRO instruments: %s", e)
    return result

def _get_instrument_config(symbol: str) -> dict:
    """Get config for a specific instrument. Falls back to BTC defaults."""
    if symbol in _INSTRUMENT_CACHE:
        return _INSTRUMENT_CACHE[symbol]
    # BTC fallback
    return {
        "symbol": symbol,
        "base_ccy": symbol.replace("USD-PERP", ""),
        "price_tick": 0.1,
        "qty_tick": 0.0001,
        "qty_decimals": 4,
        "quote_decimals": 1,
        "max_leverage": "100",
    }

def _is_btc(symbol: str) -> bool:
    return symbol == "BTCUSD-PERP"


def _make_http_session(**kwargs) -> aiohttp.ClientSession:
    local_addr = (SOURCE_IP, 0) if SOURCE_IP else None
    connector = aiohttp.TCPConnector(local_addr=local_addr)
    return aiohttp.ClientSession(connector=connector, **kwargs)


# ============================================================
# Data Structures
# ============================================================

@dataclass
class EntrySlot:
    price: float
    side: str  # "BUY" or "SELL"
    order_id: Optional[str] = None


@dataclass
class ActiveLot:
    entry_price: float
    side: str
    qty: float
    tp_price: float
    tp_order_id: Optional[str] = None
    filled_at: float = 0.0


@dataclass
class EngineState:
    direction: int = 0  # 1=LONG, -1=SHORT, 0=none
    direction_since: str = ""
    spacing: float = 0.0
    last_recenter: float = 0.0
    total_pnl: float = 0.0
    roundtrips: int = 0
    flip_count: int = 0
    flip_pnl: float = 0.0
    net_position: float = 0.0
    total_volume_usd: float = 0.0


@dataclass
class GridSettings:
    """User-configurable grid parameters."""
    instrument: str = DEFAULT_INSTRUMENT
    levels: int = DEFAULT_LEVELS
    corr_mult: float = DEFAULT_CORR_MULT
    tp_mult: float = DEFAULT_TP_MULT
    max_lots: int = DEFAULT_MAX_LOTS
    recenter_minutes: int = DEFAULT_RECENTER_MINUTES
    spacing_mode: str = "auto"  # "auto" or "manual"
    manual_spacing: float = 50.0  # only used when spacing_mode == "manual"


# ============================================================
# Utility functions
# ============================================================

def clamp_tick(price: float, price_tick: float = 0.1) -> float:
    decimals = max(0, -int(math.floor(math.log10(price_tick)))) if price_tick > 0 else 1
    return round(round(price / price_tick) * price_tick, decimals)


def compute_spacing(price: float, sigma: float, corr_mult: float, levels: int,
                    price_tick: float = 0.1, min_spacing: float = 5.0, max_spacing: float = 150.0) -> float:
    grid_sigma = max(0.01, sigma)
    corridor = price * grid_sigma * math.sqrt(TRADING_MINUTES / MINUTES_PER_YEAR) * corr_mult * 2.0
    raw = corridor / levels
    return max(min_spacing, min(max_spacing, clamp_tick(raw, price_tick)))


# ============================================================
# BinanceKlineFeed — sigma only (no OI, no funding)
# ============================================================

class BinanceKlineFeed:
    def __init__(self):
        self.candles: deque = deque(maxlen=max(LOOKBACK, 30))
        self.latest_price: float = 0.0
        self._running = True

    async def bootstrap(self):
        url = f"{BINANCE_KLINE_REST}?symbol=BTCUSDT&interval=1m&limit={max(LOOKBACK, 30)}"
        try:
            async with _make_http_session() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    for k in data:
                        h, l, c = float(k[2]), float(k[3]), float(k[4])
                        self.candles.append((h, l, c))
                        self.latest_price = c
            logger.info("Kline bootstrap: %d candles, price=%.1f", len(self.candles), self.latest_price)
        except Exception as e:
            logger.error("Kline bootstrap failed: %s", e)

    async def run(self):
        backoff = RETRY_BASE_DELAY
        while self._running:
            try:
                async with websockets.connect(
                    BINANCE_KLINE_WS, ping_interval=15, ping_timeout=10, close_timeout=5
                ) as ws:
                    backoff = RETRY_BASE_DELAY
                    logger.info("Binance kline WS connected")
                    async for msg in ws:
                        if not self._running:
                            break
                        data = json.loads(msg)
                        k = data.get("k", {})
                        self.latest_price = float(k.get("c", self.latest_price))
                        if k.get("x"):
                            h, l, c = float(k["h"]), float(k["l"]), float(k["c"])
                            self.candles.append((h, l, c))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Kline WS error: %s", e)
            if self._running:
                await asyncio.sleep(min(backoff, RETRY_MAX_DELAY))
                backoff = min(backoff * 2, RETRY_MAX_DELAY)

    def calc_sigma(self) -> float:
        n = len(self.candles)
        if n < 5:
            return 0.5
        valid = [(h, l) for h, l, c in self.candles if l > 0 and h > 0 and h >= l]
        if len(valid) < 5:
            return 0.5
        log_hl = [math.log(h / l) for h, l in valid]
        coeff = 1.0 / (4.0 * len(valid) * math.log(2))
        raw = math.sqrt(coeff * sum(x ** 2 for x in log_hl))
        return raw * math.sqrt(MINUTES_PER_YEAR)

    def stop(self):
        self._running = False


# ============================================================
# CroMarketFeed — best bid/ask from CRO ticker WS
# ============================================================

class CroMarketFeed:
    def __init__(self, ws_market_url: str, instrument: str = DEFAULT_INSTRUMENT):
        self.ws_market_url = ws_market_url
        self.instrument = instrument
        self.best_bid: float = 0.0
        self.best_ask: float = 0.0
        self._running = True
        self._current_ws = None  # reference for force_reconnect

    async def force_reconnect(self):
        """Close current WS connection so run() re-subscribes with updated instrument."""
        ws = self._current_ws
        if ws is not None and not ws.closed:
            await ws.close()

    async def run(self):
        backoff = RETRY_BASE_DELAY
        while self._running:
            try:
                async with _make_http_session() as session:
                    async with session.ws_connect(self.ws_market_url, heartbeat=25) as ws:
                        self._current_ws = ws
                        backoff = RETRY_BASE_DELAY
                        logger.info("CRO market WS connected: %s (instrument=%s)", self.ws_market_url, self.instrument)
                        await asyncio.sleep(1.0)
                        sub = {
                            "id": int(time.time() * 1000),
                            "method": "subscribe",
                            "params": {"channels": [f"ticker.{self.instrument}"]},
                            "nonce": int(time.time() * 1000),
                        }
                        await ws.send_str(json.dumps(sub))
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if data.get("method") == "public/heartbeat":
                                    await ws.send_str(json.dumps({
                                        "id": data.get("id"),
                                        "method": "public/respond-heartbeat",
                                    }))
                                    continue
                                result = data.get("result") or {}
                                channel = result.get("channel") or result.get("subscription") or ""
                                if "ticker" in channel:
                                    items = result.get("data") or []
                                    if isinstance(items, list) and items:
                                        tick = items[0]
                                    elif isinstance(items, dict):
                                        tick = items
                                    else:
                                        continue
                                    b = tick.get("b") or tick.get("best_bid")
                                    a = tick.get("k") or tick.get("best_ask")
                                    if b:
                                        self.best_bid = float(b)
                                    if a:
                                        self.best_ask = float(a)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("CRO market WS error: %s", e)
            finally:
                self._current_ws = None
            if self._running:
                await asyncio.sleep(min(backoff, RETRY_MAX_DELAY))
                backoff = min(backoff * 2, RETRY_MAX_DELAY)

    def stop(self):
        self._running = False


# ============================================================
# ManualGridEngine
# ============================================================

class ManualGridEngine:
    def __init__(
        self,
        kline_feed: BinanceKlineFeed,
        market_feed: CroMarketFeed,
        ws_client: CroWSOrderClient,
        rest_client: CroRestClient,
    ):
        self.kline = kline_feed
        self.market = market_feed
        self.ws = ws_client
        self.rest = rest_client

        self.state = EngineState()
        self.settings = GridSettings()
        self.order_qty: str = ORDER_QTY
        self.entry_slots: list[EntrySlot] = []
        self.active_lots: list[ActiveLot] = []

        self._entry_oid_map: dict[str, int] = {}
        self._tp_oid_map: dict[str, int] = {}

        self._activity_log: deque = deque(maxlen=100)
        self._lock = asyncio.Lock()
        self._grid_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self.started = False
        self._was_started = False
        self._closing_position = False
        self._unrealized_pnl: float = 0.0
        self._balance_usd: float = 0.0
        self._started_at: float = time.time()
        self._update_instrument_config()

        self._close_filled_event: Optional[asyncio.Event] = None
        self._close_order_filled: bool = False
        self._close_order_id: Optional[str] = None

        self._cro_ws_ready = False

    @property
    def instrument(self) -> str:
        return self.settings.instrument

    def _update_instrument_config(self):
        """Refresh instrument-specific parameters from cache."""
        cfg = _get_instrument_config(self.settings.instrument)
        self._price_tick: float = cfg["price_tick"]
        self._qty_decimals: int = cfg["qty_decimals"]
        self._qty_tick: float = cfg["qty_tick"]
        # Dynamic spacing bounds based on price tick
        self._min_spacing: float = self._price_tick * 10
        self._max_spacing: float = self._price_tick * 100000

    def _clamp(self, price: float) -> float:
        return clamp_tick(price, self._price_tick)

    def _get_cro_price(self) -> float:
        if self.market.best_bid > 0 and self.market.best_ask > 0:
            return (self.market.best_bid + self.market.best_ask) / 2.0
        if self.market.best_bid > 0:
            return self.market.best_bid
        return 0.0

    def _fmt_price(self, price: float) -> str:
        """Format price with correct decimal places for current instrument."""
        decimals = max(0, -int(math.floor(math.log10(self._price_tick)))) if self._price_tick > 0 else 1
        return f"{price:.{decimals}f}"

    def _log_activity(self, msg: str):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {msg}"
        self._activity_log.appendleft(entry)
        logger.info(msg)

    # ---- Grid construction ----

    def _build_grid(self, price: float, direction: int) -> list[EntrySlot]:
        if self.settings.spacing_mode == "manual" or not _is_btc(self.instrument):
            spacing = max(self._min_spacing, min(self._max_spacing, self._clamp(self.settings.manual_spacing)))
        else:
            sigma = self.kline.calc_sigma()
            spacing = compute_spacing(price, sigma, self.settings.corr_mult, self.settings.levels,
                                      self._price_tick, self._min_spacing, self._max_spacing)
        self.state.spacing = spacing

        occupied = {lot.entry_price for lot in self.active_lots}
        slots = []

        if direction == 0:
            # Bidirectional grid: half BUY below, half SELL above
            half = self.settings.levels // 2
            for i in range(half):
                gp = self._clamp(price - (i + 1) * spacing)
                if gp not in occupied:
                    slots.append(EntrySlot(price=gp, side="BUY"))
            for i in range(half):
                gp = self._clamp(price + (i + 1) * spacing)
                if gp not in occupied:
                    slots.append(EntrySlot(price=gp, side="SELL"))
            dir_label = "NEUTRAL"
        else:
            side = "BUY" if direction == 1 else "SELL"
            for i in range(self.settings.levels):
                if direction == 1:
                    gp = self._clamp(price - (i + 1) * spacing)
                else:
                    gp = self._clamp(price + (i + 1) * spacing)
                if gp not in occupied:
                    slots.append(EntrySlot(price=gp, side=side))
            dir_label = "LONG" if direction == 1 else "SHORT"

        self._log_activity(
            f"Grid built: dir={dir_label} "
            f"center={self._fmt_price(price)} spacing={self._fmt_price(spacing)} slots={len(slots)}"
        )
        return slots

    # ---- Order placement ----

    async def _place_entry_orders(self):
        if not self._cro_ws_ready:
            self._log_activity("CRO WS not connected, skipping order placement")
            return
        placed = 0
        for idx, slot in enumerate(self.entry_slots):
            if slot.order_id is not None:
                continue
            if len(self.active_lots) >= self.settings.max_lots:
                break
            price_str = self._fmt_price(slot.price)
            if slot.side == "BUY" and self.market.best_bid > 0 and slot.price >= self.market.best_bid:
                continue
            if slot.side == "SELL" and self.market.best_ask > 0 and slot.price <= self.market.best_ask:
                continue
            result = await self.ws.place_order(
                instrument=self.instrument, side=slot.side, quantity=self.order_qty,
                order_type="LIMIT", price=price_str, post_only=True,
            )
            if result:
                oid = result.get("order_id")
                if oid:
                    slot.order_id = oid
                    self._entry_oid_map[oid] = idx
                    placed += 1
            await asyncio.sleep(0.05)
        if placed:
            self._log_activity(f"Placed {placed} entry orders")

    async def _place_tp_order(self, lot_idx: int):
        if not self._cro_ws_ready:
            return
        if lot_idx >= len(self.active_lots):
            return
        lot = self.active_lots[lot_idx]
        if lot.tp_order_id is not None:
            return

        tp_side = "SELL" if lot.side == "BUY" else "BUY"
        tp_price = self._clamp(lot.tp_price)

        if tp_side == "BUY" and self.market.best_ask > 0 and tp_price >= self.market.best_ask:
            tp_price = self._clamp(self.market.best_bid)
        elif tp_side == "SELL" and self.market.best_bid > 0 and tp_price <= self.market.best_bid:
            tp_price = self._clamp(self.market.best_ask)

        for attempt in range(5):
            result = await self.ws.place_order(
                instrument=self.instrument, side=tp_side, quantity=self.order_qty,
                order_type="LIMIT", price=self._fmt_price(tp_price), post_only=True,
            )
            if result:
                oid = result.get("order_id")
                if oid:
                    lot.tp_order_id = oid
                    self._tp_oid_map[oid] = lot_idx
                    self._log_activity(f"TP placed lot#{lot_idx} {tp_side} @ {self._fmt_price(tp_price)}")
                    return
            await asyncio.sleep(0.3)
            if tp_side == "BUY" and self.market.best_bid > 0:
                tp_price = self._clamp(self.market.best_bid)
            elif tp_side == "SELL" and self.market.best_ask > 0:
                tp_price = self._clamp(self.market.best_ask)

        self._log_activity(f"TP failed lot#{lot_idx} after 5 retries")

    # ---- Fill handling ----

    async def on_order_update(self, order: dict):
        async with self._lock:
            order_id = order.get("order_id", "")
            status = (order.get("status") or "").upper()
            side = (order.get("side") or "").upper()
            fill_price = float(order.get("avg_price") or order.get("trigger_price") or "0")

            if self._close_order_id and order_id == self._close_order_id and status == "FILLED":
                self._close_order_filled = True
                if self._close_filled_event:
                    self._close_filled_event.set()

            if order_id in self._entry_oid_map:
                slot_idx = self._entry_oid_map[order_id]
                if status == "FILLED":
                    self._entry_oid_map.pop(order_id, None)
                    slot = None
                    if slot_idx < len(self.entry_slots):
                        slot = self.entry_slots[slot_idx]
                    if slot is None:
                        return

                    actual_price = fill_price if fill_price > 0 else slot.price
                    qty = float(self.order_qty)

                    if slot.side == "BUY":
                        tp_p = self._clamp(actual_price + self.state.spacing * self.settings.tp_mult)
                    else:
                        tp_p = self._clamp(actual_price - self.state.spacing * self.settings.tp_mult)

                    lot = ActiveLot(
                        entry_price=actual_price, side=slot.side, qty=qty,
                        tp_price=tp_p, filled_at=time.time(),
                    )
                    self.active_lots.append(lot)
                    lot_idx_new = len(self.active_lots) - 1

                    if side == "BUY":
                        self.state.net_position += qty
                    else:
                        self.state.net_position -= qty
                    self.state.total_volume_usd += qty * actual_price

                    self.entry_slots.pop(slot_idx)
                    self._rebuild_entry_oid_map()

                    self._log_activity(
                        f"Entry fill {side} @ {self._fmt_price(actual_price)} net={self.state.net_position:.4f} "
                        f"lots={len(self.active_lots)}"
                    )
                    await self._place_tp_order(lot_idx_new)

                elif status == "REJECTED":
                    self._entry_oid_map.pop(order_id, None)
                    if slot_idx < len(self.entry_slots):
                        self.entry_slots[slot_idx].order_id = None
                elif status in ("CANCELED", "CANCELLED"):
                    self._entry_oid_map.pop(order_id, None)
                    if slot_idx < len(self.entry_slots):
                        self.entry_slots[slot_idx].order_id = None
                return

            if order_id in self._tp_oid_map:
                lot_idx = self._tp_oid_map[order_id]
                if status == "FILLED":
                    self._tp_oid_map.pop(order_id, None)
                    if lot_idx >= len(self.active_lots):
                        return
                    lot = self.active_lots[lot_idx]

                    tp_actual = fill_price if fill_price > 0 else lot.tp_price
                    if lot.side == "BUY":
                        pnl = (tp_actual - lot.entry_price) * lot.qty
                    else:
                        pnl = (lot.entry_price - tp_actual) * lot.qty
                    self.state.total_pnl += pnl
                    self.state.roundtrips += 1

                    if side == "BUY":
                        self.state.net_position += lot.qty
                    else:
                        self.state.net_position -= lot.qty
                    self.state.total_volume_usd += lot.qty * tp_actual

                    self._log_activity(
                        f"TP fill lot#{lot_idx} {side} @ {self._fmt_price(tp_actual)} pnl=+{pnl:.4f} "
                        f"total={self.state.total_pnl:.4f} trips={self.state.roundtrips}"
                    )

                    self.active_lots.pop(lot_idx)
                    self._rebuild_tp_oid_map()

                    if not self._closing_position and self.started:
                        await self._r3_recenter_after_tp(lot.side)

                elif status == "REJECTED":
                    self._tp_oid_map.pop(order_id, None)
                    if lot_idx < len(self.active_lots):
                        self.active_lots[lot_idx].tp_order_id = None
                        await self._place_tp_order(lot_idx)
                elif status in ("CANCELED", "CANCELLED"):
                    self._tp_oid_map.pop(order_id, None)
                    if lot_idx < len(self.active_lots):
                        self.active_lots[lot_idx].tp_order_id = None

    def _rebuild_tp_oid_map(self):
        self._tp_oid_map.clear()
        for i, lot in enumerate(self.active_lots):
            if lot.tp_order_id:
                self._tp_oid_map[lot.tp_order_id] = i

    def _rebuild_entry_oid_map(self):
        self._entry_oid_map.clear()
        for i, slot in enumerate(self.entry_slots):
            if slot.order_id:
                self._entry_oid_map[slot.order_id] = i

    # ---- R3: on-TP recenter ----

    async def _r3_recenter_after_tp(self, filled_side: str = ""):
        if len(self.active_lots) >= self.settings.max_lots:
            return
        price = self._get_cro_price()
        if price <= 0:
            return

        direction = self.state.direction
        spacing = self.state.spacing
        if spacing <= 0:
            return

        occupied = {s.price for s in self.entry_slots if s.order_id}
        occupied.update(lot.entry_price for lot in self.active_lots)

        # Determine which side to replenish
        if direction == 0:
            # Neutral: replenish same side as the lot that just TP'd
            side = filled_side if filled_side in ("BUY", "SELL") else "BUY"
        else:
            side = "BUY" if direction == 1 else "SELL"

        for j in range(1, self.settings.levels * 3):
            if side == "BUY":
                candidate = self._clamp(price - j * spacing)
            else:
                candidate = self._clamp(price + j * spacing)
            if candidate not in occupied:
                new_slot = EntrySlot(price=candidate, side=side)
                self.entry_slots.append(new_slot)
                idx = len(self.entry_slots) - 1
                result = await self.ws.place_order(
                    instrument=self.instrument, side=side, quantity=self.order_qty,
                    order_type="LIMIT", price=self._fmt_price(candidate), post_only=True,
                )
                if result:
                    oid = result.get("order_id")
                    if oid:
                        new_slot.order_id = oid
                        self._entry_oid_map[oid] = idx
                        self._log_activity(f"R3 new entry {side} @ {self._fmt_price(candidate)}")
                return

    # ---- R1: periodic recenter ----

    async def recenter_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                if not self.started or self._closing_position:
                    continue
                await self._repair_orphan_tp()
                if not self.state.direction_since:
                    continue
                if self.state.last_recenter <= 0:
                    continue
                elapsed = time.time() - self.state.last_recenter
                rc_threshold = self.settings.recenter_minutes * 60
                if elapsed >= rc_threshold:
                    logger.info("R1 recenter: elapsed=%.0f / %.0f", elapsed, rc_threshold)
                    await self._r1_recenter()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("recenter_loop error: %s", e)

    async def _repair_orphan_tp(self):
        if not self._cro_ws_ready or not self.active_lots:
            return
        for i, lot in enumerate(self.active_lots):
            if lot.tp_order_id is None:
                if not self._cro_ws_ready:
                    return
                logger.info("Repairing orphan TP for lot#%d entry=%.1f", i, lot.entry_price)
                await self._place_tp_order(i)

    async def _r1_recenter(self):
        if self._grid_lock.locked():
            return
        async with self._grid_lock:
            price = self._get_cro_price()
            if price <= 0:
                return

            for slot in self.entry_slots:
                if slot.order_id:
                    try:
                        await self.ws.cancel_order(order_id=slot.order_id)
                    except Exception:
                        pass
                    self._entry_oid_map.pop(slot.order_id, None)
            self.entry_slots.clear()

            self.entry_slots = self._build_grid(price, self.state.direction)
            self._rebuild_entry_oid_map()
            await self._place_entry_orders()
            self.state.last_recenter = time.time()
            self._log_activity(f"R1 recenter done @ {self._fmt_price(price)}")

    # ---- Post-only close (chase best price) ----

    async def _post_only_close(self, side: str, qty: float, label: str) -> float:
        qty_str = f"{qty:.{self._qty_decimals}f}"
        price = self.market.best_ask if side == "SELL" else self.market.best_bid
        if price <= 0:
            self._log_activity(f"[{label}] No market price, falling back to market order")
            await self.ws.place_order(
                instrument=self.instrument, side=side, quantity=qty_str, order_type="MARKET",
            )
            return price

        price = self._clamp(price)
        self._close_order_filled = False
        self._close_order_id = None
        self._close_filled_event = asyncio.Event()

        result = await self.ws.place_order(
            instrument=self.instrument, side=side, quantity=qty_str,
            order_type="LIMIT", price=self._fmt_price(price), post_only=True,
        )
        if not result or not result.get("order_id"):
            self._log_activity(f"[{label}] Post-only place failed, using market")
            await self.ws.place_order(
                instrument=self.instrument, side=side, quantity=qty_str, order_type="MARKET",
            )
            self._close_filled_event = None
            self._close_order_id = None
            return price

        order_id = result["order_id"]
        self._close_order_id = order_id
        current_price = price
        self._log_activity(f"[{label}] Post-only {side} {qty_str} @ {self._fmt_price(price)}")

        max_chase = 100
        for _ in range(max_chase):
            try:
                await asyncio.wait_for(self._close_filled_event.wait(), timeout=0.3)
                if self._close_order_filled:
                    self._log_activity(f"[{label}] Filled @ {self._fmt_price(current_price)}")
                    self._close_filled_event = None
                    self._close_order_id = None
                    return current_price
            except asyncio.TimeoutError:
                pass

            new_price = self.market.best_ask if side == "SELL" else self.market.best_bid
            if new_price <= 0:
                continue
            new_price = self._clamp(new_price)
            if new_price != current_price:
                try:
                    await self.ws.amend_order(
                        order_id=order_id,
                        new_price=self._fmt_price(new_price),
                        new_quantity=qty_str,
                    )
                    current_price = new_price
                except Exception as e:
                    logger.warning("[%s] Amend error: %s", label, e)

            self._close_filled_event.clear()

        self._log_activity(f"[{label}] Chase timeout, using market")
        try:
            await self.ws.cancel_order(order_id=order_id)
        except Exception:
            pass
        await asyncio.sleep(0.3)
        pos = await self.rest.get_positions(self.instrument)
        if abs(pos) >= self._qty_tick:
            residual_qty = f"{abs(pos):.{self._qty_decimals}f}"
            residual_side = "SELL" if pos > 0 else "BUY"
            await self.ws.place_order(
                instrument=self.instrument, side=residual_side, quantity=residual_qty, order_type="MARKET",
            )
        self._close_filled_event = None
        self._close_order_id = None
        return current_price

    # ---- Manual direction change ----

    async def set_direction(self, new_direction: int) -> dict:
        """Set direction manually. 1=LONG, 0=NEUTRAL, -1=SHORT."""
        if new_direction not in (1, 0, -1):
            return {"status": "error", "message": "Direction must be 1, 0, or -1"}

        if not self.started:
            return {"status": "error", "message": "Engine not started. Press START first."}

        # "Already X" only if direction was explicitly set (has direction_since)
        if new_direction == self.state.direction and self.state.direction_since:
            dir_name = {1: "LONG", 0: "NEUTRAL", -1: "SHORT"}[new_direction]
            return {"status": "ok", "message": f"Already {dir_name}"}

        dir_names = {1: "LONG", 0: "NEUTRAL", -1: "SHORT"}
        dir_label = dir_names[new_direction]

        # Has existing lots? Need to flip (close lots first, then rebuild)
        if len(self.active_lots) > 0:
            await self._execute_flip(new_direction)
            return {"status": "ok", "message": f"Flipped to {dir_label}"}

        # Has entry orders but no lots? Cancel entries first, then rebuild
        if self.entry_slots:
            async with self._grid_lock:
                for slot in self.entry_slots:
                    if slot.order_id:
                        try:
                            await self.ws.cancel_order(order_id=slot.order_id)
                        except Exception:
                            pass
                        self._entry_oid_map.pop(slot.order_id, None)
                self.entry_slots.clear()
                self._rebuild_entry_oid_map()

        # Set direction and build grid
        self.state.direction = new_direction
        self.state.direction_since = datetime.now(timezone.utc).isoformat()
        price = self._get_cro_price()
        if price > 0:
            async with self._grid_lock:
                self.entry_slots = self._build_grid(price, new_direction)
                self._rebuild_entry_oid_map()
                await self._place_entry_orders()
                self.state.last_recenter = time.time()
        self._log_activity(f"Direction set: {dir_label}")
        self.save_state()
        return {"status": "ok", "message": f"Set to {dir_label}"}

    async def _execute_flip(self, new_direction: int):
        if self._grid_lock.locked():
            return
        async with self._grid_lock:
            self._closing_position = True
            old_dir = self.state.direction
            _dn = {1: "LONG", 0: "NEUTRAL", -1: "SHORT"}
            self._log_activity(f"FLIP: {_dn.get(old_dir, '?')} → {_dn.get(new_direction, '?')}")

            try:
                await self.rest.cancel_all_orders(self.instrument)
                self._entry_oid_map.clear()
                self._tp_oid_map.clear()
                for slot in self.entry_slots:
                    slot.order_id = None
                for lot in self.active_lots:
                    lot.tp_order_id = None
                await asyncio.sleep(0.5)

                pos = await self.rest.get_positions(self.instrument)
                self.state.net_position = pos
                flip_pnl = 0.0

                if abs(pos) >= self._qty_tick:
                    close_side = "SELL" if pos > 0 else "BUY"
                    close_qty = abs(pos)
                    close_price = await self._post_only_close(close_side, close_qty, "flip")

                    if close_price > 0:
                        for lot in self.active_lots:
                            if lot.side == "BUY":
                                flip_pnl += (close_price - lot.entry_price) * lot.qty
                            else:
                                flip_pnl += (lot.entry_price - close_price) * lot.qty

                    await asyncio.sleep(0.5)
                    pos = await self.rest.get_positions(self.instrument)
                    if abs(pos) >= self._qty_tick:
                        residual_side = "SELL" if pos > 0 else "BUY"
                        await self.ws.place_order(
                            instrument=self.instrument, side=residual_side,
                            quantity=f"{abs(pos):.{self._qty_decimals}f}", order_type="MARKET",
                        )
                        await asyncio.sleep(0.5)
                        pos = await self.rest.get_positions(self.instrument)

                self.state.net_position = pos
                self.state.flip_pnl += flip_pnl
                self.state.total_pnl += flip_pnl
                self.state.flip_count += 1

                self.entry_slots.clear()
                self.active_lots.clear()
                self._entry_oid_map.clear()
                self._tp_oid_map.clear()

                self.state.direction = new_direction
                self.state.direction_since = datetime.now(timezone.utc).isoformat()

                price = self._get_cro_price()
                if price > 0:
                    self.entry_slots = self._build_grid(price, new_direction)
                    self._rebuild_entry_oid_map()
                    await self._place_entry_orders()
                    self.state.last_recenter = time.time()

                self._log_activity(
                    f"Flip complete: dir={_dn.get(new_direction, '?')} "
                    f"flip_pnl={flip_pnl:.4f} total={self.state.total_pnl:.4f}"
                )
                self.save_state()

            finally:
                self._closing_position = False

    # ---- Start / Stop / Rebuild / Close ----

    async def start_grid(self):
        if self._grid_lock.locked():
            self._log_activity("Grid operation in progress, skipping START")
            return
        async with self._grid_lock:
            await self._start_grid_inner()

    async def _start_grid_inner(self):
        if self.started:
            self._log_activity("Already started, ignoring")
            return

        if not self._cro_ws_ready:
            self._log_activity("CRO WS not connected — configure API key in Settings")
            return

        if self.market.best_bid <= 0 or self.market.best_ask <= 0:
            self._log_activity("Waiting for CRO market data...")
            for _ in range(30):
                await asyncio.sleep(1)
                if self.market.best_bid > 0 and self.market.best_ask > 0:
                    break
            if self.market.best_bid <= 0:
                self._log_activity("ERROR: CRO market WS not connected")
                return

        await self.rest.cancel_all_orders(self.instrument)
        await asyncio.sleep(1)

        pos = await self.rest.get_positions(self.instrument)
        self.state.net_position = pos

        self.started = True
        self._stop_event.clear()
        if self.state.last_recenter <= 0:
            self.state.last_recenter = time.time()

        # If direction was saved from last session, rebuild grid
        _dn = {1: "LONG", 0: "NEUTRAL", -1: "SHORT"}
        if self.state.direction_since:
            # Has a saved direction (including NEUTRAL) — rebuild grid
            price = self._get_cro_price()
            if price > 0:
                self.entry_slots = self._build_grid(price, self.state.direction)
                self._rebuild_entry_oid_map()
                await self._place_entry_orders()

                for i, lot in enumerate(self.active_lots):
                    if lot.tp_order_id is None:
                        await self._place_tp_order(i)

            self._log_activity(
                f"Grid STARTED dir={_dn.get(self.state.direction, '?')} "
                f"pos={pos:.4f} pnl={self.state.total_pnl:.4f}"
            )
        else:
            self._log_activity(
                f"Grid STARTED (select direction) pos={pos:.4f}"
            )

        self.save_state()

    async def cancel_and_rebuild(self):
        if self._grid_lock.locked():
            return
        async with self._grid_lock:
            if not self.started:
                self._log_activity("Not started, cannot rebuild")
                return

            self._closing_position = True
            try:
                await self.rest.cancel_all_orders(self.instrument)
                self._entry_oid_map.clear()
                self._tp_oid_map.clear()
                self.entry_slots.clear()
                for lot in self.active_lots:
                    lot.tp_order_id = None
                await asyncio.sleep(0.5)

                pos = await self.rest.get_positions(self.instrument)
                close_pnl = 0.0
                if abs(pos) >= self._qty_tick:
                    detail = await self.rest.get_position_detail(self.instrument)
                    if detail:
                        close_pnl = float(detail.get("session_unrealized_pnl") or "0")
                    close_side = "SELL" if pos > 0 else "BUY"
                    await self._post_only_close(close_side, abs(pos), "rebuild")
                    self.state.total_pnl += close_pnl

                self.active_lots.clear()
                self._tp_oid_map.clear()

                await asyncio.sleep(0.5)
                pos = await self.rest.get_positions(self.instrument)
                self.state.net_position = pos

                if self.state.direction_since:
                    price = self._get_cro_price()
                    if price > 0:
                        self.entry_slots = self._build_grid(price, self.state.direction)
                        self._rebuild_entry_oid_map()
                        await self._place_entry_orders()
                        self.state.last_recenter = time.time()

                self._log_activity(f"Rebuild complete, close_pnl={close_pnl:.4f}")
                self.save_state()
            finally:
                self._closing_position = False

    def stop(self):
        self._stop_event.set()
        self.started = False

    async def close_position(self) -> dict:
        if self._closing_position:
            return {"status": "busy", "message": "Already closing"}
        self._closing_position = True
        try:
            await self.rest.cancel_all_orders(self.instrument)
            self._entry_oid_map.clear()
            self._tp_oid_map.clear()
            self.entry_slots.clear()
            for lot in self.active_lots:
                lot.tp_order_id = None

            pos = await self.rest.get_positions(self.instrument)
            self.state.net_position = pos

            if abs(pos) < self._qty_tick:
                self.active_lots.clear()
                if self.started:
                    self._stop_event.set()
                    self.started = False
                self.save_state()
                self._closing_position = False
                return {"status": "flat", "message": "No position"}

            close_pnl = 0.0
            detail = await self.rest.get_position_detail(self.instrument)
            if detail:
                close_pnl = float(detail.get("session_unrealized_pnl") or "0")

            close_side = "SELL" if pos > 0 else "BUY"
            await self._post_only_close(close_side, abs(pos), "close")

            self.state.total_pnl += close_pnl
            self.state.net_position = 0.0
            self.active_lots.clear()
            self._tp_oid_map.clear()

            if self.started:
                self._stop_event.set()
                self.started = False
            self.save_state()
            self._closing_position = False
            return {"status": "closed", "message": f"Closed pnl={close_pnl:.4f}"}

        except Exception as e:
            logger.error("close_position error: %s", e)
            self._closing_position = False
            return {"status": "error", "message": str(e)}

    # ---- State persistence ----

    def save_state(self):
        try:
            data = {
                "started": self.started,
                "instrument": self.instrument,
                "order_qty": self.order_qty,
                "direction": self.state.direction,
                "direction_since": self.state.direction_since,
                "spacing": self.state.spacing,
                "last_recenter": self.state.last_recenter,
                "total_pnl": self.state.total_pnl,
                "roundtrips": self.state.roundtrips,
                "flip_count": self.state.flip_count,
                "flip_pnl": self.state.flip_pnl,
                "net_position": self.state.net_position,
                "total_volume_usd": self.state.total_volume_usd,
                "entry_slots": [
                    {"price": s.price, "side": s.side, "order_id": s.order_id}
                    for s in self.entry_slots
                ],
                "active_lots": [
                    {
                        "entry_price": lot.entry_price, "side": lot.side,
                        "qty": lot.qty, "tp_price": lot.tp_price,
                        "tp_order_id": lot.tp_order_id, "filled_at": lot.filled_at,
                    }
                    for lot in self.active_lots
                ],
                "settings": {
                    "levels": self.settings.levels,
                    "corr_mult": self.settings.corr_mult,
                    "tp_mult": self.settings.tp_mult,
                    "max_lots": self.settings.max_lots,
                    "recenter_minutes": self.settings.recenter_minutes,
                    "spacing_mode": self.settings.spacing_mode,
                    "manual_spacing": self.settings.manual_spacing,
                },
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
        except Exception as e:
            logger.error("save_state error: %s", e)

    def load_state(self) -> bool:
        if not STATE_FILE.exists():
            return False
        try:
            data = json.loads(STATE_FILE.read_text(encoding="utf-8"))

            saved_instrument = data.get("instrument")
            if saved_instrument:
                self.settings.instrument = saved_instrument
                self._update_instrument_config()
            saved_qty = data.get("order_qty")
            if saved_qty:
                self.order_qty = saved_qty
            self.state.direction = data.get("direction", 0)
            self.state.direction_since = data.get("direction_since", "")
            self.state.spacing = data.get("spacing", 0)
            self.state.last_recenter = data.get("last_recenter", 0)
            self.state.total_pnl = data.get("total_pnl", 0)
            self.state.roundtrips = data.get("roundtrips", 0)
            self.state.flip_count = data.get("flip_count", 0)
            self.state.flip_pnl = data.get("flip_pnl", 0)
            self.state.net_position = data.get("net_position", 0)
            self.state.total_volume_usd = data.get("total_volume_usd", 0)

            # Load settings
            s = data.get("settings", {})
            if s:
                self.settings.levels = s.get("levels", DEFAULT_LEVELS)
                self.settings.corr_mult = s.get("corr_mult", DEFAULT_CORR_MULT)
                self.settings.tp_mult = s.get("tp_mult", DEFAULT_TP_MULT)
                self.settings.max_lots = s.get("max_lots", DEFAULT_MAX_LOTS)
                self.settings.recenter_minutes = s.get("recenter_minutes", DEFAULT_RECENTER_MINUTES)
                self.settings.spacing_mode = s.get("spacing_mode", "auto")
                self.settings.manual_spacing = s.get("manual_spacing", 50.0)

            for ld in data.get("active_lots", []):
                lot = ActiveLot(
                    entry_price=ld["entry_price"], side=ld["side"],
                    qty=ld["qty"], tp_price=ld["tp_price"],
                    tp_order_id=None,
                    filled_at=ld.get("filled_at", 0),
                )
                self.active_lots.append(lot)

            self._was_started = data.get("started", False)
            self._log_activity(
                f"State loaded: dir={'LONG' if self.state.direction == 1 else 'SHORT' if self.state.direction == -1 else 'NONE'} "
                f"since={self.state.direction_since} lots={len(self.active_lots)} "
                f"pnl={self.state.total_pnl:.4f} was_started={self._was_started}"
            )
            return True
        except Exception as e:
            logger.error("load_state error: %s", e)
            return False

    async def state_save_loop(self):
        while True:
            try:
                await asyncio.sleep(30)
                self.save_state()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def unrealized_pnl_loop(self):
        while True:
            try:
                await asyncio.sleep(2)
                if not self._cro_ws_ready:
                    self._unrealized_pnl = 0.0
                    continue
                detail = self.ws.get_position_detail(self.instrument)
                if detail:
                    self._unrealized_pnl = float(detail.get("session_unrealized_pnl", "0") or "0")
                else:
                    self._unrealized_pnl = 0.0
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("unrealized_pnl_loop error: %s", e)

    async def balance_loop(self):
        while True:
            try:
                await asyncio.sleep(10)
                if not self._cro_ws_ready:
                    continue
                bal = await self.rest.get_balance_usd()
                self._balance_usd = bal
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("balance_loop error: %s", e)

    # ---- Stats for dashboard ----

    def get_stats(self) -> dict:
        price = self._get_cro_price() or self.kline.latest_price
        if _is_btc(self.instrument):
            sigma = self.kline.calc_sigma()
            sigma_spacing = compute_spacing(price, sigma, self.settings.corr_mult, self.settings.levels,
                                            self._price_tick, self._min_spacing, self._max_spacing) if price > 0 else 0.0
        else:
            sigma = 0.0
            sigma_spacing = 0.0

        entries_pending = sum(1 for s in self.entry_slots if s.order_id)
        lots_held = len(self.active_lots)
        lots_in_profit = 0
        lots_in_loss = 0
        for lot in self.active_lots:
            if price > 0:
                if lot.side == "BUY":
                    is_profit = price > lot.entry_price
                else:
                    is_profit = price < lot.entry_price
                if is_profit:
                    lots_in_profit += 1
                else:
                    lots_in_loss += 1

        all_levels = []
        _pt = self._price_tick
        _dec = max(0, -int(math.floor(math.log10(_pt)))) if _pt > 0 else 1
        for s in self.entry_slots:
            all_levels.append({
                "p": round(s.price, _dec), "side": s.side,
                "type": "entry", "pending": s.order_id is not None,
            })
        for lot in self.active_lots:
            in_profit = False
            if price > 0:
                in_profit = (price > lot.entry_price) if lot.side == "BUY" else (price < lot.entry_price)
            all_levels.append({
                "p": round(lot.entry_price, _dec), "side": lot.side,
                "type": "lot", "tp": round(lot.tp_price, _dec),
                "profit": in_profit,
            })

        rc = self.settings.recenter_minutes
        return {
            "started": self.started,
            "cro_ws_ready": self._cro_ws_ready,
            "direction": self.state.direction,
            "direction_since": self.state.direction_since,
            "price": round(price, _dec),
            "spacing": round(self.state.spacing, _dec),
            "net_position": round(self.state.net_position, 4),
            "unrealized_pnl": round(self._unrealized_pnl, 4),
            "total_pnl": round(self.state.total_pnl, 4),
            "roundtrips": self.state.roundtrips,
            "total_volume": round(self.state.total_volume_usd, 4),
            "balance_usd": round(self._balance_usd, 2),
            "best_bid": round(self.market.best_bid, _dec),
            "best_ask": round(self.market.best_ask, _dec),
            "flip_count": self.state.flip_count,
            "flip_pnl": round(self.state.flip_pnl, 4),
            "entries_pending": entries_pending,
            "lots_held": lots_held,
            "lots_in_profit": lots_in_profit,
            "lots_in_loss": lots_in_loss,
            "max_lots": self.settings.max_lots,
            "levels_config": self.settings.levels,
            "closing_position": self._closing_position,
            "uptime_s": int(time.time() - self._started_at),
            "recenter_countdown": max(0, int(rc * 60 - (time.time() - self.state.last_recenter))) if self.state.last_recenter > 0 and self.started else -1,
            "recenter_minutes": rc,
            "instrument": self.instrument,
            "is_btc": _is_btc(self.instrument),
            "price_tick": self._price_tick,
            "order_qty": self.order_qty,
            # Sigma / volatility
            "sigma": round(sigma, 4),
            "sigma_spacing": round(sigma_spacing, _dec),
            # Settings for dashboard display
            "settings": {
                "levels": self.settings.levels,
                "spacing_mode": self.settings.spacing_mode,
                "manual_spacing": self.settings.manual_spacing,
                "recenter_minutes": self.settings.recenter_minutes,
                "tp_mult": self.settings.tp_mult,
                "max_lots": self.settings.max_lots,
            },
            # Grid levels
            "levels": all_levels,
            # Activity log
            "activity": list(self._activity_log),
        }


# ============================================================
# Flask + SocketIO Dashboard
# ============================================================

def create_dashboard(engine: ManualGridEngine, main_loop: asyncio.AbstractEventLoop):
    from flask import Flask, Response, request, jsonify
    from flask_socketio import SocketIO

    app = Flask(__name__)
    app.config["SECRET_KEY"] = os.urandom(24)
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

    DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="zh-TW"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Grid Manual</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.7.4/socket.io.min.js"></script>
<style>
:root{
  --long:#10b981;--long-glow:rgba(16,185,129,0.25);
  --short:#ef4444;--short-light:#f87171;--short-glow:rgba(239,68,68,0.25);
  --warmup:#f59e0b;--warmup-glow:rgba(245,158,11,0.25);
  --info:#3b82f6;
  --bg-primary:#0f1419;--bg-secondary:#1a1d29;--bg-tertiary:#252936;--bg-elevated:#2d3748;
  --text-primary:#e5e7eb;--text-secondary:#9ca3af;--text-muted:#6b7280;
  --border-primary:#2d3748;--border-secondary:#374151;
  --shadow-md:0 4px 6px -1px rgba(0,0,0,0.4);
  --radius-md:0.5rem;--radius-lg:0.75rem;
}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,'Inter','Segoe UI',Roboto,sans-serif;background:var(--bg-primary);color:var(--text-primary);line-height:1.5;min-height:100vh;padding:0}
.mono{font-family:'SF Mono',SFMono-Regular,Consolas,'Liberation Mono',Menlo,monospace}
.navbar{background:var(--bg-secondary);border-bottom:1px solid var(--border-primary);padding:12px 24px;display:flex;justify-content:space-between;align-items:center}
.navbar-brand{display:flex;align-items:center;gap:12px}
.brand-icon{width:32px;height:32px;background:linear-gradient(135deg,#f59e0b,#d97706);border-radius:8px;display:flex;align-items:center;justify-content:center;color:#fff;font-weight:bold;font-size:14px}
.brand-text{font-size:20px;font-weight:700;color:var(--text-primary)}
.brand-sub{font-size:13px;font-weight:600;color:#f59e0b;background:rgba(245,158,11,0.12);padding:3px 10px;border-radius:12px;border:1px solid rgba(245,158,11,0.25);letter-spacing:0.3px;transition:all 0.3s ease}
.brand-sub.flash{animation:inst-flash 0.6s ease}
@keyframes inst-flash{0%{background:rgba(245,158,11,0.4);transform:scale(1.08)}100%{background:rgba(245,158,11,0.12);transform:scale(1)}}
.navbar-right{display:flex;align-items:center;gap:16px}
.ws-badge{display:flex;align-items:center;gap:6px;padding:4px 12px;border-radius:20px;font-size:14px;font-weight:600;border:1px solid var(--border-secondary)}
.ws-dot{width:8px;height:8px;border-radius:50%;animation:pulse 2s infinite}
.ws-dot.on{background:var(--long)}
.ws-dot.off{background:var(--short)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}
.main{max-width:1200px;margin:0 auto;padding:20px 24px}
/* State banner */
.state-banner{border-radius:var(--radius-lg);padding:16px 24px;margin-bottom:12px;display:flex;justify-content:space-between;align-items:center;font-weight:700;font-size:18px;transition:all 0.4s}
.state-banner.st-long{background:linear-gradient(135deg,rgba(16,185,129,0.15),rgba(16,185,129,0.05));border:1px solid rgba(16,185,129,0.3);color:var(--long)}
.state-banner.st-short{background:linear-gradient(135deg,rgba(239,68,68,0.15),rgba(239,68,68,0.05));border:1px solid rgba(239,68,68,0.3);color:var(--short)}
.state-banner.st-wait{background:linear-gradient(135deg,rgba(245,158,11,0.15),rgba(245,158,11,0.05));border:1px solid rgba(245,158,11,0.3);color:var(--warmup)}
.state-banner.st-off{background:var(--bg-secondary);border:1px solid var(--border-primary);color:var(--text-muted)}
.state-banner.st-nokey{background:linear-gradient(135deg,rgba(99,102,241,0.15),rgba(99,102,241,0.05));border:1px solid rgba(99,102,241,0.3);color:#818cf8}
.state-banner .state-left{display:flex;align-items:center;gap:12px}
.state-banner .state-dot{width:12px;height:12px;border-radius:50%;animation:pulse 2s infinite}
.state-banner .state-dur{font-size:14px;font-weight:400;color:var(--text-secondary)}
/* Direction control row */
.dir-row{display:grid;grid-template-columns:3fr 1fr;gap:12px;margin-bottom:12px}
.dir-btns{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:16px}
.dir-btn{padding:14px 8px;border-radius:var(--radius-md);border:2px solid var(--border-primary);background:var(--bg-tertiary);cursor:pointer;transition:all 0.3s;text-align:center}
.dir-btn:hover{transform:translateY(-1px);box-shadow:var(--shadow-md)}
.dir-btn:disabled{opacity:0.3;cursor:not-allowed;transform:none!important;box-shadow:none!important}
.dir-btn .dir-icon{font-size:22px;font-weight:800;letter-spacing:0.5px}
.dir-btn .dir-sub{font-size:12px;color:var(--text-muted);margin-top:2px}
.dir-btn.long-btn{border-color:rgba(16,185,129,0.3)}
.dir-btn.long-btn:hover{border-color:var(--long);background:rgba(16,185,129,0.08)}
.dir-btn.long-btn.active{border-color:var(--long);background:rgba(16,185,129,0.15);box-shadow:0 0 16px rgba(16,185,129,0.2)}
.dir-btn.long-btn .dir-icon{color:var(--long)}
.dir-btn.neut-btn{border-color:rgba(245,158,11,0.3)}
.dir-btn.neut-btn:hover{border-color:var(--warmup);background:rgba(245,158,11,0.05)}
.dir-btn.neut-btn.active{border-color:var(--warmup);background:rgba(245,158,11,0.08);box-shadow:0 0 16px rgba(245,158,11,0.15)}
.dir-btn.neut-btn .dir-icon{color:var(--warmup)}
.dir-btn.short-btn{border-color:rgba(239,68,68,0.3)}
.dir-btn.short-btn:hover{border-color:var(--short);background:rgba(239,68,68,0.08)}
.dir-btn.short-btn.active{border-color:var(--short);background:rgba(239,68,68,0.15);box-shadow:0 0 16px rgba(239,68,68,0.2)}
.dir-btn.short-btn .dir-icon{color:var(--short)}
/* Sigma card */
.sigma-card{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:16px 20px;display:flex;flex-direction:column;justify-content:center}
.sigma-card .sigma-title{font-size:13px;color:var(--text-muted);margin-bottom:6px;font-weight:600}
.sigma-card .sigma-value{font-size:26px;font-weight:700;margin-bottom:2px}
.sigma-card .sigma-sub{font-size:12px;color:var(--text-muted)}
/* Primary metrics */
.metrics-primary{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:12px}
.card{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:16px 20px;transition:all 0.3s}
.card:hover{border-color:var(--border-secondary);box-shadow:var(--shadow-md)}
.card.highlight{border-color:#14b8a6;box-shadow:0 0 20px rgba(20,184,166,0.3)}
.card-title{font-size:14px;color:var(--text-muted);margin-bottom:8px;display:flex;justify-content:space-between;align-items:center}
.card-icon{width:28px;height:28px;background:var(--bg-tertiary);border-radius:var(--radius-md);display:flex;align-items:center;justify-content:center;font-size:16px}
.card-value{font-size:32px;font-weight:700;letter-spacing:-0.5px}
.card-value.positive{color:var(--long)}
.card-value.negative{color:var(--short-light)}
.card-value.muted{color:var(--text-muted)}
/* Secondary metrics */
.metrics-secondary{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:12px}
.card-group{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:16px 20px}
.card-group-title{font-size:15px;color:var(--text-muted);margin-bottom:12px;font-weight:600}
.mini-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}
.mini-stat{background:var(--bg-tertiary);border-radius:var(--radius-md);padding:10px 12px}
.mini-label{font-size:13px;color:var(--text-muted);margin-bottom:4px}
.mini-value{font-size:18px;font-weight:700}
/* Grid viz */
.grid-viz-container{margin-bottom:12px}
.grid-row-wrap{position:relative;margin-top:8px}
.grid-row{display:flex;gap:2px;align-items:stretch}
.lv{flex:1;min-width:0;height:32px;border-radius:3px;font-size:10px;display:flex;align-items:center;justify-content:center;color:var(--text-muted);cursor:default;transition:background 0.3s,box-shadow 0.3s;position:relative;z-index:1}
.lv-empty{background:var(--bg-tertiary)}
.lv-pending{background:#1e3a5f;color:#60a5fa}
.lv-held{background:#064e3b;color:#34d399}
.lv-profit{background:#065f46;box-shadow:0 0 6px rgba(52,211,153,0.4)}
.lv-loss{background:#064e3b;border:1px solid rgba(239,68,68,0.35)}
.lv:hover{transform:scaleY(1.35);z-index:3}
.lv-center{flex:0 0 3px;background:var(--border-primary);border-radius:1px;height:32px;align-self:center}
.price-cursor{position:absolute;top:-2px;width:3px;height:36px;background:#f59e0b;border-radius:2px;box-shadow:0 0 10px rgba(245,158,11,0.7);z-index:2;transition:left 0.5s ease;pointer-events:none}
.grid-price-tag{position:absolute;top:-20px;transform:translateX(-50%);font-size:11px;font-weight:700;color:#f59e0b;white-space:nowrap;transition:left 0.5s ease;pointer-events:none;z-index:2}
.grid-labels{display:flex;justify-content:space-between;margin-top:4px;font-size:10px;color:var(--text-muted)}
/* Log */
.log-container{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:16px 20px}
.log-title{font-size:15px;color:var(--text-muted);margin-bottom:10px;font-weight:600}
.log-body{max-height:220px;overflow-y:auto;font-size:13px;line-height:1.8}
.log-body::-webkit-scrollbar{width:4px}
.log-body::-webkit-scrollbar-track{background:var(--bg-tertiary)}
.log-body::-webkit-scrollbar-thumb{background:var(--border-secondary);border-radius:2px}
.log-entry{padding:2px 0;border-bottom:1px solid var(--bg-tertiary);color:var(--text-secondary)}
/* Buttons */
.btn-group{display:flex;gap:10px;margin-top:6px}
.btn{border:none;padding:12px 32px;border-radius:var(--radius-md);cursor:pointer;font-size:16px;font-weight:700;transition:all 0.2s;text-transform:uppercase;letter-spacing:0.5px}
.btn-start{background:linear-gradient(135deg,#10b981,#059669);color:#fff}
.btn-start:hover{box-shadow:0 0 20px rgba(16,185,129,0.4);transform:translateY(-1px)}
.btn-stop{background:linear-gradient(135deg,#ef4444,#dc2626);color:#fff}
.btn-stop:hover{box-shadow:0 0 20px rgba(239,68,68,0.4);transform:translateY(-1px)}
.btn:disabled{background:var(--bg-elevated)!important;color:var(--text-muted)!important;cursor:not-allowed;box-shadow:none!important;transform:none!important}
/* Settings modal */
.gear-btn{background:none;border:none;color:var(--text-secondary);cursor:pointer;padding:6px;border-radius:8px;transition:all 0.2s;display:flex;align-items:center;justify-content:center;margin-left:8px}
.gear-btn:hover{color:var(--text-primary);background:var(--bg-tertiary)}
/* Signal badge (AI signal reference) */
.grid-signal-badge{display:inline-flex;align-items:center;gap:6px;padding:4px 14px;border-radius:20px;font-size:13px;font-weight:700;margin-left:12px;cursor:pointer;transition:all 0.3s;letter-spacing:0.5px}
.grid-signal-badge.sig-bullish{background:rgba(16,185,129,0.15);color:var(--long);border:1px solid rgba(16,185,129,0.4)}
.grid-signal-badge.sig-bearish{background:rgba(239,68,68,0.15);color:var(--short);border:1px solid rgba(239,68,68,0.4)}
.grid-signal-badge.sig-neutral{background:rgba(245,158,11,0.15);color:var(--warmup);border:1px solid rgba(245,158,11,0.4)}
.grid-signal-badge:hover{transform:scale(1.05);filter:brightness(1.2)}
/* Mode tab */
.mode-tab{display:flex;gap:0;border:1px solid var(--border-secondary);border-radius:20px;overflow:hidden;margin-left:16px}
.mode-tab a{padding:4px 14px;font-size:13px;font-weight:600;text-decoration:none;color:var(--text-muted);transition:all 0.2s;cursor:pointer;user-select:none}
.mode-tab a.active{background:rgba(20,184,166,0.15);color:#14b8a6;cursor:default}
.mode-tab a:hover:not(.active){background:var(--bg-tertiary)}
/* Guide modal */
.guide-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.7);z-index:200;display:none;align-items:flex-start;justify-content:center;padding:40px 20px;overflow-y:auto}
.guide-overlay.show{display:flex}
.guide-box{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:32px 36px;width:680px;max-width:95vw;box-shadow:0 12px 40px rgba(0,0,0,0.6);margin:auto}
.guide-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:24px;padding-bottom:16px;border-bottom:1px solid var(--border-primary)}
.guide-header h2{font-size:20px;font-weight:700;color:var(--text-primary);display:flex;align-items:center;gap:10px}
.guide-section{margin-bottom:24px}
.guide-section:last-child{margin-bottom:0}
.guide-section h3{font-size:16px;font-weight:700;color:#14b8a6;margin-bottom:10px;display:flex;align-items:center;gap:8px}
.guide-section h3 .gs-num{width:22px;height:22px;background:rgba(20,184,166,0.15);border-radius:50%;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#14b8a6}
.guide-section p{font-size:14px;color:var(--text-secondary);line-height:1.7;margin-bottom:8px}
.guide-section strong{color:var(--text-primary)}
.guide-table{width:100%;border-collapse:collapse;margin:8px 0 12px;font-size:13px}
.guide-table th{text-align:left;padding:8px 10px;background:var(--bg-tertiary);color:var(--text-muted);font-weight:600;border-bottom:1px solid var(--border-primary)}
.guide-table td{padding:8px 10px;border-bottom:1px solid var(--bg-tertiary);color:var(--text-secondary)}
.guide-table td:first-child{color:var(--text-primary);font-weight:600;white-space:nowrap}
.guide-list{margin:6px 0 10px 0;padding-left:20px;font-size:14px;color:var(--text-secondary);line-height:1.8}
.guide-tip{background:var(--bg-tertiary);border-left:3px solid #14b8a6;border-radius:0 var(--radius-md) var(--radius-md) 0;padding:10px 14px;font-size:13px;color:var(--text-secondary);margin:10px 0}
.guide-badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:12px;font-weight:600}
.guide-badge.g-long{background:rgba(16,185,129,0.15);color:var(--long)}
.guide-badge.g-short{background:rgba(239,68,68,0.15);color:var(--short)}
.guide-badge.g-warn{background:rgba(245,158,11,0.15);color:var(--warmup)}
/* MoneyFlow modal */
.mf-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.75);z-index:250;display:none;align-items:center;justify-content:center}
.mf-overlay.show{display:flex}
.mf-box{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);width:92vw;height:88vh;max-width:1400px;box-shadow:0 12px 40px rgba(0,0,0,0.6);display:flex;flex-direction:column;overflow:hidden}
.mf-header{display:flex;justify-content:space-between;align-items:center;padding:14px 20px;border-bottom:1px solid var(--border-primary)}
.mf-header h2{font-size:18px;font-weight:700;color:var(--text-primary);display:flex;align-items:center;gap:10px}
.mf-header h2 svg{color:#f59e0b}
.mf-iframe{flex:1;border:none;width:100%;background:#0f1419}
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,0.6);z-index:100;display:none;align-items:center;justify-content:center}
.modal-overlay.show{display:flex}
.modal-box{background:var(--bg-secondary);border:1px solid var(--border-primary);border-radius:var(--radius-lg);padding:24px 28px;width:480px;max-width:90vw;box-shadow:0 8px 30px rgba(0,0,0,0.5);max-height:90vh;overflow-y:auto}
.modal-title{font-size:18px;font-weight:700;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center}
.modal-close{background:none;border:none;color:var(--text-muted);font-size:22px;cursor:pointer;padding:2px 6px}
.modal-close:hover{color:var(--text-primary)}
.modal-section{margin-bottom:20px}
.modal-section:last-child{margin-bottom:0}
.modal-label{font-size:14px;color:var(--text-muted);margin-bottom:6px;font-weight:600}
.modal-row{display:flex;gap:8px;align-items:center}
.modal-input{flex:1;background:var(--bg-tertiary);border:1px solid var(--border-secondary);border-radius:var(--radius-md);padding:8px 12px;color:var(--text-primary);font-family:inherit;font-size:14px;outline:none;transition:border 0.2s}
.modal-input:focus{border-color:#14b8a6}
.modal-select{flex:1;background:var(--bg-tertiary);border:1px solid var(--border-secondary);border-radius:var(--radius-md);padding:8px 12px;color:var(--text-primary);font-size:14px;outline:none}
.modal-btn{padding:8px 16px;border:none;border-radius:var(--radius-md);cursor:pointer;font-size:14px;font-weight:600;transition:all 0.2s}
.modal-btn-primary{background:#14b8a6;color:#fff}
.modal-btn-primary:hover{background:#0d9488}
.modal-btn-danger{background:var(--short);color:#fff}
.modal-btn-danger:hover{background:#dc2626}
.modal-btn-ghost{background:var(--bg-tertiary);color:var(--text-secondary)}
.modal-btn-ghost:hover{background:var(--bg-elevated)}
.modal-btn:disabled{opacity:0.5;cursor:not-allowed}
.modal-divider{border:none;border-top:1px solid var(--border-primary);margin:16px 0}
.modal-hint{font-size:12px;color:var(--text-muted);margin-top:6px}
.modal-status{font-size:13px;margin-top:8px;font-weight:600}
.eye-btn{background:none;border:none;color:var(--text-muted);cursor:pointer;font-size:16px;padding:4px}
.eye-btn:hover{color:var(--text-primary)}
@media(max-width:900px){.metrics-primary{grid-template-columns:repeat(2,1fr)}.mini-grid{grid-template-columns:repeat(2,1fr)}}
</style></head><body>

<nav class="navbar">
  <div class="navbar-brand">
    <div class="brand-icon" style="background:linear-gradient(135deg,#f59e0b,#d97706)">M</div>
    <span class="brand-text">Grid Manual</span>
    <span class="brand-sub" id="instrument">--</span>
    <span class="grid-signal-badge sig-neutral" id="grid-signal-badge" onclick="openMoneyFlow()" title="AI Signal (reference only, click for report)">--</span>
    <div class="mode-tab">
      <a onclick="switchMode('auto')">Auto</a>
      <a class="active">Manual</a>
    </div>
  </div>
  <div class="navbar-right">
    <div class="ws-badge">
      <span class="ws-dot" id="ws-dot"></span>
      <span id="ws-text">Connecting</span>
    </div>
    <div class="btn-group">
      <button class="btn btn-start" id="btn-start" onclick="startEngine()">START</button>
      <button class="btn btn-stop" id="btn-stop" onclick="stopEngine()" disabled>STOP</button>
      <button class="btn" id="btn-rebuild" onclick="rebuildGrid()" disabled style="background:rgba(99,130,202,0.15);color:#93a8d4;border:1px solid rgba(99,130,202,0.3)">REBUILD</button>
      <button class="btn" id="btn-close" onclick="closePosition()" style="background:linear-gradient(135deg,#4B5563,#374151);color:#fff">CLOSE</button>
    </div>
    <button class="gear-btn" onclick="openSettings()" title="Settings"><svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg></button>
    <button class="gear-btn" onclick="openGuide()" title="Guide"><svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg></button>
    <button class="gear-btn" onclick="openMoneyFlow()" title="Money Flow" style="color:#f59e0b"><svg width="26" height="26" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg></button>
  </div>
</nav>

<!-- Guide Modal -->
<div class="guide-overlay" id="guide-modal" onclick="if(event.target===this)closeGuide()">
  <div class="guide-box">
    <div class="guide-header">
      <h2><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg> Manual Grid Guide</h2>
      <button class="modal-close" onclick="closeGuide()">&times;</button>
    </div>

    <div class="guide-section">
      <h3><span class="gs-num">1</span> What Is This?</h3>
      <p>A <strong>manual grid trading system</strong> on Crypto.com perpetual futures. Select instrument in Settings, choose direction (LONG, SHORT, or NEUTRAL), and the system places limit entry orders with automatic take-profit.</p>
    </div>

    <div class="guide-section">
      <h3><span class="gs-num">2</span> How It Works</h3>
      <ol class="guide-list">
        <li>Click <strong>START</strong> to activate the system</li>
        <li>Choose <span class="guide-badge g-long">LONG</span> or <span class="guide-badge g-short">SHORT</span></li>
        <li>System places entry orders automatically based on your grid settings</li>
        <li>Each fill gets a take-profit order</li>
        <li>Grid recenters periodically at current price</li>
      </ol>
    </div>

    <div class="guide-section">
      <h3><span class="gs-num">3</span> Direction Switch</h3>
      <p>Clicking the opposite direction while holding positions will:</p>
      <ol class="guide-list">
        <li>Show a confirmation dialog</li>
        <li>Close all current positions (post-only limit, chases price)</li>
        <li>Rebuild grid in the new direction</li>
      </ol>
      <div class="guide-tip">The system always tries post-only (maker) orders first to minimize fees. Falls back to market order after 30s timeout.</div>
    </div>

    <div class="guide-section">
      <h3><span class="gs-num">4</span> Settings (gear icon)</h3>
      <table class="guide-table">
        <tr><th>Setting</th><th>Default</th><th>Description</th></tr>
        <tr><td>Order QTY</td><td>varies</td><td>Quantity per lot (instrument-specific)</td></tr>
        <tr><td>Grid Levels</td><td>20</td><td>Number of entry orders</td></tr>
        <tr><td>Spacing</td><td>Auto</td><td>Auto = volatility-based, or fixed $</td></tr>
        <tr><td>Recenter</td><td>60 min</td><td>Rebuild interval</td></tr>
        <tr><td>Max Lots</td><td>60</td><td>Max concurrent positions</td></tr>
      </table>
      <div class="guide-tip">Risk = QTY &times; Max Lots &times; Price. Check your exposure for the selected instrument.</div>
    </div>

    <div class="guide-section">
      <h3><span class="gs-num">5</span> Buttons</h3>
      <table class="guide-table">
        <tr><td>START</td><td>Activate system, then select direction</td></tr>
        <tr><td>STOP</td><td>Cancel orders, keep positions open</td></tr>
        <tr><td>REBUILD</td><td>Close positions &rarr; rebuild grid (same direction)</td></tr>
        <tr><td>CLOSE</td><td>Close all positions &rarr; stop system</td></tr>
      </table>
    </div>
  </div>
</div>

<!-- Money Flow Modal -->
<div class="mf-overlay" id="mf-modal" onclick="if(event.target===this)closeMoneyFlow()">
  <div class="mf-box">
    <div class="mf-header">
      <h2><svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 20V10"/><path d="M18 20V4"/><path d="M6 20v-4"/></svg> Money Flow Report</h2>
      <button class="modal-close" onclick="closeMoneyFlow()">&times;</button>
    </div>
    <iframe class="mf-iframe" id="mf-iframe" src="about:blank"></iframe>
  </div>
</div>

<!-- Settings Modal -->
<div class="modal-overlay" id="settings-modal" onclick="if(event.target===this)closeSettings()">
  <div class="modal-box">
    <div class="modal-title">
      Settings
      <button class="modal-close" onclick="closeSettings()">&times;</button>
    </div>

    <div class="modal-section">
      <div class="modal-label">Instrument</div>
      <div class="modal-row">
        <select class="modal-select mono" id="set-instrument"></select>
        <button class="modal-btn modal-btn-primary" id="btn-apply-instrument" onclick="applyInstrument()">Apply</button>
      </div>
      <div class="modal-hint">Current: <span id="set-instrument-current" class="mono">--</span></div>
      <div class="modal-hint" style="color:var(--warmup)">&#9888; Engine must be STOPPED to switch instrument</div>
      <div class="modal-status" id="instrument-status"></div>
    </div>

    <hr class="modal-divider">

    <div class="modal-section">
      <div class="modal-label">Order QTY</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-qty" type="text" placeholder="0.0001">
        <button class="modal-btn modal-btn-primary" onclick="applyQty()">Apply</button>
      </div>
      <div class="modal-hint">Current: <span id="set-qty-current" class="mono">--</span></div>
      <div class="modal-hint" id="qty-tick-hint" style="color:var(--info)">Min: -- | Step: --</div>
      <div class="modal-status" id="qty-status"></div>
    </div>

    <hr class="modal-divider">

    <div class="modal-section">
      <div class="modal-label">Grid Levels</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-levels" type="number" min="5" max="50" value="20">
      </div>
      <div class="modal-hint">Number of entry orders (5-50)</div>
    </div>

    <div class="modal-section">
      <div class="modal-label">Spacing Mode</div>
      <div class="modal-row">
        <select class="modal-select" id="set-spacing-mode" onchange="toggleSpacingInput()">
          <option value="auto">Auto (volatility-based, BTC only)</option>
          <option value="manual">Manual (fixed)</option>
        </select>
      </div>
    </div>

    <div class="modal-section" id="manual-spacing-section" style="display:none">
      <div class="modal-label">Manual Spacing ($)</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-manual-spacing" type="number" step="any" value="50">
      </div>
      <div class="modal-hint">Fixed distance between grid levels</div>
    </div>

    <div class="modal-section">
      <div class="modal-label">Recenter Interval (minutes)</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-recenter" type="number" min="5" max="1440" value="60">
      </div>
      <div class="modal-hint">How often to rebuild entry orders at current price</div>
    </div>

    <div class="modal-section">
      <div class="modal-label">Max Lots</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-max-lots" type="number" min="10" max="200" value="60">
      </div>
      <div class="modal-hint">Maximum concurrent filled positions</div>
    </div>

    <div class="modal-row" style="margin-top:16px;justify-content:flex-end;gap:10px">
      <button class="modal-btn modal-btn-ghost" onclick="closeSettings()">Cancel</button>
      <button class="modal-btn modal-btn-primary" onclick="applyGridSettings()">Apply Grid Settings</button>
    </div>
    <div class="modal-status" id="grid-settings-status"></div>

    <hr class="modal-divider">

    <div class="modal-section">
      <div class="modal-label">API Key</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-ak" type="password" placeholder="API Key">
        <button class="eye-btn" onclick="toggleVis('set-ak')">&#128065;</button>
      </div>
      <div class="modal-hint">Current: <span id="set-ak-masked" class="mono">--</span></div>
    </div>

    <div class="modal-section">
      <div class="modal-label">Secret Key</div>
      <div class="modal-row">
        <input class="modal-input mono" id="set-sk" type="password" placeholder="Secret Key">
        <button class="eye-btn" onclick="toggleVis('set-sk')">&#128065;</button>
      </div>
      <div class="modal-hint">Current: <span id="set-sk-masked" class="mono">--</span></div>
    </div>

    <div class="modal-row" style="margin-top:16px;justify-content:flex-end;gap:10px">
      <button class="modal-btn modal-btn-danger" id="btn-save-key" onclick="saveApiKey()">Save &amp; Reconnect</button>
    </div>
    <div class="modal-hint" style="margin-top:8px;color:var(--warmup)">&#9888; API Key change requires engine STOP first, will reconnect WS</div>
    <div class="modal-status" id="key-status"></div>
  </div>
</div>

<div class="main">

  <!-- State Banner -->
  <div class="state-banner st-off" id="state-banner">
    <div class="state-left">
      <div class="state-dot" id="state-dot"></div>
      <span id="state-label">STOPPED</span>
    </div>
    <span class="state-dur" id="state-dur"></span>
  </div>

  <!-- Direction Control + Sigma -->
  <div class="dir-row">
    <div class="dir-btns">
      <button class="dir-btn long-btn" id="dir-long" onclick="setDirection(1)">
        <div class="dir-icon">&#9650; LONG</div>
        <div class="dir-sub">BUY below</div>
      </button>
      <button class="dir-btn neut-btn" id="dir-neut" onclick="setDirection(0)">
        <div class="dir-icon">&#9644; NEUTRAL</div>
        <div class="dir-sub">BUY + SELL</div>
      </button>
      <button class="dir-btn short-btn" id="dir-short" onclick="setDirection(-1)">
        <div class="dir-icon">&#9660; SHORT</div>
        <div class="dir-sub">SELL above</div>
      </button>
    </div>
    <div class="sigma-card">
      <div class="sigma-title">Volatility (Sigma)</div>
      <div class="sigma-value mono" id="sigma-val">--</div>
      <div class="sigma-sub">Est. Spacing: <span class="mono" id="sigma-spacing">--</span></div>
    </div>
  </div>

  <!-- Primary Metrics -->
  <div class="metrics-primary">
    <div class="card">
      <div class="card-title">Balance <div class="card-icon">$</div></div>
      <div class="card-value mono" id="balance_usd">--</div>
    </div>
    <div class="card highlight">
      <div class="card-title">Total PnL <div class="card-icon">P</div></div>
      <div class="card-value mono" id="total_pnl">--</div>
    </div>
    <div class="card">
      <div class="card-title">Unrealized <div class="card-icon">U</div></div>
      <div class="card-value mono" id="unrealized_pnl">--</div>
    </div>
    <div class="card">
      <div class="card-title">Net Position <div class="card-icon">N</div></div>
      <div class="card-value mono" id="net_position">--</div>
    </div>
  </div>

  <!-- Secondary Metrics -->
  <div class="metrics-secondary">
    <div class="card-group">
      <div class="card-group-title">Grid Info</div>
      <div class="mini-grid">
        <div class="mini-stat">
          <div class="mini-label">Direction</div>
          <div class="mini-value" id="dir-label" style="font-size:16px">--</div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Since</div>
          <div class="mini-value mono" id="last-flip-date" style="font-size:13px">--</div>
          <div style="font-size:12px;color:var(--text-muted);margin-top:2px" id="last-flip-ago"></div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Spacing</div>
          <div class="mini-value mono" id="spacing">--</div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Pending</div>
          <div class="mini-value mono" id="entries-pending">0</div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Lots / Max</div>
          <div class="mini-value mono" id="lots-held">0 / 60</div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Recenter</div>
          <div class="mini-value mono" id="recenter-info">--</div>
        </div>
      </div>
    </div>
    <div class="card-group">
      <div class="card-group-title">Trading</div>
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div class="mini-stat">
          <div class="mini-label">Roundtrips</div>
          <div class="mini-value mono" id="roundtrips">0</div>
          <div class="mini-label" style="margin-top:6px">Volume</div>
          <div class="mini-value mono" id="total_volume">0</div>
        </div>
        <div class="mini-stat">
          <div class="mini-label">Flips / Flip PnL</div>
          <div class="mini-value mono" id="flip-info">0 / 0</div>
          <div class="mini-label" style="margin-top:6px">Ask / Bid</div>
          <div class="mini-value mono" id="best-prices" style="font-size:14px">-- / --</div>
        </div>
      </div>
    </div>
  </div>

  <!-- Grid Visualization -->
  <div class="card-group grid-viz-container">
    <div class="card-group-title" style="display:flex;justify-content:space-between;align-items:center">
      <span>Grid
        <span style="font-size:13px;color:var(--text-muted);margin-left:8px">
          <span style="color:#60a5fa;font-size:16px">&bull;</span> Entry
          <span style="color:#34d399;font-size:16px;margin-left:6px">&bull;</span> Held
          <span style="color:var(--text-muted);font-size:16px;margin-left:6px">&bull;</span> Empty
        </span>
      </span>
      <span style="font-size:13px;display:flex;align-items:center;gap:12px">
        <span id="grid-dir-label" style="color:var(--text-muted)">--</span>
        <span id="recenter-cd" style="color:var(--text-muted);font-family:var(--font-mono,monospace)">R1: --</span>
      </span>
    </div>
    <div class="grid-row-wrap" style="margin-top:24px">
      <div class="grid-price-tag" id="grid-price-tag">--</div>
      <div class="price-cursor" id="price-cursor"></div>
      <div class="grid-row" id="grid-row"></div>
    </div>
    <div class="grid-labels">
      <span id="grid-lo">--</span>
      <span style="color:var(--text-secondary)" id="grid-summary">--</span>
      <span id="grid-hi">--</span>
    </div>
  </div>

  <!-- Activity Log -->
  <div class="log-container">
    <div class="log-title">Activity Log</div>
    <div class="log-body" id="activity"></div>
  </div>
</div>

<script>
const socket = io();
socket.on('connect', () => {
  document.getElementById('ws-dot').className = 'ws-dot on';
  document.getElementById('ws-text').textContent = 'Connected';
});
socket.on('disconnect', () => {
  document.getElementById('ws-dot').className = 'ws-dot off';
  document.getElementById('ws-text').textContent = 'Disconnected';
});

function fmtDuration(secs) {
  if (secs < 60) return secs + 's';
  if (secs < 3600) return Math.floor(secs/60) + 'm ' + (secs%60) + 's';
  return Math.floor(secs/3600) + 'h ' + Math.floor((secs%3600)/60) + 'm';
}

var _dirChanging = false;

socket.on('stats_update', function(d) {
  var settingsOpen = document.getElementById('settings-modal') && document.getElementById('settings-modal').classList.contains('show');
  var guideOpen = document.getElementById('guide-modal') && document.getElementById('guide-modal').classList.contains('show');
  var mfOpen = document.getElementById('mf-modal') && document.getElementById('mf-modal').classList.contains('show');
  if (settingsOpen || guideOpen || mfOpen) return;

  // State Banner
  var banner = document.getElementById('state-banner');
  var stLabel = document.getElementById('state-label');
  var stDot = document.getElementById('state-dot');
  var stDur = document.getElementById('state-dur');
  banner.className = 'state-banner';
  if (!d.cro_ws_ready) {
    banner.classList.add('st-nokey');
    stLabel.textContent = 'CONFIGURE API KEY \u2699';
    stDot.style.background = '#818cf8';
    stDur.textContent = 'Open Settings to configure';
  } else if (!d.started) {
    banner.classList.add('st-off');
    stLabel.textContent = 'STOPPED';
    stDot.style.background = 'var(--text-muted)';
    stDur.textContent = '';
  } else if (d.direction === 0) {
    banner.classList.add('st-wait');
    if (d.direction_since) {
      stLabel.textContent = 'NEUTRAL \u25AC';
      stDur.textContent = 'BUY below + SELL above \u2014 Uptime: ' + fmtDuration(d.uptime_s);
    } else {
      stLabel.textContent = 'SELECT DIRECTION';
      stDur.textContent = 'Choose LONG, NEUTRAL, or SHORT below';
    }
    stDot.style.background = 'var(--warmup)';
  } else if (d.direction === 1) {
    banner.classList.add('st-long');
    stLabel.textContent = 'LONG \u25B2';
    stDot.style.background = 'var(--long)';
    stDur.textContent = 'Uptime: ' + fmtDuration(d.uptime_s);
  } else {
    banner.classList.add('st-short');
    stLabel.textContent = 'SHORT \u25BC';
    stDot.style.background = 'var(--short)';
    stDur.textContent = 'Uptime: ' + fmtDuration(d.uptime_s);
  }

  // Instrument — update navbar badge + flash on change
  var instEl = document.getElementById('instrument');
  var newInst = d.instrument || '--';
  if (instEl.textContent !== newInst) {
    instEl.textContent = newInst;
    instEl.classList.remove('flash');
    void instEl.offsetWidth;
    instEl.classList.add('flash');
  }

  // Direction buttons (3-way)
  var longBtn = document.getElementById('dir-long');
  var neutBtn = document.getElementById('dir-neut');
  var shortBtn = document.getElementById('dir-short');
  longBtn.className = 'dir-btn long-btn' + (d.direction === 1 ? ' active' : '');
  neutBtn.className = 'dir-btn neut-btn' + (d.direction === 0 && d.started ? ' active' : '');
  shortBtn.className = 'dir-btn short-btn' + (d.direction === -1 ? ' active' : '');
  var dirDisabled = !d.started || !d.cro_ws_ready || d.closing_position || _dirChanging;
  longBtn.disabled = dirDisabled;
  neutBtn.disabled = dirDisabled;
  shortBtn.disabled = dirDisabled;

  // Sigma display
  var sigVal = document.getElementById('sigma-val');
  var sigSpc = document.getElementById('sigma-spacing');
  if (d.sigma !== undefined && d.sigma > 0) {
    sigVal.textContent = (d.sigma * 100).toFixed(2) + '%';
    sigVal.style.color = d.sigma > 0.8 ? 'var(--short)' : d.sigma > 0.5 ? 'var(--warmup)' : 'var(--long)';
    sigSpc.textContent = d.sigma_spacing ? ('$' + d.sigma_spacing.toFixed(1)) : '--';
  } else { sigVal.textContent = '--'; sigVal.style.color = ''; sigSpc.textContent = '--'; }

  // Balance
  document.getElementById('balance_usd').textContent = d.balance_usd ? d.balance_usd.toLocaleString('en-US',{minimumFractionDigits:2}) + ' USD' : '--';

  // PnL
  var tpEl = document.getElementById('total_pnl');
  tpEl.textContent = d.total_pnl >= 0 ? '+' + d.total_pnl.toFixed(4) : d.total_pnl.toFixed(4);
  tpEl.className = 'card-value mono ' + (d.total_pnl >= 0 ? 'positive' : 'negative');

  var upEl = document.getElementById('unrealized_pnl');
  upEl.textContent = d.unrealized_pnl >= 0 ? '+' + d.unrealized_pnl.toFixed(4) : d.unrealized_pnl.toFixed(4);
  upEl.className = 'card-value mono ' + (d.unrealized_pnl >= 0 ? 'positive' : 'negative');

  var npEl = document.getElementById('net_position');
  npEl.textContent = d.net_position.toFixed(4);
  npEl.className = 'card-value mono' + (d.net_position > 0 ? ' positive' : d.net_position < 0 ? ' negative' : ' muted');

  // Grid Info
  var dirEl = document.getElementById('dir-label');
  if (d.direction === 1) { dirEl.textContent = 'LONG \u25B2'; dirEl.style.color = 'var(--long)'; }
  else if (d.direction === -1) { dirEl.textContent = 'SHORT \u25BC'; dirEl.style.color = 'var(--short)'; }
  else if (d.direction === 0 && d.direction_since) { dirEl.textContent = 'NEUTRAL \u25AC'; dirEl.style.color = 'var(--text-secondary)'; }
  else { dirEl.textContent = 'WAITING'; dirEl.style.color = 'var(--warmup)'; }

  var lfDate = document.getElementById('last-flip-date');
  var lfAgo = document.getElementById('last-flip-ago');
  if (d.direction_since) {
    try {
      var dd = new Date(d.direction_since);
      var now = new Date();
      var diff = Math.floor((now - dd) / 1000);
      lfDate.textContent = dd.toISOString().slice(5,16).replace('T',' ') + ' UTC+0';
      if (diff < 3600) lfAgo.textContent = Math.floor(diff/60) + 'm ago';
      else if (diff < 86400) lfAgo.textContent = Math.floor(diff/3600) + 'h ago';
      else lfAgo.textContent = Math.floor(diff/86400) + 'd ' + Math.floor((diff%86400)/3600) + 'h ago';
    } catch(e) { lfDate.textContent = '--'; lfAgo.textContent = ''; }
  } else { lfDate.textContent = '--'; lfAgo.textContent = ''; }
  document.getElementById('spacing').textContent = d.spacing || '--';
  document.getElementById('entries-pending').textContent = d.entries_pending;
  document.getElementById('lots-held').textContent = d.lots_held + ' / ' + d.max_lots;
  var rcEl = document.getElementById('recenter-info');
  if (d.recenter_countdown >= 0 && d.started) {
    var rmm = Math.floor(d.recenter_countdown / 60);
    var rss = d.recenter_countdown % 60;
    rcEl.textContent = rmm + ':' + (rss < 10 ? '0' : '') + rss;
    rcEl.style.color = d.recenter_countdown < 60 ? 'var(--warmup)' : 'var(--text-primary)';
  } else { rcEl.textContent = '--'; rcEl.style.color = 'var(--text-muted)'; }

  // Trading
  document.getElementById('roundtrips').textContent = d.roundtrips;
  document.getElementById('total_volume').textContent = d.total_volume.toLocaleString('en-US',{maximumFractionDigits:0}) + ' USD';
  document.getElementById('flip-info').textContent = d.flip_count + ' / ' + (d.flip_pnl >= 0 ? '+' : '') + d.flip_pnl.toFixed(4);
  document.getElementById('best-prices').textContent = (d.best_ask || '--') + ' / ' + (d.best_bid || '--');

  // Buttons
  document.getElementById('btn-start').disabled = d.started || !d.cro_ws_ready;
  document.getElementById('btn-stop').disabled = !d.started;
  document.getElementById('btn-rebuild').disabled = !d.started || d.closing_position || !d.direction_since;
  document.getElementById('btn-close').disabled = d.closing_position;

  // Grid Visualization
  var lvs = d.levels || [];
  var price = d.price || 0;
  var dir = d.direction;

  var entries = lvs.filter(function(l){ return l.type === 'entry'; });
  var lots = lvs.filter(function(l){ return l.type === 'lot'; });

  var gdl = document.getElementById('grid-dir-label');
  if (dir === 1) gdl.textContent = 'LONG \u25B2 \u2014 BUY below';
  else if (dir === -1) gdl.textContent = 'SHORT \u25BC \u2014 SELL above';
  else if (d.direction_since) gdl.textContent = 'NEUTRAL \u25AC \u2014 BUY below + SELL above';
  else gdl.textContent = 'Select direction';

  var cdEl = document.getElementById('recenter-cd');
  if (d.recenter_countdown >= 0 && d.started) {
    var mm = Math.floor(d.recenter_countdown / 60);
    var ss = d.recenter_countdown % 60;
    cdEl.textContent = 'R1: ' + mm + ':' + (ss < 10 ? '0' : '') + ss;
    cdEl.style.color = d.recenter_countdown < 60 ? 'var(--warmup)' : 'var(--text-muted)';
  } else { cdEl.textContent = 'R1: --'; cdEl.style.color = 'var(--text-muted)'; }

  var ordered = [];
  if (dir === 1 || dir === 0) {
    entries.sort(function(a,b){ return a.p - b.p; });
    lots.sort(function(a,b){ return a.p - b.p; });
    ordered = entries.concat([null], lots);
  } else {
    lots.sort(function(a,b){ return b.p - a.p; });
    entries.sort(function(a,b){ return a.p - b.p; });
    ordered = lots.concat([null], entries);
  }

  var rowEl = document.getElementById('grid-row');
  var cellsHtml = '';
  for (var i = 0; i < ordered.length; i++) {
    var l = ordered[i];
    if (l === null) { cellsHtml += '<div class="lv-center"></div>'; continue; }
    var cls = 'lv ';
    var tip = '';
    if (l.type === 'entry') {
      cls += l.pending ? 'lv-pending' : 'lv-empty';
      tip = l.side + ' #' + (entries.indexOf(l)+1) + ' $' + l.p + (l.pending ? ' PENDING' : ' EMPTY');
    } else {
      cls += l.profit ? 'lv-profit' : 'lv-loss';
      tip = l.side + ' lot $' + l.p + ' TP=$' + (l.tp||'--') + (l.profit ? ' \u2191' : ' \u2193');
    }
    cellsHtml += '<div class="' + cls + '" title="' + tip + '"></div>';
  }
  rowEl.innerHTML = cellsHtml;

  var allPrices = lvs.map(function(l){ return l.p; }).filter(function(p){ return p > 0; });
  var cursor = document.getElementById('price-cursor');
  var tag = document.getElementById('grid-price-tag');
  if (allPrices.length > 0 && price > 0) {
    var lo = Math.min.apply(null, allPrices);
    var hi = Math.max.apply(null, allPrices);
    document.getElementById('grid-lo').textContent = lo.toLocaleString();
    document.getElementById('grid-hi').textContent = hi.toLocaleString();
    var pct = hi > lo ? Math.max(0, Math.min(100, (price - lo) / (hi - lo) * 100)) : 50;
    cursor.style.left = 'calc(' + pct + '% - 1.5px)';
    cursor.style.display = 'block';
    tag.style.left = pct + '%';
    tag.textContent = price.toLocaleString();
  } else { cursor.style.display = 'none'; tag.textContent = '--'; }

  document.getElementById('grid-summary').textContent =
    d.entries_pending + ' pending, ' + d.lots_held + ' held (' + d.lots_in_profit + '\u2191 ' + d.lots_in_loss + '\u2193)';

  // Activity Log
  var act = document.getElementById('activity');
  act.innerHTML = (d.activity||[]).map(function(a){ return '<div class="log-entry">' + a + '</div>'; }).join('');
});

// ---- Direction Control ----
function setDirection(dir) {
  var label = dir === 1 ? 'LONG (做多)' : dir === 0 ? 'NEUTRAL (雙向)' : 'SHORT (做空)';
  var hasLots = parseInt(document.getElementById('lots-held').textContent.split('/')[0].trim()) || 0;
  var currentDir = document.getElementById('dir-label').textContent;
  var hasDirection = currentDir.indexOf('LONG') >= 0 || currentDir.indexOf('SHORT') >= 0 || currentDir.indexOf('NEUTRAL') >= 0;

  var desc = dir === 1 ? 'BUY orders below current price'
           : dir === -1 ? 'SELL orders above current price'
           : 'BUY below + SELL above current price';

  if (hasLots > 0 && hasDirection) {
    if (!confirm('Switch to ' + label + '?\n\nWill CLOSE all ' + hasLots + ' positions first, then immediately place ' + desc + '.\n\nConfirm?')) return;
  } else {
    if (!confirm('Set direction to ' + label + '?\n\nWill immediately place ' + desc + '.')) return;
  }

  _dirChanging = true;
  document.getElementById('dir-long').disabled = true;
  document.getElementById('dir-neut').disabled = true;
  document.getElementById('dir-short').disabled = true;

  fetch('/api/set-direction', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({direction: dir})
  }).then(function(r){ return r.json(); }).then(function(d) {
    _dirChanging = false;
    if (d.status !== 'ok') alert(d.message || 'Error');
  }).catch(function(e) {
    _dirChanging = false;
    alert('Network error: ' + e.message);
  });
}

// ---- Engine Controls ----
function startEngine(){
  if(confirm('Start Manual Grid? Select direction after starting.')){
    document.getElementById('btn-start').disabled=true;
    fetch('/api/start',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
      if(d.status!=='starting' && d.status!=='started') alert(d.status);
    });
  }
}
function stopEngine(){
  if(confirm('Stop and cancel all orders?')){
    fetch('/api/stop',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
      if(d.status!=='stopped') alert(d.status);
    });
  }
}
function rebuildGrid(){
  if(confirm('Close position (post-only), then rebuild grid?')){
    document.getElementById('btn-rebuild').disabled=true;
    fetch('/api/rebuild',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
      if(d.status!=='rebuilding') alert(d.status);
    });
  }
}
function closePosition(){
  if(confirm('Cancel all orders and close position (post-only)?')){
    var btn = document.getElementById('btn-close');
    btn.disabled=true; btn.textContent='Closing...';
    fetch('/api/close',{method:'POST'}).then(function(r){return r.json()}).then(function(d){
      if(d.status==='closing' || d.status==='closed'){
        btn.textContent='Closing...'; btn.style.background='linear-gradient(135deg,#f59e0b,#d97706)';
        setTimeout(function(){ btn.textContent='CLOSE'; btn.style.background='linear-gradient(135deg,#4B5563,#374151)'; btn.disabled=false; }, 5000);
      } else if(d.status==='flat'){
        btn.textContent='No position';
        setTimeout(function(){ btn.textContent='CLOSE'; btn.style.background='linear-gradient(135deg,#4B5563,#374151)'; btn.disabled=false; }, 2000);
      } else {
        btn.textContent=d.message||d.status;
        setTimeout(function(){ btn.textContent='CLOSE'; btn.style.background='linear-gradient(135deg,#4B5563,#374151)'; btn.disabled=false; }, 3000);
      }
    }).catch(function(){
      btn.textContent='Error'; btn.disabled=false;
      setTimeout(function(){ btn.textContent='CLOSE'; btn.style.background='linear-gradient(135deg,#4B5563,#374151)'; }, 3000);
    });
  }
}

// ---- Settings Modal ----
function openSettings(){
  document.getElementById('settings-modal').classList.add('show');
  document.getElementById('qty-status').textContent='';
  document.getElementById('key-status').textContent='';
  document.getElementById('grid-settings-status').textContent='';
  document.getElementById('instrument-status').textContent='';
  document.getElementById('set-ak').value='';
  document.getElementById('set-sk').value='';
  fetch('/api/settings').then(function(r){return r.json()}).then(function(d){
    document.getElementById('set-qty').value=d.order_qty||'';
    document.getElementById('set-qty-current').textContent=d.order_qty||'--';
    // QTY tick hint
    var qtick=d.qty_tick||0.0001; var qdec=d.qty_decimals!=null?d.qty_decimals:4;
    document.getElementById('qty-tick-hint').textContent='Min: '+qtick+' | Step: '+qtick+' | Decimals: '+qdec;
    document.getElementById('set-qty').step=qtick; document.getElementById('set-qty').min=qtick;
    document.getElementById('set-qty').placeholder=String(qtick);
    document.getElementById('set-ak-masked').textContent=d.api_key_masked||'--';
    document.getElementById('set-sk-masked').textContent=d.secret_masked||'--';
    // Instrument dropdown
    var sel=document.getElementById('set-instrument');
    sel.innerHTML='';
    var current=d.instrument||'BTCUSD-PERP';
    document.getElementById('set-instrument-current').textContent=current;
    var instruments=d.instrument_list||[current];
    for(var i=0;i<instruments.length;i++){
      var opt=document.createElement('option');
      opt.value=instruments[i]; opt.textContent=instruments[i];
      if(instruments[i]===current) opt.selected=true;
      sel.appendChild(opt);
    }
    if(d.grid){
      document.getElementById('set-levels').value=d.grid.levels||20;
      var sm=d.grid.spacing_mode||'auto';
      // Non-BTC: force manual
      var isBtc=current==='BTCUSD-PERP';
      var smSel=document.getElementById('set-spacing-mode');
      smSel.value=isBtc?sm:'manual';
      smSel.disabled=!isBtc;
      document.getElementById('set-manual-spacing').value=d.grid.manual_spacing||50;
      document.getElementById('set-recenter').value=d.grid.recenter_minutes||60;
      document.getElementById('set-max-lots').value=d.grid.max_lots||60;
      toggleSpacingInput();
    }
  });
}
function closeSettings(){ document.getElementById('settings-modal').classList.remove('show'); }

function toggleSpacingInput(){
  var mode = document.getElementById('set-spacing-mode').value;
  var isBtc=(document.getElementById('set-instrument-current').textContent||'').indexOf('BTCUSD-PERP')>=0;
  document.getElementById('manual-spacing-section').style.display = (mode === 'manual' || !isBtc) ? 'block' : 'none';
}

function applyInstrument(){
  var inst=document.getElementById('set-instrument').value;
  if(!inst) return;
  var st=document.getElementById('instrument-status');
  st.textContent='Switching...'; st.style.color='var(--warmup)';
  fetch('/api/settings/instrument',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({instrument:inst})})
    .then(function(r){return r.json()}).then(function(d){
      if(d.status==='ok'){
        st.textContent='Switched to '+d.instrument; st.style.color='var(--long)';
        document.getElementById('set-instrument-current').textContent=d.instrument;
        // Immediately update navbar instrument badge
        var navInst=document.getElementById('instrument');
        navInst.textContent=d.instrument;
        navInst.classList.remove('flash'); void navInst.offsetWidth; navInst.classList.add('flash');
        // Update QTY to new default
        var dq=d.default_qty||''; var qt=d.qty_tick||0.0001; var qd=d.qty_decimals!=null?d.qty_decimals:4;
        document.getElementById('set-qty').value=dq;
        document.getElementById('set-qty-current').textContent=dq;
        document.getElementById('qty-tick-hint').textContent='Min: '+qt+' | Step: '+qt+' | Decimals: '+qd;
        document.getElementById('set-qty').step=qt; document.getElementById('set-qty').min=qt;
        document.getElementById('set-qty').placeholder=String(qt);
        // Update spacing mode UI
        var isBtc=d.instrument==='BTCUSD-PERP';
        var smSel=document.getElementById('set-spacing-mode');
        if(!isBtc){smSel.value='manual';smSel.disabled=true;}else{smSel.disabled=false;}
        toggleSpacingInput();
      } else { st.textContent=d.message||'Error'; st.style.color='var(--short)'; }
    }).catch(function(){ st.textContent='Network error'; st.style.color='var(--short)'; });
}

function applyQty(){
  var qty=document.getElementById('set-qty').value.trim();
  if(!qty){return;}
  var st=document.getElementById('qty-status');
  st.textContent='Applying...'; st.style.color='var(--warmup)';
  fetch('/api/settings/qty',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({qty:qty})})
    .then(function(r){return r.json()}).then(function(d){
      if(d.status==='ok'){
        st.textContent='Updated to '+d.qty; st.style.color='var(--long)';
        document.getElementById('set-qty-current').textContent=d.qty;
      } else { st.textContent=d.message||'Error'; st.style.color='var(--short)'; }
    }).catch(function(){ st.textContent='Network error'; st.style.color='var(--short)'; });
}

function applyGridSettings(){
  var isBtc=(document.getElementById('set-instrument-current').textContent||'').indexOf('BTCUSD-PERP')>=0;
  var data = {
    levels: parseInt(document.getElementById('set-levels').value) || 20,
    spacing_mode: isBtc ? document.getElementById('set-spacing-mode').value : 'manual',
    manual_spacing: parseFloat(document.getElementById('set-manual-spacing').value) || 50,
    recenter_minutes: parseInt(document.getElementById('set-recenter').value) || 60,
    max_lots: parseInt(document.getElementById('set-max-lots').value) || 60,
  };
  var st = document.getElementById('grid-settings-status');
  st.textContent = 'Applying...'; st.style.color = 'var(--warmup)';
  fetch('/api/settings/grid', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(data)})
    .then(function(r){return r.json()}).then(function(d){
      if(d.status==='ok'){
        st.textContent='Grid settings updated'; st.style.color='var(--long)';
      } else { st.textContent=d.message||'Error'; st.style.color='var(--short)'; }
    }).catch(function(){ st.textContent='Network error'; st.style.color='var(--short)'; });
}

function toggleVis(id){
  var el=document.getElementById(id);
  el.type = el.type==='password' ? 'text' : 'password';
}

function openGuide(){ document.getElementById('guide-modal').classList.add('show'); }
function closeGuide(){ document.getElementById('guide-modal').classList.remove('show'); }
function openMoneyFlow(){
  document.getElementById('mf-modal').classList.add('show');
  var proxy = window.PROXY || '';
  document.getElementById('mf-iframe').src = proxy + '/api/moneyflow';
}
function closeMoneyFlow(){
  document.getElementById('mf-modal').classList.remove('show');
  document.getElementById('mf-iframe').src = 'about:blank';
}
function loadGridSignal(){
  fetch('/api/grid-signal').then(function(r){return r.json()}).then(function(d){
    var badge=document.getElementById('grid-signal-badge');
    var sig=d.signal||'NEUTRAL';
    var labels={'BULLISH':'BULLISH GRID','BEARISH':'BEARISH GRID','NEUTRAL':'NEUTRAL GRID'};
    badge.textContent=labels[sig]||'NEUTRAL GRID';
    badge.className='grid-signal-badge sig-'+sig.toLowerCase();
    badge.title='AI Signal (reference): '+sig+' — '+(d.reason||'')+' ('+(d.date||'')+')';
  }).catch(function(){ var b=document.getElementById('grid-signal-badge'); b.textContent='NO SIGNAL'; b.className='grid-signal-badge sig-neutral'; });
}
loadGridSignal(); setInterval(loadGridSignal, 300000);
function switchMode(mode) {
  if (!confirm('Switch to ' + mode.toUpperCase() + ' mode?\n\nThis will stop the current bot and restart in the new mode.')) return;
  try { window.top.location.href = '/dashboard/__USER_ID__?switch=' + mode; }
  catch(e) { alert('Mode switch only works via the main dashboard'); }
}

function saveApiKey(){
  var ak=document.getElementById('set-ak').value.trim();
  var sk=document.getElementById('set-sk').value.trim();
  if(!ak||!sk){ document.getElementById('key-status').textContent='Both fields required'; document.getElementById('key-status').style.color='var(--short)'; return; }
  if(!confirm('Update API Key and reconnect WS?')) return;
  var st=document.getElementById('key-status');
  var btn=document.getElementById('btn-save-key');
  btn.disabled=true; st.textContent='Reconnecting...'; st.style.color='var(--warmup)';
  fetch('/api/settings/apikey',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_key:ak,secret_key:sk})})
    .then(function(r){return r.json()}).then(function(d){
      btn.disabled=false;
      if(d.status==='ok'){
        st.textContent='Connected with new key'; st.style.color='var(--long)';
        document.getElementById('set-ak').value='';
        document.getElementById('set-sk').value='';
        fetch('/api/settings').then(function(r){return r.json()}).then(function(d2){
          document.getElementById('set-ak-masked').textContent=d2.api_key_masked||'--';
          document.getElementById('set-sk-masked').textContent=d2.secret_masked||'--';
        });
      } else { st.textContent=d.message||'Error'; st.style.color='var(--short)'; }
    }).catch(function(){ btn.disabled=false; st.textContent='Network error'; st.style.color='var(--short)'; });
}
</script></body></html>"""

    @app.route("/")
    def index():
        html = DASHBOARD_HTML.replace('__USER_ID__', USER_ID)
        return Response(html, mimetype="text/html")

    @app.route("/api/start", methods=["POST"])
    def start_api():
        if not engine._cro_ws_ready:
            return jsonify({"status": "Configure API key first"})
        if engine.started:
            return jsonify({"status": "already running"})
        asyncio.run_coroutine_threadsafe(engine.start_grid(), main_loop)
        return jsonify({"status": "starting"})

    @app.route("/api/stop", methods=["POST"])
    def stop_api():
        async def _stop():
            await engine.rest.cancel_all_orders(engine.instrument)
        try:
            future = asyncio.run_coroutine_threadsafe(_stop(), main_loop)
            future.result(timeout=10)
        except Exception as e:
            return jsonify({"status": f"error: {e}"})
        engine.stop()
        engine.save_state()
        return jsonify({"status": "stopped"})

    @app.route("/api/rebuild", methods=["POST"])
    def rebuild_api():
        if not engine.started:
            return jsonify({"status": "not running"})
        asyncio.run_coroutine_threadsafe(engine.cancel_and_rebuild(), main_loop)
        return jsonify({"status": "rebuilding"})

    @app.route("/api/close", methods=["POST"])
    def close_api():
        if engine._closing_position:
            return jsonify({"status": "busy", "message": "Already closing"})
        asyncio.run_coroutine_threadsafe(engine.close_position(), main_loop)
        return jsonify({"status": "closing"})

    @app.route("/api/set-direction", methods=["POST"])
    def set_direction_api():
        data = request.get_json(silent=True) or {}
        direction = data.get("direction")
        if direction not in (1, 0, -1):
            return jsonify({"status": "error", "message": "Invalid direction"})
        future = asyncio.run_coroutine_threadsafe(engine.set_direction(direction), main_loop)
        try:
            result = future.result(timeout=60)
            return jsonify(result)
        except Exception as e:
            return jsonify({"status": "error", "message": str(e)})

    @app.route("/api/stats")
    def stats_api():
        return jsonify(engine.get_stats())

    @app.route("/api/settings", methods=["GET"])
    def get_settings():
        ak = engine.ws.api_key
        sk = engine.ws.secret_key
        inst_list = sorted(_INSTRUMENT_CACHE.keys()) if _INSTRUMENT_CACHE else [engine.instrument]
        return jsonify({
            "instrument": engine.instrument,
            "instrument_list": inst_list,
            "order_qty": engine.order_qty,
            "qty_tick": engine._qty_tick,
            "qty_decimals": engine._qty_decimals,
            "api_key_masked": f"{'•' * max(0, len(ak) - 4)}{ak[-4:]}" if len(ak) >= 4 else ak,
            "secret_masked": f"{'•' * max(0, len(sk) - 4)}{sk[-4:]}" if len(sk) >= 4 else sk,
            "grid": {
                "levels": engine.settings.levels,
                "spacing_mode": engine.settings.spacing_mode,
                "manual_spacing": engine.settings.manual_spacing,
                "recenter_minutes": engine.settings.recenter_minutes,
                "max_lots": engine.settings.max_lots,
            },
        })

    @app.route("/api/settings/qty", methods=["POST"])
    def set_qty():
        data = request.get_json(silent=True) or {}
        new_qty = data.get("qty", "").strip()
        try:
            val = float(new_qty)
            if val <= 0:
                return jsonify({"status": "error", "message": "QTY must be > 0"})
        except (ValueError, TypeError):
            return jsonify({"status": "error", "message": "Invalid QTY"})

        qt = engine._qty_tick
        qd = engine._qty_decimals
        # Must be >= qty_tick
        if val < qt:
            return jsonify({"status": "error", "message": f"QTY must be >= {qt} for {engine.instrument}"})
        # Must be a multiple of qty_tick
        remainder = round(val % qt, qd + 2)
        if remainder > 0 and remainder < qt:
            return jsonify({"status": "error", "message": f"QTY must be a multiple of {qt}"})
        # Normalize to correct decimals
        new_qty = f"{val:.{qd}f}"

        engine.order_qty = new_qty
        engine.save_state()
        logger.info("QTY updated to %s via dashboard", new_qty)
        return jsonify({"status": "ok", "qty": new_qty})

    @app.route("/api/settings/grid", methods=["POST"])
    def set_grid_settings():
        data = request.get_json(silent=True) or {}
        levels = data.get("levels", engine.settings.levels)
        spacing_mode = data.get("spacing_mode", engine.settings.spacing_mode)
        manual_spacing = data.get("manual_spacing", engine.settings.manual_spacing)
        recenter_minutes = data.get("recenter_minutes", engine.settings.recenter_minutes)
        max_lots = data.get("max_lots", engine.settings.max_lots)

        # Validate
        levels = max(5, min(50, int(levels)))
        manual_spacing = max(engine._price_tick, min(engine._max_spacing, float(manual_spacing)))
        recenter_minutes = max(5, min(1440, int(recenter_minutes)))
        max_lots = max(10, min(200, int(max_lots)))
        if spacing_mode not in ("auto", "manual"):
            spacing_mode = "auto"
        # Non-BTC: force manual spacing
        if not _is_btc(engine.instrument):
            spacing_mode = "manual"

        engine.settings.levels = levels
        engine.settings.spacing_mode = spacing_mode
        engine.settings.manual_spacing = manual_spacing
        engine.settings.recenter_minutes = recenter_minutes
        engine.settings.max_lots = max_lots
        engine.save_state()

        logger.info(
            "Grid settings updated: levels=%d spacing=%s(%s) recenter=%dmin max_lots=%d",
            levels, spacing_mode, manual_spacing, recenter_minutes, max_lots,
        )
        return jsonify({"status": "ok"})

    @app.route("/api/settings/instrument", methods=["POST"])
    def set_instrument():
        if engine.started:
            return jsonify({"status": "error", "message": "Stop engine before switching instrument"})
        data = request.get_json(silent=True) or {}
        new_inst = data.get("instrument", "").strip()
        if not new_inst:
            return jsonify({"status": "error", "message": "No instrument specified"})
        if _INSTRUMENT_CACHE and new_inst not in _INSTRUMENT_CACHE:
            return jsonify({"status": "error", "message": f"Unknown instrument: {new_inst}"})

        old_inst = engine.instrument
        engine.settings.instrument = new_inst
        engine._update_instrument_config()

        # Update market feed subscription target and force WS re-subscribe
        engine.market.instrument = new_inst
        asyncio.run_coroutine_threadsafe(engine.market.force_reconnect(), main_loop)

        # Non-BTC: force manual spacing
        if not _is_btc(new_inst):
            engine.settings.spacing_mode = "manual"

        # Auto-set QTY to min qty (qty_tick) for the new instrument
        new_default_qty = f"{engine._qty_tick:.{engine._qty_decimals}f}"
        engine.order_qty = new_default_qty

        engine.save_state()
        logger.info("Instrument switched: %s -> %s (qty=%s)", old_inst, new_inst, new_default_qty)
        return jsonify({
            "status": "ok", "instrument": new_inst,
            "default_qty": new_default_qty,
            "qty_tick": engine._qty_tick,
            "qty_decimals": engine._qty_decimals,
        })

    @app.route("/api/settings/apikey", methods=["POST"])
    def set_apikey():
        if engine.started:
            return jsonify({"status": "error", "message": "Stop engine before changing API key"})
        data = request.get_json(silent=True) or {}
        new_ak = data.get("api_key", "").strip()
        new_sk = data.get("secret_key", "").strip()
        if not new_ak or not new_sk:
            return jsonify({"status": "error", "message": "Both API Key and Secret required"})

        if engine._cro_ws_ready:
            engine.ws.close()
            time.sleep(1)

        engine.ws.api_key = new_ak
        engine.ws.secret_key = new_sk
        engine.rest.api_key = new_ak
        engine.rest.secret_key = new_sk
        engine.ws.source_ip = SOURCE_IP

        engine.ws.connect()
        ok = engine.ws.wait_until_ready(timeout=15)
        if not ok:
            logger.error("WS reconnect with new API key failed")
            engine._cro_ws_ready = False
            return jsonify({"status": "error", "message": "WS auth failed with new key"})

        engine._cro_ws_ready = True
        logger.info("API key updated and WS reconnected via dashboard")

        try:
            import user_manager
            cfg = user_manager.get_user(USER_ID)
            if cfg:
                cfg["api_key"] = new_ak
                cfg["secret_key"] = new_sk
                f = user_manager._get_fernet()
                import json as _json
                encrypted = f.encrypt(_json.dumps(cfg).encode())
                user_manager._config_path(USER_ID).write_bytes(encrypted)
                logger.info("API key persisted to user config")
        except Exception as e:
            logger.warning("Failed to persist API key: %s", e)

        return jsonify({"status": "ok"})

    _MONEYFLOW_DIR = Path("/opt/grid-bot/moneyflow")

    @app.route("/api/moneyflow")
    def moneyflow_api():
        """Serve the latest Money Flow HTML report."""
        if not _MONEYFLOW_DIR.exists():
            return Response("<h2 style='color:#9ca3af;text-align:center;margin-top:40px'>尚無報告 — 請先在本機執行 money_flow.py</h2>", mimetype="text/html")
        htmls = sorted(_MONEYFLOW_DIR.glob("flow_*.html"))
        if not htmls:
            return Response("<h2 style='color:#9ca3af;text-align:center;margin-top:40px'>尚無報告 — 請先在本機執行 money_flow.py</h2>", mimetype="text/html")
        return Response(htmls[-1].read_text(encoding="utf-8"), mimetype="text/html")

    _GRID_SIGNAL_FILE = _MONEYFLOW_DIR / "grid_signal.json"

    @app.route("/api/grid-signal")
    def grid_signal_api():
        """Return latest AI Grid Signal for reference."""
        if not _GRID_SIGNAL_FILE.exists():
            return jsonify({"signal": "NEUTRAL", "reason": "尚無分析", "detail": "", "date": ""})
        try:
            data = json.loads(_GRID_SIGNAL_FILE.read_text(encoding="utf-8"))
            return jsonify(data)
        except Exception:
            return jsonify({"signal": "NEUTRAL", "reason": "讀取失敗", "detail": "", "date": ""})

    def emit_stats_loop():
        while True:
            time.sleep(2)
            try:
                stats = engine.get_stats()
                socketio.emit("stats_update", stats)
            except Exception:
                pass

    return app, socketio, emit_stats_loop


# ============================================================
# Main Entry
# ============================================================

async def main():
    logger.info("=" * 60)
    logger.info("Grid Manual Mode — Server (user=%s)", USER_ID)
    logger.info("=" * 60)

    # Fetch instrument list from CRO (populate cache)
    global _INSTRUMENT_CACHE
    _INSTRUMENT_CACHE = _fetch_instrument_list()

    env_ak = os.environ.get("CRO_API_KEY", "").strip()
    env_sk = os.environ.get("CRO_SECRET_KEY", "").strip()
    if env_ak:
        GRID_CONFIG["API_KEY"] = env_ak
    if env_sk:
        GRID_CONFIG["SECRET_KEY"] = env_sk
    if SOURCE_IP:
        GRID_CONFIG["SOURCE_IP"] = SOURCE_IP

    api_key = GRID_CONFIG.get("API_KEY", "")
    has_key = bool(api_key)

    logger.info(
        "Config: key=%s Network: %s SOURCE_IP: %s PORT: %d",
        f"{api_key[:8]}..." if has_key else "(empty)",
        GRID_CONFIG.get("NETWORK", "?"),
        SOURCE_IP or "(none)",
        DASHBOARD_PORT,
    )

    kline_feed = BinanceKlineFeed()
    # market_feed instrument will be updated after load_state if saved
    market_feed = CroMarketFeed(GRID_CONFIG["WS_MARKET_URL"], instrument=DEFAULT_INSTRUMENT)
    ws_client = CroWSOrderClient(config=GRID_CONFIG)
    rest_client = CroRestClient(config=GRID_CONFIG)
    engine = ManualGridEngine(kline_feed, market_feed, ws_client, rest_client)

    # load_state first so we know the instrument
    if engine.load_state():
        logger.info(
            "Loaded state: instrument=%s dir=%d since=%s lots=%d",
            engine.instrument, engine.state.direction, engine.state.direction_since,
            len(engine.active_lots),
        )
        market_feed.instrument = engine.instrument
    else:
        logger.info("No saved state — press START to begin")

    # BTC: bootstrap Binance kline for sigma; non-BTC: skip
    if _is_btc(engine.instrument):
        await kline_feed.bootstrap()
        if kline_feed.latest_price <= 0:
            logger.error("Failed to get initial price from Binance")
            return
    else:
        logger.info("Non-BTC instrument (%s): Binance kline disabled, manual spacing only", engine.instrument)

    loop = asyncio.get_running_loop()
    ws_client.set_main_loop(loop)
    ws_client.set_on_order_callback(engine.on_order_update)

    if has_key:
        _bind_tls.source_ip = SOURCE_IP if SOURCE_IP else None
        ws_client.connect()
        logger.info("Waiting for CRO WS auth...")
        if ws_client.wait_until_ready(timeout=15):
            engine._cro_ws_ready = True
            logger.info("CRO WS ready")
        else:
            logger.warning("CRO WS auth timeout — user may need to update API key via dashboard")
    else:
        logger.info("No API key configured — user can set via dashboard Settings")

    logger.info(
        "Settings: instrument=%s levels=%d spacing=%s(%s) recenter=%dmin max_lots=%d",
        engine.instrument, engine.settings.levels, engine.settings.spacing_mode,
        engine.settings.manual_spacing, engine.settings.recenter_minutes,
        engine.settings.max_lots,
    )

    app, socketio, emit_stats_loop = create_dashboard(engine, loop)
    dashboard_thread = threading.Thread(
        target=lambda: socketio.run(app, host="0.0.0.0", port=DASHBOARD_PORT, allow_unsafe_werkzeug=True, log_output=False),
        daemon=True,
    )
    dashboard_thread.start()
    stats_thread = threading.Thread(target=emit_stats_loop, daemon=True)
    stats_thread.start()
    logger.info("Dashboard running on http://0.0.0.0:%d", DASHBOARD_PORT)

    tasks = [
        asyncio.create_task(market_feed.run(), name="market_ws"),
        asyncio.create_task(engine.recenter_loop(), name="recenter_loop"),
        asyncio.create_task(engine.state_save_loop(), name="state_save"),
        asyncio.create_task(engine.unrealized_pnl_loop(), name="unrealized_pnl"),
        asyncio.create_task(engine.balance_loop(), name="balance"),
    ]
    if _is_btc(engine.instrument):
        tasks.append(asyncio.create_task(kline_feed.run(), name="kline_ws"))

    # Auto-resume
    if getattr(engine, '_was_started', False) and engine._cro_ws_ready:
        async def _auto_resume():
            for _ in range(30):
                if engine.market.best_bid > 0:
                    break
                await asyncio.sleep(1)
            if engine.market.best_bid > 0 and not engine.started:
                logger.info("Auto-resuming: bot was started before shutdown")
                await engine.start_grid()
            elif engine.market.best_bid <= 0:
                logger.warning("Auto-resume aborted: no market data after 30s")
        tasks.append(asyncio.create_task(_auto_resume(), name="auto_resume"))

    shutdown = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        shutdown.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _signal_handler)
        loop.add_signal_handler(signal.SIGTERM, _signal_handler)
    except NotImplementedError:
        pass

    logger.info("System running. Press Ctrl+C to save state and exit.")

    try:
        await shutdown.wait()
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down...")
        engine.save_state()
        logger.info("State saved")
        engine.stop()
        kline_feed.stop()
        market_feed.stop()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        ws_client.close()
        await rest_client.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n[GridManual-{USER_ID}] Interrupted", flush=True)
