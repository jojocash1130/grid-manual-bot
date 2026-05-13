# -*- coding: utf-8 -*-
"""
CRO REST client for batch orders + positions.
Uses the same signing logic as cro_position_manager.py.
"""
import hashlib
import hmac
import json
import logging
import time
from typing import Any, Optional

import aiohttp

logger = logging.getLogger(__name__)


def _is_obj(x):
    return isinstance(x, dict)


def _is_arr(x):
    return isinstance(x, list)


def _array_to_string(arr) -> str:
    s = ""
    for v in arr:
        if _is_obj(v):
            s += _object_to_string(v)
        elif _is_arr(v):
            s += _array_to_string(v)
        else:
            s += str(v)
    return s


def _object_to_string(obj: dict) -> str:
    if obj is None:
        return ""
    out = ""
    for k in sorted(obj.keys()):
        v = obj[k]
        out += str(k)
        if _is_arr(v):
            out += _array_to_string(v)
        elif _is_obj(v):
            out += _object_to_string(v)
        else:
            out += str(v)
    return out


class CroRestClient:
    def __init__(self, config: dict):
        self.api_key = config["API_KEY"]
        self.secret_key = config["SECRET_KEY"]
        self.base_url = config["BASE_URL"]
        self._source_ip = config.get("SOURCE_IP", "") or ""
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self):
        if self._session is None or self._session.closed:
            local_addr = (self._source_ip, 0) if self._source_ip else None
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, local_addr=local_addr),
                timeout=aiohttp.ClientTimeout(total=10.0, connect=5.0),
            )

    def _sign(self, method: str, req_id: int, nonce: int, params: dict) -> str:
        params_str = _object_to_string(params)
        payload = f"{method}{req_id}{self.api_key}{params_str}{nonce}"
        return hmac.new(
            self.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _post(self, method: str, params: dict) -> Optional[dict]:
        await self._ensure_session()
        req_id = int(time.time() * 1000)
        nonce = int(time.time() * 1000)
        body = {
            "id": req_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "sig": self._sign(method, req_id, nonce, params),
            "nonce": nonce,
        }
        url = f"{self.base_url}/{method}"
        try:
            async with self._session.post(url, json=body) as resp:
                if resp.content_type and "json" not in resp.content_type:
                    text = await resp.text()
                    logger.error(
                        "REST %s HTTP %d non-JSON response: %s",
                        method, resp.status, text[:200],
                    )
                    return None
                data = await resp.json()
                if data.get("code") == 0:
                    return data.get("result")
                err_code = data.get("code")
                err_msg = data.get("message", "")
                logger.error(
                    "REST %s failed | code=%s | message=%s | params=%s",
                    method, err_code, err_msg, json.dumps(params, ensure_ascii=False)[:500],
                )
                return None
        except Exception as e:
            logger.error("REST %s exception: %s", method, e)
            return None

    async def create_order_list(self, orders: list[dict]) -> Optional[dict]:
        """Batch create orders (max 10 per call)."""
        return await self._post("private/create-order-list", {"order_list": orders})

    async def cancel_order_list(self, order_ids: list[str]) -> Optional[dict]:
        """Batch cancel orders by order_id."""
        items = [{"order_id": oid} for oid in order_ids]
        return await self._post("private/cancel-order-list", {"order_list": items})

    async def cancel_all_orders(self, instrument: str) -> Optional[dict]:
        return await self._post(
            "private/cancel-all-orders",
            {"instrument_name": instrument, "type": "LIMIT"},
        )

    async def get_positions(self, instrument: str) -> float:
        """Return signed position quantity (positive=long, negative=short).
        CRO API quantity is already signed: positive=long, negative=short.
        """
        result = await self._post("private/get-positions", {"instrument_name": instrument})
        if not result:
            return 0.0
        data = result.get("data") or result.get("position_list") or []
        if isinstance(data, list):
            for pos in data:
                if pos.get("instrument_name") == instrument:
                    return float(pos.get("quantity", "0") or "0")
        return 0.0

    async def get_position_detail(self, instrument: str) -> dict:
        """Return full position detail including unrealized PnL."""
        result = await self._post("private/get-positions", {"instrument_name": instrument})
        if not result:
            return {}
        data = result.get("data") or result.get("position_list") or []
        if isinstance(data, list):
            for pos in data:
                if pos.get("instrument_name") == instrument:
                    return pos
        return {}

    async def get_open_orders(self, instrument: str) -> list:
        result = await self._post("private/get-open-orders", {"instrument_name": instrument})
        if not result:
            return []
        return result.get("data") or result.get("order_list") or []

    async def get_balance_usd(self) -> float:
        """Return USD cash balance from user-balance endpoint."""
        result = await self._post("private/user-balance", {})
        if not result:
            return 0.0
        rows = result.get("data") or []
        if not rows:
            return 0.0
        return float(rows[0].get("total_cash_balance", 0) or 0)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
