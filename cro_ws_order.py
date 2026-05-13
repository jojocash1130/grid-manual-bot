import asyncio
import hashlib
import hmac
import json
import logging
import socket
import threading
import time
from typing import Any, Optional

from websocket import WebSocketApp


logger = logging.getLogger(__name__)

STALE_ORDER_CODES = {212, 40401}
STALE_ORDER_MESSAGES = {"INVALID_ORDERID", "NOT_FOUND"}


class CroWSOrderClient:
    def __init__(self, config: dict):
        self.api_key = config["API_KEY"]
        self.secret_key = config["SECRET_KEY"]
        self.ws_url = config.get("WS_USER_URL", "wss://stream.crypto.com/exchange/v1/user")
        self.source_ip = config.get("SOURCE_IP", "")
        self.ws: Optional[WebSocketApp] = None
        self.ws_thread: Optional[threading.Thread] = None
        self.is_connected = False
        self.is_logged_in = False
        self.running = True

        self._req_id_counter = 0
        self._lock = threading.Lock()
        self._login_event = threading.Event()
        self._main_loop = None
        self._pending_requests: dict[int, asyncio.Future] = {}

        self.latest_position: dict[str, float] = {}
        self.latest_position_detail: dict[str, dict] = {}  # full position data from WS
        self._position_lock = threading.Lock()

        # order update callback (set by grid engine)
        self._on_order_callback = None

    def set_main_loop(self, loop):
        self._main_loop = loop

    def set_on_order_callback(self, callback):
        self._on_order_callback = callback

    def _get_req_id(self) -> int:
        with self._lock:
            self._req_id_counter += 1
            return int(time.time() * 1000) + self._req_id_counter

    def _params_to_string(self, params: dict[str, Any]) -> str:
        if not params:
            return ""
        result = ""
        for key, value in sorted(params.items()):
            result += str(key)
            if isinstance(value, dict):
                result += self._params_to_string(value)
            elif isinstance(value, list):
                result += json.dumps(value, separators=(",", ":"))
            else:
                result += str(value)
        return result

    def _sign(self, method: str, req_id: int, nonce: int, params: dict[str, Any]) -> str:
        payload = f"{method}{req_id}{self.api_key}{self._params_to_string(params)}{nonce}"
        return hmac.new(
            self.secret_key.encode("utf-8"),
            payload.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    def _build_login_payload(self) -> tuple[dict[str, Any], int]:
        req_id = self._get_req_id()
        nonce = int(time.time() * 1000)
        return {
            "id": req_id,
            "method": "public/auth",
            "api_key": self.api_key,
            "sig": self._sign("public/auth", req_id, nonce, {}),
            "nonce": nonce,
        }, req_id

    def _build_payload(self, method: str, params: dict[str, Any]) -> tuple[dict[str, Any], int]:
        req_id = self._get_req_id()
        nonce = int(time.time() * 1000)
        return {
            "id": req_id,
            "method": method,
            "params": params,
            "nonce": nonce,
        }, req_id

    async def _safe_set_future(self, future: asyncio.Future, value: dict[str, Any]):
        if not future.done():
            future.set_result(value)

    def _resolve_pending(self, request_id: int, payload: dict[str, Any]):
        future = self._pending_requests.pop(request_id, None)
        if future is None:
            return
        if self._main_loop and not self._main_loop.is_closed():
            asyncio.run_coroutine_threadsafe(self._safe_set_future(future, payload), self._main_loop)
        elif not future.done():
            future.set_result(payload)

    def _on_open(self, ws):
        self.is_connected = True
        self.is_logged_in = False
        logger.info("CRO WS order connected: %s", self.ws_url)
        logger.info("CRO WS order connected (print): %s", self.ws_url)
        time.sleep(1)
        payload, req_id = self._build_login_payload()
        logger.info("CRO WS auth request id=%s", req_id)
        ws.send(json.dumps(payload))

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
        except Exception:
            logger.exception("CRO WS order invalid JSON: %s", message)
            return

        if data.get("method") == "public/heartbeat":
            recv_id = data.get("id", 0)
            ws.send(json.dumps({"id": recv_id, "method": "public/respond-heartbeat"}))
            return

        if data.get("method") == "public/auth":
            if data.get("code") == 0:
                self.is_logged_in = True
                logger.info("CRO WS auth ok")
                logger.info("CRO WS auth ok (print)")
                sub = {
                    "id": int(time.time() * 1000),
                    "method": "subscribe",
                    "params": {"channels": ["user.positions", "user.order"]},
                    "nonce": int(time.time() * 1000),
                }
                ws.send(json.dumps(sub))
                logger.info("CRO WS subscribing user.positions + user.order")
            else:
                logger.error("CRO WS auth failed: %s", data)
                logger.error("CRO WS auth failed (print): %s", data)
            self._login_event.set()
            return

        if data.get("method") == "subscribe":
            result = data.get("result") or {}
            channel = result.get("channel") or (result.get("subscription") or "")

            if "user.positions" in channel:
                items = result.get("data") or []
                if items:
                    with self._position_lock:
                        for item in items:
                            inst = item.get("instrument_name", "")
                            qty = float(item.get("quantity", "0") or "0")
                            side = (item.get("side") or "").upper()
                            if side == "SELL":
                                qty = -abs(qty)
                            elif side == "BUY":
                                qty = abs(qty)
                            else:
                                qty = 0.0
                            self.latest_position[inst] = qty
                            self.latest_position_detail[inst] = item
                else:
                    logger.info("CRO WS subscribed user.positions (print)")
                    logger.info("CRO WS subscribed user.positions")

            if "user.order" in channel:
                items = result.get("data") or []
                if items:
                    self._dispatch_order_updates(items)
                else:
                    logger.info("CRO WS subscribed user.order (print)")
                    logger.info("CRO WS subscribed user.order")
            return

        request_id = data.get("id")
        if request_id is not None:
            self._resolve_pending(
                request_id,
                {
                    "code": data.get("code"),
                    "result": data.get("result"),
                    "raw": data,
                },
            )

    def _dispatch_order_updates(self, items: list):
        if not self._on_order_callback:
            return
        for order in items:
            try:
                if asyncio.iscoroutinefunction(self._on_order_callback):
                    if self._main_loop and not self._main_loop.is_closed():
                        asyncio.run_coroutine_threadsafe(
                            self._on_order_callback(order), self._main_loop
                        )
                else:
                    self._on_order_callback(order)
            except Exception as e:
                logger.error("on_order callback error: %s", e)

    def _on_error(self, ws, error):
        logger.error("CRO WS order error: %s", error)
        logger.error("CRO WS order error (print): %s", error)

    def _on_close(self, ws, close_status_code, close_msg):
        logger.warning("CRO WS order closed: %s %s", close_status_code, close_msg)
        logger.warning("CRO WS order closed (print): %s %s", close_status_code, close_msg)
        self.is_connected = False
        self.is_logged_in = False
        self._login_event.clear()

    def connect(self):
        self.is_connected = False
        self.is_logged_in = False
        self._login_event.clear()

        def run_ws():
            # Enable thread-local source IP binding (see grid_manual.py monkey-patch)
            from grid_manual import _bind_tls
            if self.source_ip:
                _bind_tls.source_ip = self.source_ip

            socket_options = ((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),)
            while self.running:
                try:
                    logger.info("CRO WS order connecting %s", self.ws_url)
                    self.ws = WebSocketApp(
                        self.ws_url,
                        on_open=self._on_open,
                        on_message=self._on_message,
                        on_error=self._on_error,
                        on_close=self._on_close,
                    )
                    self.ws.run_forever(sockopt=socket_options)
                except Exception as exc:
                    logger.warning("CRO WS order reconnect after error: %s", exc)
                    logger.info("CRO WS order reconnect after error: %s", exc)
                if self.running:
                    logger.info("CRO WS order retry in 5s")
                    time.sleep(5)

        self.running = True
        self.ws_thread = threading.Thread(target=run_ws, daemon=True)
        self.ws_thread.start()

    def wait_until_ready(self, timeout=10):
        return self._login_event.wait(timeout=timeout)

    async def _request(self, method: str, params: dict[str, Any], timeout: float = 2.0):
        if not self.is_connected or not self.is_logged_in or self.ws is None:
            logger.warning("CRO WS order not ready for method=%s", method)
            return None

        if self._main_loop is None:
            self._main_loop = asyncio.get_running_loop()

        payload, req_id = self._build_payload(method, params)
        future = asyncio.Future()
        with self._lock:
            self._pending_requests[req_id] = future

        self.ws.send(json.dumps(payload))

        try:
            response = await asyncio.wait_for(future, timeout=timeout)
        except asyncio.TimeoutError:
            with self._lock:
                self._pending_requests.pop(req_id, None)
            logger.error("CRO WS request timeout method=%s req_id=%s", method, req_id)
            return None

        if response.get("code") == 0:
            return response.get("result")

        code = response.get("code")
        raw = response.get("raw") or {}
        message = str(raw.get("message") or "")
        if method in {"private/amend-order", "private/cancel-order"} and (
            code in STALE_ORDER_CODES or message in STALE_ORDER_MESSAGES
        ):
            logger.info(
                "CRO WS stale order reference method=%s req_id=%s code=%s message=%s",
                method, req_id, code, message,
            )
            return None

        logger.error(
            "CRO WS request failed | method=%s | code=%s | message=%s | req_id=%s | params=%s",
            method, code, message, req_id, json.dumps(params, ensure_ascii=False)[:500],
        )
        return None

    async def place_order(
        self,
        instrument: str,
        side: str,
        quantity: str,
        order_type: str = "MARKET",
        timeout: float = 2.0,
        price: Optional[str] = None,
        post_only: bool = False,
        client_oid: Optional[str] = None,
        stp_scope: str = "M",
        stp_inst: str = "B",
        stp_id: str = "0",
        time_in_force: Optional[str] = None,
    ):
        params: dict[str, Any] = {
            "instrument_name": instrument,
            "side": side,
            "type": order_type,
            "quantity": quantity,
            "broker_id": "JASP",
            "stp_scope": stp_scope,
            "stp_inst": stp_inst,
            "stp_id": stp_id,
        }

        if order_type.upper() == "LIMIT":
            if price is None:
                raise ValueError("LIMIT order requires price")
            params["price"] = price
            params["time_in_force"] = time_in_force or "GOOD_TILL_CANCEL"
        else:
            params["time_in_force"] = time_in_force or "IMMEDIATE_OR_CANCEL"

        if post_only:
            params["exec_inst"] = ["POST_ONLY"]
        if client_oid:
            params["client_oid"] = client_oid

        return await self._request("private/create-order", params, timeout=timeout)

    async def cancel_order(
        self,
        order_id: Optional[str] = None,
        client_oid: Optional[str] = None,
        timeout: float = 2.0,
    ):
        params: dict[str, Any] = {}
        if order_id:
            params["order_id"] = str(order_id)
        if client_oid:
            params["client_oid"] = client_oid
        if not params:
            raise ValueError("cancel_order requires order_id or client_oid")
        return await self._request("private/cancel-order", params, timeout=timeout)

    async def amend_order(
        self,
        new_price: str,
        new_quantity: str,
        order_id: Optional[str] = None,
        orig_client_oid: Optional[str] = None,
        client_oid: Optional[str] = None,
        timeout: float = 2.0,
    ):
        params: dict[str, Any] = {
            "new_price": new_price,
            "new_quantity": new_quantity,
        }
        if order_id:
            params["order_id"] = str(order_id)
        if orig_client_oid:
            params["orig_client_oid"] = orig_client_oid
        if client_oid:
            params["client_oid"] = client_oid
        if "order_id" not in params and "orig_client_oid" not in params:
            raise ValueError("amend_order requires order_id or orig_client_oid")
        return await self._request("private/amend-order", params, timeout=timeout)

    async def cancel_all_orders(
        self,
        instrument_name: str,
        order_type: str = "LIMIT",
        timeout: float = 2.0,
    ):
        return await self._request(
            "private/cancel-all-orders",
            {"instrument_name": instrument_name, "type": order_type},
            timeout=timeout,
        )

    async def get_open_orders(self, instrument_name: str, timeout: float = 2.0):
        return await self._request(
            "private/get-open-orders",
            {"instrument_name": instrument_name},
            timeout=timeout,
        )

    def get_position(self, instrument: str) -> float:
        with self._position_lock:
            return self.latest_position.get(instrument, 0.0)

    def get_position_detail(self, instrument: str) -> dict:
        with self._position_lock:
            return self.latest_position_detail.get(instrument, {})

    def set_position(self, instrument: str, qty: float):
        with self._position_lock:
            self.latest_position[instrument] = qty

    def close(self):
        self.running = False
        if self.ws:
            self.ws.close()
