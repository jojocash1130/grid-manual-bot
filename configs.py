import os

# ============================================
# Manual Grid Strategy Parameters
# User selects direction (LONG/SHORT) and instrument manually.
# No auto-direction signal — all grid params are set from dashboard.
# ============================================

# -- Sigma (for BTC auto-spacing calculation) --
LOOKBACK = 25            # 1m candles for Parkinson sigma

# -- Order size --
ORDER_QTY = os.getenv("ORDER_QTY", "0.0001")  # BTC per lot (env override for multi-user)

# ============================================
# Network switching: PUBLIC (direct) vs CDC (local proxy)
# ============================================
CRO_NETWORK = os.getenv("CRO_NETWORK", "PUBLIC").strip().upper()

_CRO_PUBLIC_ENDPOINTS = {
    "BASE_URL": "https://api.crypto.com/exchange/v1",
    "WS_USER_URL": "wss://stream.crypto.com/exchange/v1/user",
    "WS_MARKET_URL": "wss://stream.crypto.com/exchange/v1/market",
}

_CRO_CDC_ENDPOINTS = {
    "BASE_URL": "http://api.crypto.local:11101/v1",
    "WS_USER_URL": "ws://api.crypto.local:21101/v2/user",
    "WS_MARKET_URL": "ws://api.crypto.local:24101",
}

_cro_endpoints = _CRO_CDC_ENDPOINTS if CRO_NETWORK == "CDC" else _CRO_PUBLIC_ENDPOINTS

# ============================================
# API Config
# Env vars take precedence (for multi-user server mode)
# ============================================
GRID_CONFIG = {
    "API_KEY": os.getenv("CRO_API_KEY", ""),
    "SECRET_KEY": os.getenv("CRO_SECRET_KEY", ""),
    "NETWORK": CRO_NETWORK,
    "BASE_URL": _cro_endpoints["BASE_URL"],
    "WS_USER_URL": _cro_endpoints["WS_USER_URL"],
    "WS_MARKET_URL": _cro_endpoints["WS_MARKET_URL"],
}

# ============================================
# Multi-user server settings (env vars)
# ============================================
SOURCE_IP = os.getenv("SOURCE_IP", "")            # bind outbound traffic to this IP
DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "5559"))
USER_ID = os.getenv("USER_ID", "default")

# Inject SOURCE_IP into GRID_CONFIG so REST client can use it
GRID_CONFIG["SOURCE_IP"] = SOURCE_IP
