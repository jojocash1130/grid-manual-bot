# AI Context — Grid Manual Bot

This document provides context for AI assistants working on this codebase.

## What This Is

A **manual grid trading bot** for Crypto.com Exchange perpetual contracts. "Manual" means the user chooses the trading direction (LONG/SHORT) — there is no auto-direction signal. The bot handles grid construction, order placement, fill tracking, and recentering automatically.

## Key Concepts

- **Unidirectional grid**: LONG mode places BUY orders below mid-price; SHORT mode places SELL orders above. Never both directions simultaneously.
- **Spacing**: For BTC, calculated from Binance 1m kline Parkinson sigma. For non-BTC instruments, user provides manual spacing in the dashboard.
- **Recenter**: R1 = periodic rebuild (default 60 min). R3 = rebuild triggered on TP fill. Both recalculate the grid around current mid-price.
- **Direction flip**: Closes all positions via post-only orders, then rebuilds grid in the opposite direction. Requires user confirmation.
- **Multi-user**: Each user runs as a separate Python process with isolated env vars (API keys, source IP, port). Admin panel manages lifecycle.
- **Source IP binding**: On multi-IP servers (AWS EC2), each user's outbound traffic is bound to a specific private IP → Elastic IP, so each user has a unique public IP for CRO API key whitelisting.

## Data Flow

```
Binance WS (1m klines)  ──→  sigma calculation  ──→  grid spacing
CRO Market WS (bid/ask) ──→  mid-price tracking ──→  grid placement
CRO User WS (fills)     ──→  fill detection     ──→  TP placement / recenter
Flask+SocketIO           ──→  dashboard UI       ──→  user controls
```

## File Responsibilities

| File | Role | Key Classes/Functions |
|------|------|----------------------|
| `grid_manual.py` | Core bot logic (~3500 lines). Grid state machine, WS connections, order management, Flask dashboard. | Main asyncio event loop, grid rebuild, fill handler |
| `configs.py` | All configuration from env vars. Network endpoints, API credentials, server settings. | `GRID_CONFIG` dict |
| `cro_ws_order.py` | WebSocket client for CRO user channel (order placement, cancel, fill notifications). | `CroWSOrderClient` |
| `cro_rest_client.py` | REST client for CRO (position query, cancel-all, account info). | `CroRestClient` |
| `admin_panel.py` | FastAPI server. User CRUD, bot start/stop via subprocess, reverse proxy to per-user dashboards. | FastAPI `app`, PID management |
| `user_manager.py` | Encrypted config storage (Fernet). User create/read/delete, invite code generation. | `create_slot()`, `get_user()` |

## State Persistence

- `data/{user_id}/grid_manual_state.json` — Grid state (direction, lots, fills, PnL). Survives bot restarts.
- `data/{user_id}/config.json` — Encrypted user config (API keys, source IP, port).
- `data/{user_id}/grid_manual.log` — Rotating log file (5MB x 3).
- `data/{user_id}/bot.pid` — PID file for process management.

## Important Patterns

1. **Env-first config**: All secrets and per-user settings come from environment variables, never hardcoded.
2. **Process isolation**: Each user bot is a separate Python process spawned by admin_panel. Crash isolation.
3. **Source IP binding**: Uses socket monkey-patch with thread-local storage. Do not modify this pattern — see `feedback_no_touch_source_ip.md`.
4. **Post-only orders**: Grid entries use post-only to avoid taker fees. If rejected, the bot retries on next tick.
5. **TP = spacing x tp_mult**: Manual grid uses tp_mult=1.0 (equal spacing), different from auto grid's 3.0.

## Common Tasks

- **Add a new instrument config field**: Update `_fetch_instrument_list()` in `grid_manual.py`.
- **Change grid defaults**: Edit `DEFAULT_*` constants at top of `grid_manual.py` (not configs.py — those are env-level).
- **Add admin API endpoint**: Add to `admin_panel.py`, update ADMIN_HTML JavaScript.
- **Modify user config fields**: Update `user_manager.py` create/read functions and admin panel UI.
