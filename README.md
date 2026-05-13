# Grid Manual Bot — Crypto.com Perpetual Grid Trading 🤖📈

> A multi-user, **manual-direction grid trading bot** built for [Crypto.com Exchange](https://crypto.com/exch/bvmzsu9zuq) perpetual contracts. You pick the direction, the bot does the rest.

---

## 🏦 About Crypto.com Exchange

[Crypto.com](https://crypto.com/exch/bvmzsu9zuq) is one of the world's largest crypto platforms, serving **100M+ users** across 90+ countries.

| Feature | Details |
|---------|---------|
| **Supported Coins** | 425+ cryptocurrencies |
| **Trading Products** | Spot, Margin, Perpetuals, Futures, Options |
| **Leverage** | Up to 200x on derivatives |
| **Maker / Taker Fee** | 0.075% base (volume & CRO staking discounts available) |
| **Security** | $750M insurance fund, multi-jurisdiction licenses |
| **Extras** | Visa Card, NFT Marketplace, DeFi Wallet, Earn |

Crypto.com is especially competitive for **perpetual contract** traders — deep liquidity, low latency API, and 245+ PERP pairs make it ideal for grid strategies.

### 🎁 Referral — 20% Fee Rebate

> **Use this link to sign up and get 20% trading fee rebate:**
>
> **👉 [https://crypto.com/exch/bvmzsu9zuq](https://crypto.com/exch/bvmzsu9zuq)**
>
> If you have **high trading volume**, reach out for a **higher rebate tier** — see [Contact](#-contact) below.

---

## 🧩 What Is This Bot?

A **unidirectional grid trading bot** that automates order placement, fill tracking, take-profit, and grid recentering on Crypto.com perpetual contracts.

**"Manual"** means **you decide the direction** (LONG or SHORT) — the bot handles everything else:

```
You choose: LONG BTC-PERP-USDT
  → Bot places 20 BUY limit orders below mid-price
  → Each fill triggers a TP order above
  → Grid auto-recenters every 60 min
  → Rinse and repeat 🔄
```

### Core Features

| Feature | Description |
|---------|-------------|
| 🎯 **Manual Direction** | You decide LONG or SHORT — full control, no black-box signals |
| 🪙 **245+ Instruments** | All Crypto.com PERP pairs supported (BTC, ETH, SOL, DOGE...) |
| 📐 **Smart Spacing** | BTC auto-calculates from Binance 1m kline volatility; other coins use manual spacing |
| 🔄 **Auto Recenter** | Periodic (R1, default 60 min) + on-TP (R3) grid rebuild around current price |
| 🔃 **Direction Flip** | One-click flip from LONG to SHORT (or vice versa) with confirmation dialog |
| 👥 **Multi-User Server** | Admin panel manages multiple users, each with isolated API keys & source IP |
| 🔐 **Encrypted Storage** | API keys encrypted at rest with Fernet |
| 📊 **Real-Time Dashboard** | Live grid visualization, PnL tracking, controls — all in your browser |
| 🎟️ **Invite Code Auth** | Share access via invite codes — no complex auth setup needed |

---

## 🏗️ Architecture

```
admin_panel.py            FastAPI admin — user CRUD, bot start/stop, reverse proxy
  ├── user_manager.py     Encrypted user config storage
  └── grid_manual.py      The grid bot process (one per user)
        ├── configs.py          Environment-based configuration
        ├── cro_ws_order.py     Crypto.com WebSocket order client
        └── cro_rest_client.py  Crypto.com REST API client
```

```
Data Flow:

Binance WS (1m klines)  ──→  sigma calculation  ──→  grid spacing
CRO Market WS (bid/ask) ──→  mid-price tracking ──→  grid placement
CRO User WS (fills)     ──→  fill detection     ──→  TP placement / recenter
Flask + SocketIO         ──→  real-time dashboard ──→  user controls
```

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+**
- A **Crypto.com Exchange** account with API trading enabled
  - Don't have one yet? [Sign up here (20% fee rebate)](https://crypto.com/exch/bvmzsu9zuq)

### Option A: Single User (Local)

```bash
# 1. Clone the repo
git clone https://github.com/0xkcya/grid-manual-bot.git
cd grid-manual-bot

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your Crypto.com API key & secret

# 4. Run the bot
python grid_manual.py

# 5. Open dashboard
# Visit http://localhost:5560 in your browser
```

### Option B: Multi-User Server

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start admin panel
python admin_panel.py

# 3. Access admin panel
# Visit http://localhost:8000
#   → Create user slots (assign Source IP + Port)
#   → Share invite codes with users
#   → Users access via http://your-server/dashboard/
```

---

## ⚙️ Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CRO_API_KEY` | *(required)* | Crypto.com API key |
| `CRO_SECRET_KEY` | *(required)* | Crypto.com secret key |
| `CRO_NETWORK` | `PUBLIC` | `PUBLIC` (direct) or `CDC` (local proxy) |
| `SOURCE_IP` | *(empty)* | EC2 private IP for outbound binding |
| `DASHBOARD_PORT` | `5560` | Per-user dashboard port |
| `USER_ID` | `default` | Unique user identifier |
| `ORDER_QTY` | `0.0001` | BTC quantity per grid lot |
| `MASTER_KEY` | *(auto)* | Fernet encryption key for user configs |

### Grid Parameters (Dashboard Configurable)

| Parameter | Default | Description |
|-----------|---------|-------------|
| Levels | 20 | Number of entry grid levels |
| Corridor Mult | 0.75 | Grid corridor width multiplier |
| TP Mult | 1.0 | Take-profit distance = spacing x TP_MULT |
| Max Lots | 60 | Maximum concurrent filled lots |
| Recenter Minutes | 60 | Periodic grid rebuild interval |

---

## 📁 File Overview

| File | Purpose |
|------|---------|
| `grid_manual.py` | Main bot — grid logic, WS connections, dashboard |
| `admin_panel.py` | FastAPI admin — user management, bot lifecycle, reverse proxy |
| `user_manager.py` | Encrypted user config CRUD |
| `configs.py` | Centralized configuration from env vars |
| `cro_ws_order.py` | Crypto.com WebSocket order/fill client |
| `cro_rest_client.py` | Crypto.com REST API client |
| `AI_CONTEXT.md` | Developer context for AI-assisted development |

---

## ⚠️ Disclaimer

> **This software is provided as-is for educational and research purposes.**
>
> - Trading cryptocurrencies involves **significant risk**. You can lose some or all of your capital.
> - Past performance does not guarantee future results.
> - The authors are **not responsible** for any financial losses incurred from using this bot.
> - Always start with **small position sizes** and test thoroughly before scaling up.
> - Make sure you understand how grid trading works before deploying real capital.

---

## 🎁 Support This Project

If this bot is useful to you, the best way to support it is to **sign up for Crypto.com using the referral link below** — you get a **20% fee rebate**, and it helps fund continued development:

> **👉 [https://crypto.com/exch/bvmzsu9zuq](https://crypto.com/exch/bvmzsu9zuq) — 20% Trading Fee Rebate**

---

## 📬 Contact

Have questions, feature requests, bug reports, or want to negotiate a **higher referral rebate**?

> **Email: [0xkcya@gmail.com](mailto:0xkcya@gmail.com)**

Feel free to reach out for:
- 🐛 Bug reports & feature requests
- 💬 General questions about grid trading
- 🤝 Higher rebate tiers for high-volume traders
- 🧑‍💻 Collaboration opportunities

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

<p align="center">
  <b>Built with ☕ and Python by <a href="mailto:0xkcya@gmail.com">CASH</a></b><br>
  <sub>Automate your grid, not your decisions.</sub>
</p>
