# TradeWatch

A simulated multi-manager hedge fund trading environment for practising **trade support**.

Three PMs with their own strategies and risk limits send orders through an OMS with pre-trade risk checks
and a TWAP execution service that slices parent orders into child orders (both in **C# / .NET 8**), then
through a FIX 4.4 gateway to two simulated brokers. Every component runs as its own service and they talk only through **Kafka**; an order-store service
persists every order event, FIX message and state change to **PostgreSQL**.

On top of that sits the support layer: a monitor with alert rules (stuck orders, broker rejects, duplicate
orders, per-PM order-size anomalies by z-score, FIX session drops, stale market data, Kafka consumer lag,
service heartbeats) that alerts to Telegram and triggers an automatic kill switch for P1 issues; an end-of-day
report with broker statement reconciliation; a `tw` CLI to trace any order across services; Grafana + Loki
for dashboards and logs; an **ops console web app (ASP.NET Core + Angular)** to watch the desk and pause/resume
PMs, cancel orders and resolve alerts; daily database backups.

A chaos service injects hidden, randomly scheduled real-world incidents (broker disconnects, lost execution
reports, bad deploys, duplicate submissions, stale market data, locate shortages, stalled consumers, ...)
so the desk can be practised like a real on-call shift.

```
market-data → oms (C#: strategies + risk) → ems (C#: TWAP) → fix-gateway ⇄ broker-sim
        all events → Kafka → order-store → PostgreSQL ← monitor / tw / Grafana
```

- `ONCALL.md` – on-call handbook (中文)
- `dotnet/` – C# services: `Trading` (OMS, risk, TWAP EMS), `Console` (ASP.NET Core API), `Shared`
- `web/` – Angular front end of the ops console
- `services/` – Python services: market_data, fix_gateway, broker_sim, order_store, monitor
  (plus Python reference versions of oms / ems, runnable as a fallback)
- `tools/` – `tw` CLI, daily report + reconciliation, backup script
- `sql/` – database schema
- `grafana/`, `loki/` – on-call screens and log platform
- `docs/` – incident reports, runbooks, shift handovers
- `step1_...` to `step6_...` – learning steps that led here

Run: `cp .env.example .env` (fill in passwords), put market data in `data/us_1m.csv`
(`python download_data.py`), then `docker compose up -d --build`.
