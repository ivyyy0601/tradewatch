# TradeWatch

A simulated multi-manager hedge fund trading environment for practising **trade support**:
three PMs with their own strategies and risk limits, an OMS, pre-trade risk checks, a TWAP execution algo
slicing parent orders into child orders, two brokers over FIX 4.4, end-of-day broker statements,
and hidden, randomly scheduled real-world incidents (broker disconnects, lost execution reports,
bad deploys, stale market data, locate shortages, ...).

The on-call desk is built from Grafana dashboards, Loki for logs and PostgreSQL for orders, fills and
FIX messages. My own monitoring, reconciliation and incident write-ups live in `my_tools/` and `docs/`.

- `ONCALL.md` – on-call handbook (中文)
- `engine.py` – trading system (OMS, risk, EMS/TWAP, FIX gateway, broker simulator)
- `sql/01_schema.sql` – database schema
- `grafana/`, `loki/` – on-call screens and log platform
- `step1_...` to `step6_...` – learning steps that led here
