"""
每日复盘报告 + 对账（monitor 在券商对账单到了以后自动运行；也可以手动：tw report）
================================================
对账：券商对账单（data/statements/*.csv）里的每一笔成交 vs 我们的 executions 表，按成交编号（FIX tag 17）比
  MISSING_IN_OURS    券商有、我们没有 → 我们漏记了成交，持仓是错的（最严重）
  MISSING_AT_BROKER  我们有、券商没有
  QTY_OR_PRICE_DIFF  两边都有，但数量或价格不一样
报告写到 data/reports/report_<日期>_<时间>.md
"""

import csv
import glob
import os
from datetime import datetime, timedelta, timezone

from common import connect
from config import BROKERS

STATEMENT_DIR = os.getenv("STATEMENT_DIR", "data/statements")
REPORT_DIR = os.getenv("REPORT_DIR", "data/reports")


def statement_files():
    """{(日期_时间): {券商: 文件}}"""
    sessions = {}
    for path in glob.glob(os.path.join(STATEMENT_DIR, "*.csv")):
        name = os.path.basename(path)[:-4]
        broker, _, suffix = name.partition("_")
        if broker in BROKERS:
            sessions.setdefault(suffix, {})[broker] = path
    return sessions


def pending_sessions():
    """两家券商的对账单都到了、但还没出报告的交易日。"""
    done = {os.path.basename(p)[7:-3] for p in glob.glob(os.path.join(REPORT_DIR, "report_*.md"))}
    return sorted(s for s, files in statement_files().items() if len(files) == len(BROKERS) and s not in done)


def _mtime(path):
    return datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)


def _window(broker, path):
    """这份对账单覆盖的时间：上一份对账单之后 → 这一份生成的时候。"""
    files = sorted(glob.glob(os.path.join(STATEMENT_DIR, f"{broker}_*.csv")), key=os.path.getmtime)
    end = _mtime(path)
    earlier = [f for f in files if _mtime(f) < end - timedelta(seconds=1)]
    start = _mtime(earlier[-1]) if earlier else end - timedelta(hours=8)
    return start, end


def recon(cur, broker, path, suffix):
    with open(path) as fh:
        theirs = {r["exec_id"]: r for r in csv.DictReader(fh)}
    start, end = _window(broker, path)
    cur.execute("SELECT exec_id, qty, price, child_id FROM executions WHERE broker = %s AND ts > %s AND ts <= %s",
                (broker, start, end))
    ours = {r[0]: r for r in cur.fetchall()}
    if theirs:
        cur.execute("SELECT exec_id, qty, price, child_id FROM executions WHERE exec_id = ANY(%s)", (list(theirs),))
        for r in cur.fetchall():
            ours.setdefault(r[0], r)
    breaks = []
    for eid, t in theirs.items():
        o = ours.get(eid)
        if o is None:
            breaks.append((eid, "MISSING_IN_OURS", f"{t['account']} {t['side']} {t['qty']} {t['symbol']} @ {t['price']} "
                                                   f"child {t['cl_ord_id']} at {t['exec_time']}"))
        elif int(o[1]) != int(t["qty"]) or abs(float(o[2]) - float(t["price"])) > 1e-6:
            breaks.append((eid, "QTY_OR_PRICE_DIFF", f"ours {o[1]} @ {o[2]}, broker {t['qty']} @ {t['price']}"))
    all_theirs = set()                     # 这家券商所有对账单里的成交（刚好跨过收盘的成交可能在上一份里）
    for f in glob.glob(os.path.join(STATEMENT_DIR, f"{broker}_*.csv")):
        with open(f) as fh:
            all_theirs.update(r["exec_id"] for r in csv.DictReader(fh))
    for eid, o in ours.items():
        if eid not in all_theirs:
            breaks.append((eid, "MISSING_AT_BROKER", f"child {o[3]} {o[1]} @ {o[2]}"))
    for eid, kind, detail in breaks:
        cur.execute("INSERT INTO recon_breaks (trade_date, broker, exec_id, kind, detail) VALUES (%s, %s, %s, %s, %s) "
                    "ON CONFLICT DO NOTHING", (suffix, broker, eid, kind, detail))
    return len(theirs), len(ours), breaks, start, end


def table(cur, sql, params=()):
    cur.execute(sql, params)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    if not rows:
        return "（无）\n"
    out = "| " + " | ".join(cols) + " |\n|" + "---|" * len(cols) + "\n"
    for r in rows:
        out += "| " + " | ".join("" if v is None else str(v) for v in r) + " |\n"
    return out


def generate(suffix):
    files = statement_files()[suffix]
    db = connect("report")
    cur = db.cursor()
    md, breaks = [f"# TradeWatch 复盘报告 {suffix}\n"], {}
    rec_md, start, end = [], None, None
    for b in sorted(files):
        n_theirs, n_ours, bk, s, e = recon(cur, b, files[b], suffix)
        start, end = min(start or s, s), max(end or e, e)
        breaks[b] = len(bk)
        rec_md.append(f"**{b}**：对账单 {n_theirs} 笔，我们记了 {n_ours} 笔，对不上 {len(bk)} 处\n")
        for eid, kind, detail in bk[:20]:
            rec_md.append(f"- `{eid}` {kind}：{detail}")
        rec_md.append("")
    w = (start, end)
    md.append(f"时间范围：{start:%Y-%m-%d %H:%M} → {end:%Y-%m-%d %H:%M} UTC，生成于 {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC\n")
    md.append("## 1. 订单\n")
    md.append(table(cur, """
        SELECT pm_id AS "PM", COUNT(*) AS "母单", COUNT(*) FILTER (WHERE status = 'DONE') AS "完成",
               COUNT(*) FILTER (WHERE status = 'REJECTED') AS "风控拒绝",
               COUNT(*) FILTER (WHERE status = 'CANCELLED') AS "取消",
               COUNT(*) FILTER (WHERE status = 'WORKING') AS "还在执行",
               ROUND(100.0 * SUM(filled_qty) FILTER (WHERE status <> 'REJECTED')
                     / NULLIF(SUM(qty) FILTER (WHERE status <> 'REJECTED'), 0), 1) AS "成交率%%"
        FROM parent_orders WHERE created_at BETWEEN %s AND %s GROUP BY pm_id ORDER BY pm_id""", w))
    md.append("## 2. 风控拒单原因\n")
    md.append(table(cur, """
        SELECT pm_id AS "PM", split_part(reject_reason, ' ', 1) AS "原因", COUNT(*) AS "次数"
        FROM parent_orders WHERE status = 'REJECTED' AND created_at BETWEEN %s AND %s
        GROUP BY 1, 2 ORDER BY 3 DESC""", w))
    md.append("## 3. 执行质量\n")
    md.append(table(cur, """
        SELECT pm_id AS "PM", COUNT(*) AS "完成母单",
               ROUND(AVG(CASE side WHEN 'BUY' THEN 1 ELSE -1 END * (avg_price - ref_price) / ref_price * 1e4), 1)
                 AS "平均滑点 bps"
        FROM parent_orders WHERE status = 'DONE' AND created_at BETWEEN %s AND %s GROUP BY pm_id ORDER BY pm_id""", w))
    md.append(table(cur, """
        SELECT c.broker AS "券商", COUNT(*) AS "子单",
               ROUND(100.0 * COUNT(*) FILTER (WHERE c.status = 'FILLED') / COUNT(*), 1) AS "全部成交%%",
               COUNT(*) FILTER (WHERE c.status = 'REJECTED') AS "券商拒单",
               ROUND(AVG(EXTRACT(EPOCH FROM a.ts - s.ts))::numeric, 1) AS "平均确认时间(秒)"
        FROM child_orders c
        LEFT JOIN LATERAL (SELECT MIN(ts) AS ts FROM order_events WHERE child_id = c.child_id AND event_type = 'CHILD_SENT') s ON true
        LEFT JOIN LATERAL (SELECT MIN(ts) AS ts FROM order_events WHERE child_id = c.child_id AND event_type = 'CHILD_ACK') a ON true
        WHERE c.created_at BETWEEN %s AND %s GROUP BY c.broker ORDER BY c.broker""", w))
    md.append("## 4. 持仓和盈亏（现在）\n")
    md.append(table(cur, """
        SELECT e.pm_id AS "PM", ROUND(e.gross) AS "总敞口", p.max_gross AS "上限", ROUND(e.net) AS "净敞口",
               p.max_net AS "上限 ", ROUND(e.pnl) AS "盈亏"
        FROM pm_exposure e JOIN pms p USING (pm_id) ORDER BY 1"""))
    md.append("## 5. 对账（券商对账单 vs 我们的 executions）\n")
    md.append("\n".join(rec_md) + "\n")
    md.append("## 6. 这段时间的告警\n")
    md.append(table(cur, """
        SELECT to_char(fired_at, 'MM-DD HH24:MI') AS "时间", severity AS "级别", rule AS "规则", detail AS "内容",
               CASE WHEN resolved_at IS NULL THEN '未解决' ELSE to_char(resolved_at, 'HH24:MI') END AS "解决"
        FROM alerts WHERE fired_at BETWEEN %s AND %s ORDER BY fired_at""", w))
    os.makedirs(REPORT_DIR, exist_ok=True)
    path = os.path.join(REPORT_DIR, f"report_{suffix}.md")
    with open(path, "w") as fh:
        fh.write("\n".join(md))
    db.close()
    return path, breaks
