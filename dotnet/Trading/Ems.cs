// EMS 服务（tw-ems，C# 版）：执行算法 TWAP
// 读：orders.parent（通过风控的母单）、md.bars（行情 + 时钟）、exec.reports（FIX 网关翻译好的券商回报）、ops.commands（撤母单）
// 写：exec.orders（要发的子单 / 撤单 → FIX 网关）、orders.events（CHILD_SENT、CHILD_ACK、FILL、PARENT_DONE……）
//
// 每根 K 线：给每个母单发下一个子单（母单拆成 8 份，限价 = 现价 ± 5 个基点）；
// 子单 2 根 K 线还没成交完就撤掉，剩下的下一根 K 线重新下。和某家券商的 FIX 连接断了，就先不往那家发。
using System.Globalization;
using System.Text.Json.Nodes;

namespace TradeWatch;

public sealed class Ems
{
    const string Service = "ems";

    sealed class Parent
    {
        public required string Id { get; init; }
        public required string Pm { get; init; }
        public required string Symbol { get; init; }
        public required string Side { get; init; }
        public int Qty, Filled, Slices;
        public double Notional;
        public string? OpenChild;
    }

    sealed class Child
    {
        public required string Id { get; init; }
        public required string Parent { get; init; }
        public required string Pm { get; init; }
        public required string Broker { get; init; }
        public required string Symbol { get; init; }
        public required string Side { get; init; }
        public int Qty, Filled, SentBar;
        public double Limit, Notional;
        public string Status = "PENDING_NEW";
        public string? StatusBeforeCancel;
        public bool CancelSent;
    }

    readonly Bus bus = new(Service);
    readonly ChaosClient chaos = new();
    readonly Dictionary<string, double> lastPrice = new();
    readonly Dictionary<string, Parent> parents = new();           // 还在执行的母单
    readonly Dictionary<string, Child> children = new();           // 还没结束的子单（cl_ord_id → 子单）
    readonly Dictionary<string, bool> sessionUp = Config.Brokers.Keys.ToDictionary(b => b, _ => true);
    readonly Dictionary<string, string> recentlyDone = new();      // 刚结束的子单：券商晚到的回报不算"不认识的单"
    readonly Queue<string> recentlyDoneOrder = new();
    int bar;

    static double? Avg(int filled, double notional) => filled > 0 ? Math.Round(notional / filled, 4) : null;

    static Dictionary<string, object?> Snap(Child c, string? reason = null) => new()
    {
        ["status"] = c.Status, ["filled_qty"] = c.Filled, ["avg_price"] = Avg(c.Filled, c.Notional), ["reason"] = reason,
    };

    void FinishChild(Child c)
    {
        children.Remove(c.Id);
        recentlyDone[c.Id] = c.Status;
        recentlyDoneOrder.Enqueue(c.Id);
        while (recentlyDoneOrder.Count > 5000) recentlyDone.Remove(recentlyDoneOrder.Dequeue());
        if (parents.TryGetValue(c.Parent, out var p) && p.OpenChild == c.Id) p.OpenChild = null;
    }

    // ============================================================
    // 券商回报（FIX 网关已经把 FIX 原文翻译成 tag → 值）
    // ============================================================
    void OnReport(JsonNode r)
    {
        var broker = Util.Str(r["broker"])!;
        if (Util.Str(r["type"]) == "session")
        {
            sessionUp[broker] = Util.Str(r["status"]) == "UP";
            return;
        }
        var msgType = Util.Str(r["msg_type"]);
        var f = r["fields"]!;
        string? Tag(string t) => Util.Str(f[t]);
        var clOrdId = msgType == "9" ? Tag("41") : Tag("11");
        if (clOrdId is null) return;
        if (!children.TryGetValue(clOrdId, out var c))
        {
            if (recentlyDone.TryGetValue(clOrdId, out var ourStatus))
                Log.Info(Service, "late_report_for_done_child", new()
                {
                    ["broker"] = broker, ["cl_ord_id"] = clOrdId, ["msg_type"] = msgType, ["exec_type"] = Tag("150"),
                    ["our_status"] = ourStatus, ["text"] = Tag("58"),
                });
            else
                Log.Warn(Service, "unknown_cl_ord_id", new() { ["broker"] = broker, ["cl_ord_id"] = clOrdId, ["msg_type"] = msgType });
            return;
        }
        parents.TryGetValue(c.Parent, out var p);

        if (msgType == "9")                                      // 撤单被拒
        {
            if (Tag("39") == "2")                                // 券商说这单早就全部成交了
            {
                c.Status = "FILLED";
                bus.Emit("ems", "CANCEL_REJECTED", c.Parent, c.Id, Tag("58"), "WARN",
                         child: Snap(c, $"cancel rejected {Tag("58")}: broker reports order filled"));
                FinishChild(c);                                  // 母单按我们自己记到的成交量继续执行
            }
            else if (Tag("58") == "UNKNOWN_ORDER")               // 券商根本不知道这张单（比如券商那边重启过）
            {
                c.Status = "CANCELLED";
                bus.Emit("ems", "CANCEL_REJECTED", c.Parent, c.Id, Tag("58"), "WARN",
                         child: Snap(c, "cancel rejected: broker does not know this order"));
                FinishChild(c);
            }
            else
            {
                c.Status = c.StatusBeforeCancel ?? c.Status;
                c.CancelSent = false;
                bus.Emit("ems", "CANCEL_REJECTED", c.Parent, c.Id, Tag("58"), "WARN", child: Snap(c, $"cancel rejected: {Tag("58")}"));
            }
            return;
        }

        switch (Tag("150"))
        {
            case "0":                                            // 券商确认收到
                c.Status = "NEW";
                bus.Emit("ems", "CHILD_ACK", c.Parent, c.Id, $"broker order {Tag("37")}", child: Snap(c));
                break;
            case "8":                                            // 券商拒单
                c.Status = "REJECTED";
                bus.Emit("ems", "CHILD_REJECTED", c.Parent, c.Id, Tag("58"), "WARN", child: Snap(c, Tag("58")));
                FinishChild(c);
                break;
            case "4":                                            // 撤单成功
                c.Status = "CANCELLED";
                bus.Emit("ems", "CHILD_CANCELLED", c.Parent, c.Id, $"leaves {Tag("151")}", child: Snap(c));
                FinishChild(c);
                break;
            case "F":                                            // 成交
                var qty = int.Parse(Tag("32")!, CultureInfo.InvariantCulture);
                var px = double.Parse(Tag("31")!, CultureInfo.InvariantCulture);
                var cum = int.Parse(Tag("14")!, CultureInfo.InvariantCulture);
                if (cum != c.Filled + qty)                       // 券商的累计数和我们对不上
                    Log.Warn(Service, "cum_qty_mismatch", new()
                    {
                        ["cl_ord_id"] = c.Id, ["broker"] = broker, ["ours"] = c.Filled + qty, ["broker_cum"] = cum,
                    });
                c.Filled += qty;
                c.Notional += qty * px;
                c.Status = Tag("39") == "2" ? "FILLED" : "PARTIALLY_FILLED";
                Dictionary<string, object?>? parentSnap = null;
                if (p != null)
                {
                    p.Filled += qty;
                    p.Notional += qty * px;
                    parentSnap = new()
                    {
                        ["status"] = p.Filled >= p.Qty ? "DONE" : "WORKING", ["filled_qty"] = p.Filled,
                        ["avg_price"] = Math.Round(p.Notional / p.Filled, 4),
                    };
                }
                bus.Emit("ems", "FILL", c.Parent, c.Id, $"{qty} @ {px}", child: Snap(c), parent: parentSnap,
                         execution: new()
                         {
                             ["exec_id"] = Tag("17"), ["child_id"] = c.Id, ["parent_id"] = c.Parent, ["pm_id"] = c.Pm,
                             ["broker"] = broker, ["symbol"] = c.Symbol, ["side"] = c.Side, ["qty"] = qty, ["price"] = px,
                         },
                         logFields: new() { ["qty"] = qty, ["price"] = px });
                if (p != null && p.Filled >= p.Qty)
                {
                    bus.Emit("ems", "PARENT_DONE", p.Id, detail: $"{p.Filled} @ {(p.Notional / p.Filled).ToString("F4", CultureInfo.InvariantCulture)}");
                    parents.Remove(p.Id);
                }
                if (c.Status == "FILLED") FinishChild(c);
                break;
        }
    }

    // ============================================================
    // TWAP
    // ============================================================
    void SendCancel(Child c)
    {
        c.CancelSent = true;
        c.StatusBeforeCancel = c.Status;
        c.Status = "PENDING_CANCEL";
        bus.Send("exec.orders", c.Id, new Dictionary<string, object?>
        {
            ["action"] = "cancel", ["broker"] = c.Broker, ["cl_ord_id"] = Util.NewId("CX", 8), ["orig_cl_ord_id"] = c.Id,
            ["symbol"] = c.Symbol, ["side"] = c.Side, ["qty"] = c.Qty,
        });
    }

    void EmsStep()
    {
        foreach (var p in parents.Values.ToList())
        {
            if (chaos.Active("algo_stall", pm: p.Pm) != null) continue;
            var b = Config.Routing[p.Symbol];
            if (!sessionUp[b]) continue;                         // 连接断了，先不发
            if (p.OpenChild != null && children.TryGetValue(p.OpenChild, out var open))
            {
                if (open.CancelSent) continue;
                if (bar - open.SentBar >= Config.ChildTimeoutBars || bar < open.SentBar)   // 行情服务重启后 bar 会从 0 重新数
                {
                    SendCancel(open);
                    bus.Emit("ems", "CHILD_CANCEL_SENT", p.Id, open.Id, $"not filled within {Config.ChildTimeoutBars} bars", child: Snap(open));
                }
                continue;
            }
            var remaining = p.Qty - p.Filled;
            if (remaining <= 0 || !lastPrice.TryGetValue(p.Symbol, out var px)) continue;
            var slicesLeft = Math.Max(1, Config.TwapSlices - p.Slices);
            var qty = Math.Min(remaining, Math.Max(Config.Lot, (int)Math.Ceiling((double)remaining / slicesLeft / Config.Lot) * Config.Lot));
            var limit = Math.Round(p.Side == "BUY" ? px * (1 + Config.ChildLimitBps / 1e4) : px * (1 - Config.ChildLimitBps / 1e4), 2);
            var cid = Util.NewId("C", 9);
            var c = new Child
            {
                Id = cid, Parent = p.Id, Pm = p.Pm, Broker = b, Symbol = p.Symbol, Side = p.Side, Qty = qty, Limit = limit, SentBar = bar,
            };
            children[cid] = c;
            p.OpenChild = cid;
            p.Slices++;
            bus.Emit("ems", "CHILD_SENT", p.Id, cid, $"slice {p.Slices}/{Config.TwapSlices} {qty} @ {limit} to {b}",
                     childNew: new()
                     {
                         ["pm_id"] = p.Pm, ["broker"] = b, ["symbol"] = p.Symbol, ["side"] = p.Side, ["qty"] = qty, ["limit_price"] = limit,
                     });
            bus.Send("exec.orders", cid, new Dictionary<string, object?>
            {
                ["action"] = "new", ["broker"] = b, ["cl_ord_id"] = cid, ["pm_id"] = p.Pm, ["symbol"] = p.Symbol,
                ["side"] = p.Side, ["qty"] = qty, ["limit"] = limit,
            });
        }
    }

    void OnParent(JsonNode m)
    {
        var id = Util.Str(m["parent_id"])!;
        parents[id] = new Parent
        {
            Id = id, Pm = Util.Str(m["pm_id"])!, Symbol = Util.Str(m["symbol"])!, Side = Util.Str(m["side"])!, Qty = Util.Int(m["qty"]),
        };
    }

    /// <summary>值班的人撤母单（tw cancel 或者控制台网页）：把它和它的子单一起撤掉。</summary>
    void OnCommand(JsonNode cmd)
    {
        if (Util.Str(cmd["cmd"]) != "cancel" || Util.Str(cmd["parent_id"]) is not { } pid) return;
        var found = parents.Remove(pid);
        foreach (var c in children.Values.Where(c => c.Parent == pid).ToList())
            if (!c.CancelSent)
            {
                SendCancel(c);
                bus.Emit("ems", "CHILD_CANCEL_SENT", pid, c.Id, "manual cancel by ops", child: Snap(c));
            }
        var reason = $"manual cancel by {Util.Str(cmd["by"]) ?? "ops"}: {Util.Str(cmd["reason"]) ?? ""}";
        bus.Emit("ems", "PARENT_CANCELLED", pid, detail: found ? reason : "not working (already gone)", level: "WARN",
                 parent: found ? new Dictionary<string, object?> { ["status"] = "CANCELLED", ["reject_reason"] = reason } : null);
    }

    /// <summary>重启后：10 分钟内还在执行的母单接着做；更早的取消。券商那边还挂着的子单全部撤掉。</summary>
    void Startup()
    {
        using var db = Db.Connect(Service);
        foreach (var r in Db.Query(db,
                     "SELECT parent_id, pm_id, symbol, side, qty, filled_qty, COALESCE(avg_price, 0) AS avg_price, " +
                     "updated_at > now() - interval '10 minutes' AS recent, " +
                     "(SELECT COUNT(*) FROM child_orders c WHERE c.parent_id = p.parent_id) AS slices " +
                     "FROM parent_orders p WHERE status = 'WORKING'"))
        {
            var pid = (string)r["parent_id"]!;
            if ((bool)r["recent"]!)
            {
                var filled = Convert.ToInt32(r["filled_qty"]);
                parents[pid] = new Parent
                {
                    Id = pid, Pm = (string)r["pm_id"]!, Symbol = (string)r["symbol"]!, Side = (string)r["side"]!,
                    Qty = Convert.ToInt32(r["qty"]), Filled = filled, Notional = filled * Convert.ToDouble(r["avg_price"]),
                    Slices = Convert.ToInt32(r["slices"]),
                };
            }
            else
            {
                bus.Emit("ems", "PARENT_CANCELLED", pid, detail: "stale after restart",
                         parent: new() { ["status"] = "CANCELLED", ["reject_reason"] = "stale after restart" });
            }
        }
        foreach (var r in Db.Query(db,
                     "SELECT child_id, parent_id, pm_id, broker, symbol, side, qty, limit_price, status, filled_qty, " +
                     "COALESCE(avg_price, 0) AS avg_price FROM child_orders " +
                     "WHERE status IN ('PENDING_NEW', 'NEW', 'PARTIALLY_FILLED', 'PENDING_CANCEL')"))
        {
            var filled = Convert.ToInt32(r["filled_qty"]);
            var c = new Child
            {
                Id = (string)r["child_id"]!, Parent = (string)r["parent_id"]!, Pm = (string)r["pm_id"]!, Broker = (string)r["broker"]!,
                Symbol = (string)r["symbol"]!, Side = (string)r["side"]!, Qty = Convert.ToInt32(r["qty"]),
                Limit = Convert.ToDouble(r["limit_price"]), Status = (string)r["status"]!, Filled = filled,
                Notional = filled * Convert.ToDouble(r["avg_price"]),
            };
            children[c.Id] = c;
            if (parents.TryGetValue(c.Parent, out var p)) p.OpenChild = c.Id;
            SendCancel(c);
            bus.Emit("ems", "CHILD_CANCEL_SENT", c.Parent, c.Id, "ems restart: cancel orphaned child", child: Snap(c));
        }
        bus.Flush();
        Log.Info(Service, "start", new()
        {
            ["working_parents"] = parents.Count, ["open_children"] = children.Count, ["runtime"] = ".NET " + Environment.Version,
        });
    }

    public void Run()
    {
        Kafka.EnsureTopics(Service);
        Startup();
        using var consumer = Kafka.Consumer(Service, new[] { "md.bars", "orders.parent", "exec.reports", "ops.commands" }, fromEnd: true);
        while (true)
        {
            var m = Kafka.Poll(consumer, Service);
            if (m is null) continue;
            var (topic, v) = m.Value;
            switch (topic)
            {
                case "md.bars" when Util.Str(v["type"]) == "bar" && !ChaosClient.IsStale(v):
                    bar = Util.Int(v["bar"]);
                    foreach (var (s, px) in v["prices"]!.AsObject()) lastPrice[s] = Util.Num(px);
                    EmsStep();
                    bus.Heartbeat(Service, new() { ["bar"] = bar, ["working_parents"] = parents.Count, ["open_children"] = children.Count });
                    break;
                case "orders.parent":
                    OnParent(v);
                    break;
                case "exec.reports":
                    OnReport(v);
                    break;
                case "ops.commands":
                    OnCommand(v);
                    break;
            }
            bus.Flush();
        }
    }
}
