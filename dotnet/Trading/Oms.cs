// OMS 服务（tw-oms，C# 版）：PM 策略 + 订单管理 + 交易前风控
// 读：md.bars（行情）、orders.events（成交，用来更新持仓）、ops.commands（暂停 / 恢复 PM）
// 写：orders.events（PARENT_NEW、RISK_PASS、RISK_REJECT）、orders.parent（通过风控的母单 → EMS）、ops.state
//
// 每 5 根 K 线：每个 PM 的策略给出目标仓位 → 目标 − 现有持仓 = 母单 → 风控检查 → 通过就交给 EMS。
// 重启后从数据库恢复持仓、PM 开关、借券额度和还在执行的母单。
using System.Text.Json.Nodes;

namespace TradeWatch;

public sealed class Oms
{
    const string Service = "oms";

    record Parent(string Pm, string Symbol, string Side, int Qty) { public int Filled; }

    readonly Bus bus = new(Service);
    readonly ChaosClient chaos = new();
    readonly Dictionary<string, double> lastPrice = new();
    readonly Dictionary<string, List<double>> hist = new();
    readonly Dictionary<(string Pm, string Symbol), int> positions = new();   // 我们记的持仓
    readonly Dictionary<string, Parent> parents = new();                      // 还在执行的母单
    readonly Dictionary<string, string> paused = new();                       // PM → 暂停原因
    readonly Dictionary<string, int> locates = new(Config.DailyLocates);
    readonly HashSet<string> badDeployOn = new();

    int Pos(string pm, string symbol) => positions.TryGetValue((pm, symbol), out var q) ? q : 0;

    // ============================================================
    // 风控
    // ============================================================
    /// <summary>持仓 + 还没执行完的母单 = 如果全部做完会是多少。</summary>
    Dictionary<(string Pm, string Symbol), int> Projected()
    {
        var pos = new Dictionary<(string, string), int>(positions);
        foreach (var p in parents.Values)
        {
            var key = (p.Pm, p.Symbol);
            pos[key] = (pos.TryGetValue(key, out var q) ? q : 0) + (p.Side == "BUY" ? 1 : -1) * Math.Max(0, p.Qty - p.Filled);
        }
        return pos;
    }

    void SetLocates(string symbol, int available)
    {
        locates[symbol] = available;
        bus.Send("ops.state", symbol, new Dictionary<string, object?>
        {
            ["kind"] = "locates", ["symbol"] = symbol, ["available"] = available, ["ts"] = Util.Iso(Util.Now()),
        });
    }

    string? RiskCheck(string pmId, string symbol, string side, int qty, double price)
    {
        var cfg = Config.Pms[pmId];
        if (paused.ContainsKey(pmId)) return "PM_PAUSED";
        if (qty * price > Config.MaxOrderNotional)
            return $"MAX_ORDER_NOTIONAL ({qty * price:N0} > {Config.MaxOrderNotional:N0})";
        var pos = Projected();
        var before = pos.TryGetValue((pmId, symbol), out var b) ? b : 0;
        var after = before + (side == "BUY" ? qty : -qty);
        if (Math.Abs(after * price) > cfg.MaxPosition)
            return $"POSITION_LIMIT ({Math.Abs(after * price):N0} > {cfg.MaxPosition:N0})";
        pos[(pmId, symbol)] = after;
        var mine = pos.Where(kv => kv.Key.Item1 == pmId && lastPrice.ContainsKey(kv.Key.Item2)).ToList();
        var gross = mine.Sum(kv => Math.Abs(kv.Value * lastPrice[kv.Key.Item2]));
        var net = mine.Sum(kv => kv.Value * lastPrice[kv.Key.Item2]);
        if (gross > cfg.MaxGross) return $"GROSS_LIMIT ({gross:N0} > {cfg.MaxGross:N0})";
        if (Math.Abs(net) > cfg.MaxNet) return $"NET_LIMIT ({Math.Abs(net):N0} > {cfg.MaxNet:N0})";
        var newShort = Math.Max(0, -after) - Math.Max(0, -before);
        if (newShort > 0)
        {
            var available = locates.TryGetValue(symbol, out var l) ? l : 0;
            if (newShort > available) return $"NO_LOCATE (need {newShort}, available {available})";
            SetLocates(symbol, available - newShort);
        }
        return null;
    }

    // ============================================================
    // OMS
    // ============================================================
    void CreateParent(string pmId, string symbol, string side, int qty, double price, bool retry = false)
    {
        var pid = Util.NewId("P", 8);
        bus.Emit("oms", "PARENT_NEW", pid, detail: $"{pmId} {side} {qty} {symbol} @ {price}" + (retry ? " (resubmit)" : ""),
                 parentNew: new() { ["pm_id"] = pmId, ["symbol"] = symbol, ["side"] = side, ["qty"] = qty, ["ref_price"] = price });
        var reason = RiskCheck(pmId, symbol, side, qty, price);
        if (reason != null)
        {
            bus.Emit("risk", "RISK_REJECT", pid, detail: reason, level: "WARN",
                     parent: new() { ["status"] = "REJECTED", ["reject_reason"] = reason },
                     logFields: new() { ["pm_id"] = pmId, ["symbol"] = symbol });
            return;
        }
        bus.Emit("risk", "RISK_PASS", pid, parent: new() { ["status"] = "WORKING" });
        parents[pid] = new Parent(pmId, symbol, side, qty);
        bus.Send("orders.parent", pid, new Dictionary<string, object?>
        {
            ["parent_id"] = pid, ["pm_id"] = pmId, ["symbol"] = symbol, ["side"] = side, ["qty"] = qty,
            ["ref_price"] = price, ["ts"] = Util.Iso(Util.Now()),
        });
        if (!retry && chaos.Active("duplicate_parent", pm: pmId) != null)
        {
            Log.Warn("oms", "submit_ack_timeout_resubmitting", new() { ["parent_id"] = pid, ["pm_id"] = pmId });
            CreateParent(pmId, symbol, side, qty, price, retry: true);
        }
    }

    void OmsStep()
    {
        var working = parents.Values.Select(p => (p.Pm, p.Symbol)).ToHashSet();
        foreach (var cfg in Config.Pms.Values)
        {
            if (cfg.Symbols.Any(s => !lastPrice.ContainsKey(s))) continue;
            if (paused.ContainsKey(cfg.Id)) continue;               // 暂停的 PM 不再生成新单（已有的母单 EMS 会继续执行）
            var targets = Strategies.Run(cfg.Strategy, hist, lastPrice, cfg);
            var bad = badDeployOn.Contains(cfg.Id);
            foreach (var (symbol, t) in targets)
            {
                var target = bad ? t * 40 : t;                      // 新版本的 bug：单位算错
                if (working.Contains((cfg.Id, symbol))) continue;
                var diff = target - Pos(cfg.Id, symbol);
                if (Math.Abs(diff) * lastPrice[symbol] < Config.MinOrderNotional) continue;
                CreateParent(cfg.Id, symbol, diff > 0 ? "BUY" : "SELL", Math.Abs(diff), lastPrice[symbol]);
            }
        }
    }

    /// <summary>策略程序的"发版"：新版本上线时会打一条重启日志（真实系统也会有）。</summary>
    void CheckChaos()
    {
        var f = chaos.Active("bad_deploy");
        var want = f != null ? new HashSet<string> { Util.Str(f["pm"])! } : new HashSet<string>();
        foreach (var pm in want.Except(badDeployOn))
            Log.Info($"strategy-{pm}", "strategy_restarted", new()
            {
                ["pm_id"] = pm, ["version"] = "3.7.0", ["detail"] = "deployed by release pipeline",
            });
        badDeployOn.Clear();
        badDeployOn.UnionWith(want);
        var ls = chaos.Active("locate_shortage");
        if (ls != null && Util.Str(ls["symbol"]) is { } sym && locates.TryGetValue(sym, out var n) && n > 0)
            SetLocates(sym, 0);
    }

    // ============================================================
    // 收到的消息
    // ============================================================
    void OnEvent(JsonNode e)
    {
        var type = Util.Str(e["type"]);
        var pid = Util.Str(e["parent_id"]) ?? "";
        if (type == "FILL" && e["execution"] is { } x)
        {
            var key = (Util.Str(x["pm_id"])!, Util.Str(x["symbol"])!);
            var qty = Util.Int(x["qty"]);
            positions[key] = Pos(key.Item1, key.Item2) + (Util.Str(x["side"]) == "BUY" ? qty : -qty);
            if (parents.TryGetValue(pid, out var p)) p.Filled += qty;
        }
        else if (type is "PARENT_DONE" or "PARENT_CANCELLED")
        {
            parents.Remove(pid);
        }
    }

    void OnCommand(JsonNode cmd)
    {
        var c = Util.Str(cmd["cmd"]);
        var pm = Util.Str(cmd["pm_id"]);
        if (c is not ("pause" or "resume") || pm is null || !Config.Pms.ContainsKey(pm)) return;
        var by = Util.Str(cmd["by"]) ?? "?";
        var reason = Util.Str(cmd["reason"]) ?? "";
        if (c == "pause")
        {
            paused[pm] = reason;
            Log.Warn(Service, "pm_paused", new() { ["pm_id"] = pm, ["by"] = by, ["reason"] = reason });
        }
        else
        {
            paused.Remove(pm);
            Log.Info(Service, "pm_resumed", new() { ["pm_id"] = pm, ["by"] = by, ["reason"] = reason });
        }
        bus.Send("ops.state", pm, new Dictionary<string, object?>
        {
            ["kind"] = "control", ["pm_id"] = pm, ["paused"] = c == "pause", ["reason"] = reason,
            ["changed_by"] = by, ["ts"] = Util.Iso(Util.Now()),
        });
    }

    void Startup()
    {
        foreach (var p in Config.Pms.Values)
            bus.Send("ops.state", p.Id, new Dictionary<string, object?>
            {
                ["kind"] = "pm", ["pm_id"] = p.Id, ["name"] = p.Name, ["strategy"] = p.Strategy, ["capital"] = p.Capital,
                ["max_gross"] = p.MaxGross, ["max_net"] = p.MaxNet, ["max_position"] = p.MaxPosition,
            });
        using var db = Db.Connect(Service);
        foreach (var r in Db.Query(db, "SELECT pm_id, symbol, qty FROM positions"))
            positions[((string)r["pm_id"]!, (string)r["symbol"]!)] = Convert.ToInt32(r["qty"]);
        foreach (var r in Db.Query(db, "SELECT pm_id, reason FROM control WHERE paused"))
            paused[(string)r["pm_id"]!] = (string?)r["reason"] ?? "";
        foreach (var r in Db.Query(db, "SELECT symbol, available FROM locates"))
            locates[(string)r["symbol"]!] = Convert.ToInt32(r["available"]);
        // 最近的价格历史：策略要 30～60 根 K 线才能算信号，从数据库读回来，重启后马上就能继续下单
        foreach (var r in Db.Query(db, @"SELECT symbol, price FROM (
                     SELECT symbol, price, ts, row_number() OVER (PARTITION BY symbol ORDER BY ts DESC) AS rn
                     FROM market_data WHERE ts > now() - interval '1 day') t
                 WHERE rn <= 300 ORDER BY symbol, ts"))
        {
            var s = (string)r["symbol"]!;
            if (!hist.TryGetValue(s, out var h)) hist[s] = h = new List<double>();
            h.Add(Convert.ToDouble(r["price"]));
        }
        // 还在执行的母单（10 分钟内还有动静的），EMS 会接着执行；更早的 EMS 会取消
        foreach (var r in Db.Query(db, "SELECT parent_id, pm_id, symbol, side, qty, filled_qty FROM parent_orders " +
                                       "WHERE status = 'WORKING' AND updated_at > now() - interval '10 minutes'"))
            parents[(string)r["parent_id"]!] = new Parent((string)r["pm_id"]!, (string)r["symbol"]!, (string)r["side"]!,
                                                          Convert.ToInt32(r["qty"])) { Filled = Convert.ToInt32(r["filled_qty"]) };
        bus.Flush();
        Log.Info(Service, "start", new()
        {
            ["pms"] = Config.Pms.Keys.ToList(), ["positions"] = positions.Count, ["working_parents"] = parents.Count,
            ["history_bars"] = hist.ToDictionary(kv => kv.Key, kv => kv.Value.Count),
            ["paused"] = paused.Keys.ToList(), ["runtime"] = ".NET " + Environment.Version,
        });
    }

    public void Run()
    {
        Kafka.EnsureTopics(Service);
        Startup();
        using var c = Kafka.Consumer(Service, new[] { "md.bars", "orders.events", "ops.commands" }, fromEnd: true);
        while (true)
        {
            var m = Kafka.Poll(c, Service);
            if (m is null) continue;
            var (topic, v) = m.Value;
            switch (topic)
            {
                case "md.bars" when Util.Str(v["type"]) == "sod":
                    foreach (var (s, n) in Config.DailyLocates) SetLocates(s, n);
                    Log.Info("ops", "start_of_day", new() { ["trade_date"] = Util.Str(v["trade_date"]), ["locates"] = new Dictionary<string, int>(locates) });
                    break;
                case "md.bars" when Util.Str(v["type"]) == "bar" && !ChaosClient.IsStale(v):
                    foreach (var (s, px) in v["prices"]!.AsObject())
                    {
                        var price = Util.Num(px);
                        lastPrice[s] = price;
                        if (!hist.TryGetValue(s, out var h)) hist[s] = h = new List<double>();
                        h.Add(price);
                        if (h.Count > 300) h.RemoveRange(0, h.Count - 300);
                    }
                    CheckChaos();
                    var bar = Util.Int(v["bar"]);
                    if (bar % Config.RebalanceEvery == 0) OmsStep();
                    bus.Heartbeat(Service, new() { ["bar"] = bar, ["working_parents"] = parents.Count });
                    break;
                case "orders.events":
                    OnEvent(v);
                    break;
                case "ops.commands":
                    OnCommand(v);
                    break;
            }
            bus.Flush();
        }
    }
}
