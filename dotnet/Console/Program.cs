// 值班控制台的后端（tw-console）：ASP.NET Core
// - GET  /api/...  从数据库读数据给网页显示（状态、告警、订单、一张单的完整经历）
// - POST /api/...  值班操作：暂停 / 恢复 PM、撤母单 → 发到 Kafka 的 ops.commands（和 ./tw 一样，由 OMS / EMS 执行）
// - 其他地址：返回 Angular 打包好的网页（wwwroot/）
using Npgsql;
using TradeWatch;

const string Service = "console";

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();                                 // 不要 ASP.NET 自带的日志格式，统一用我们的 JSON 日志
var app = builder.Build();
app.UseDefaultFiles();                                            // 打开 / 时返回 index.html
app.UseStaticFiles();                                             // 网页的 js / css 文件（在 wwwroot/ 里）

Kafka.EnsureTopics(Service);
var bus = new Bus(Service);
var dataSource = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder
{
    Host = Util.Env("DB_HOST", "postgres"),
    Port = int.Parse(Util.Env("DB_PORT", "5432")),
    Database = "tradewatch",
    Username = "tradewatch",
    Password = Util.Env("POSTGRES_PASSWORD", ""),
}.ConnectionString);

List<Dictionary<string, object?>> Q(string sql, params (string, object?)[] args)
{
    using var conn = dataSource.OpenConnection();
    return Db.Query(conn, sql, args);
}

// ============================================================
// 读数据
// ============================================================
app.MapGet("/api/status", () => new
{
    server_time = DateTime.UtcNow,
    heartbeats = Q(@"SELECT service, ROUND(EXTRACT(EPOCH FROM now() - last_seen)) AS age_s
                     FROM service_heartbeats ORDER BY service"),
    sessions = Q(@"SELECT broker, broker_name, status, ROUND(EXTRACT(EPOCH FROM now() - last_heartbeat)) AS heartbeat_age_s,
                          ROUND(EXTRACT(EPOCH FROM now() - last_msg_in)) AS last_msg_age_s
                   FROM fix_sessions ORDER BY broker"),
    prices = Q(@"SELECT symbol, price, ROUND(EXTRACT(EPOCH FROM now() - ts)) AS age_s FROM latest_prices ORDER BY symbol"),
    lag = Q(@"SELECT group_id, SUM(lag) AS lag FROM kafka_lag WHERE ts = (SELECT MAX(ts) FROM kafka_lag)
              GROUP BY group_id ORDER BY 2 DESC, 1"),
    pms = Q(@"SELECT p.pm_id, p.name, COALESCE(c.paused, false) AS paused, c.reason, c.changed_by, c.changed_at,
                     (SELECT COUNT(*) FROM parent_orders o WHERE o.pm_id = p.pm_id AND o.status = 'WORKING') AS working,
                     ROUND(e.gross) AS gross, p.max_gross, ROUND(e.net) AS net, p.max_net, ROUND(e.pnl) AS pnl
              FROM pms p LEFT JOIN control c USING (pm_id) LEFT JOIN pm_exposure e USING (pm_id) ORDER BY 1"),
    locates = Q("SELECT symbol, available FROM locates ORDER BY symbol"),
});

app.MapGet("/api/alerts", (bool? all) => Q(
    @"SELECT alert_id, severity, rule, key, pm_id, detail, fired_at, resolved_at, note FROM alerts " +
    (all == true ? "" : "WHERE resolved_at IS NULL ") + "ORDER BY resolved_at IS NOT NULL, severity, fired_at DESC LIMIT 100"));

app.MapGet("/api/parents/working", () => Q(
    @"SELECT p.parent_id, p.pm_id, p.symbol, p.side, p.qty, p.filled_qty, p.avg_price, p.ref_price, p.created_at,
             (SELECT COUNT(*) FROM child_orders c WHERE c.parent_id = p.parent_id) AS children,
             (SELECT MAX(created_at) FROM child_orders c WHERE c.parent_id = p.parent_id) AS last_child_at
      FROM parent_orders p WHERE p.status = 'WORKING' ORDER BY p.created_at"));

app.MapGet("/api/parents/rejected", () => Q(
    @"SELECT parent_id, pm_id, symbol, side, qty, reject_reason, updated_at FROM parent_orders
      WHERE status = 'REJECTED' ORDER BY updated_at DESC LIMIT 30"));

app.MapGet("/api/events", () => Q(
    @"SELECT ts, service, event_type, parent_id, child_id, detail FROM order_events ORDER BY event_id DESC LIMIT 60"));

app.MapGet("/api/trace/{id}", (string id) =>
{
    var pid = id;
    if (id.StartsWith("C"))
    {
        var r = Q("SELECT parent_id FROM child_orders WHERE child_id = @id", ("id", id));
        if (r.Count == 0) return Results.NotFound(new { error = $"找不到子单 {id}" });
        pid = (string)r[0]["parent_id"]!;
    }
    var parent = Q(@"SELECT parent_id, pm_id, symbol, side, qty, filled_qty, avg_price, ref_price, status, reject_reason,
                            created_at, updated_at FROM parent_orders WHERE parent_id = @p", ("p", pid));
    if (parent.Count == 0)
        return Results.NotFound(new { error = $"找不到母单 {pid}（刚下的单可能 order-store 还没写进数据库）" });
    return Results.Ok(new
    {
        parent = parent[0],
        children = Q(@"SELECT child_id, broker, qty, limit_price, filled_qty, avg_price, status, reason, created_at
                       FROM child_orders WHERE parent_id = @p ORDER BY created_at", ("p", pid)),
        events = Q(@"SELECT ts, service, event_type, child_id, detail FROM order_events
                     WHERE parent_id = @p ORDER BY ts, event_id", ("p", pid)),
        fix = Q(@"SELECT f.ts, f.direction, f.broker, f.msg_type, f.cl_ord_id, f.raw FROM fix_messages f
                  JOIN child_orders c ON c.child_id = f.cl_ord_id WHERE c.parent_id = @p ORDER BY f.ts, f.id", ("p", pid)),
    });
});

// ============================================================
// 值班操作：发到 ops.commands，由 OMS / EMS 执行（和 ./tw pause / resume / cancel 完全一样）
// ============================================================
IResult Command(string cmd, string target, OpsRequest req)
{
    if (string.IsNullOrWhiteSpace(req.Reason))
        return Results.BadRequest(new { error = "请写下原因（谁让你做的、为什么）" });
    var by = string.IsNullOrWhiteSpace(req.By) ? "console" : req.By!;
    var msg = new Dictionary<string, object?> { ["cmd"] = cmd, ["by"] = by, ["reason"] = req.Reason, ["ts"] = Util.Iso(Util.Now()) };
    if (cmd == "cancel") msg["parent_id"] = target; else msg["pm_id"] = target;
    bus.Send("ops.commands", target, msg);
    bus.Flush();
    Log.Warn(Service, "ops_command_sent", new() { ["cmd"] = cmd, ["target"] = target, ["by"] = by, ["reason"] = req.Reason });
    return Results.Ok(new { sent = true, cmd, target });
}

app.MapPost("/api/pms/{pm}/pause", (string pm, OpsRequest req) =>
    Config.Pms.ContainsKey(pm) ? Command("pause", pm, req) : Results.NotFound(new { error = $"没有这个 PM：{pm}" }));
app.MapPost("/api/pms/{pm}/resume", (string pm, OpsRequest req) =>
    Config.Pms.ContainsKey(pm) ? Command("resume", pm, req) : Results.NotFound(new { error = $"没有这个 PM：{pm}" }));
app.MapPost("/api/parents/{id}/cancel", (string id, OpsRequest req) => Command("cancel", id, req));

app.MapPost("/api/alerts/{id:long}/resolve", (long id, OpsRequest req) =>
{
    if (string.IsNullOrWhiteSpace(req.Note))
        return Results.BadRequest(new { error = "请写下处理说明" });
    using var conn = dataSource.OpenConnection();
    var n = Db.Execute(conn, "UPDATE alerts SET resolved_at = COALESCE(resolved_at, now()), note = @note WHERE alert_id = @id",
                       ("note", $"{(string.IsNullOrWhiteSpace(req.By) ? "console" : req.By)}: {req.Note}"), ("id", id));
    return n > 0 ? Results.Ok(new { resolved = id }) : Results.NotFound(new { error = $"没有编号 {id} 的告警" });
});

// ============================================================
// 网页（Angular 打包好的文件）
// ============================================================
app.MapFallbackToFile("index.html");

Log.Info(Service, "start", new() { ["runtime"] = ".NET " + Environment.Version, ["urls"] = Util.Env("ASPNETCORE_HTTP_PORTS", "8080") });
app.Run();

record OpsRequest(string? Reason, string? By, string? Note);
