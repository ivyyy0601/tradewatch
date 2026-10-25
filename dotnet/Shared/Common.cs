// 公共代码：日志、时间、JSON、数据库、Kafka（和 Python 版 common.py 做同样的事，消息格式完全一样）
using System.Globalization;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Npgsql;

namespace TradeWatch;

public static class Util
{
    public static DateTimeOffset Now() => DateTimeOffset.UtcNow;

    // 时间统一写成 2026-10-25T18:30:00.123456+00:00（Python 和 PostgreSQL 都能直接读）
    public static string Iso(DateTimeOffset t) =>
        t.ToUniversalTime().ToString("yyyy-MM-dd'T'HH:mm:ss.ffffffzzz", CultureInfo.InvariantCulture);

    public static string NewId(string prefix, int n = 8) =>
        prefix + Guid.NewGuid().ToString("N")[..n].ToUpperInvariant();

    public static string Env(string name, string fallback) =>
        Environment.GetEnvironmentVariable(name) is { Length: > 0 } v ? v : fallback;

    public static readonly JsonSerializerOptions JsonOpts = new()
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,      // 中文不要转成 \uXXXX
    };

    public static string ToJson(object value) => JsonSerializer.Serialize(value, JsonOpts);

    // 从收到的 JSON 里取值（Python 发来的数字可能是 430 也可能是 430.0）
    public static string? Str(JsonNode? n) => n?.GetValueKind() == JsonValueKind.String ? n.GetValue<string>() : n?.ToString();
    public static double Num(JsonNode? n) => n is null ? 0 : n.GetValue<double>();
    public static int Int(JsonNode? n) => (int)Math.Round(Num(n));
}

// ============================================================
// 日志：每行一条 JSON，格式和 Python 服务一样，Loki 才能按 level / service 筛选
// ============================================================
public static class Log
{
    static readonly bool Quiet = Environment.GetEnvironmentVariable("TW_QUIET") is { Length: > 0 };
    static readonly object Lock = new();

    public static void Write(string service, string level, string msg, Dictionary<string, object?>? fields = null)
    {
        if (Quiet && level == "INFO") return;
        var record = new Dictionary<string, object?>
        {
            ["ts"] = Util.Now().ToString("yyyy-MM-dd'T'HH:mm:ss.fffzzz", CultureInfo.InvariantCulture),
            ["level"] = level,
            ["service"] = service,
            ["msg"] = msg,
        };
        if (fields != null)
            foreach (var (k, v) in fields)
                if (v != null) record[k] = v;
        var line = Util.ToJson(record);
        lock (Lock) Console.WriteLine(line);
    }

    public static void Info(string service, string msg, Dictionary<string, object?>? f = null) => Write(service, "INFO", msg, f);
    public static void Warn(string service, string msg, Dictionary<string, object?>? f = null) => Write(service, "WARN", msg, f);
    public static void Error(string service, string msg, Dictionary<string, object?>? f = null) => Write(service, "ERROR", msg, f);
}

// ============================================================
// 数据库
// ============================================================
public static class Db
{
    public static NpgsqlConnection Connect(string service)
    {
        var cs = new NpgsqlConnectionStringBuilder
        {
            Host = Util.Env("DB_HOST", "postgres"),
            Port = int.Parse(Util.Env("DB_PORT", "5432")),
            Database = "tradewatch",
            Username = "tradewatch",
            Password = Util.Env("POSTGRES_PASSWORD", ""),
        }.ConnectionString;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                var conn = new NpgsqlConnection(cs);
                conn.Open();
                Log.Info(service, "db_connected");
                return conn;
            }
            catch (Exception e) when (attempt < 60)
            {
                Log.Warn(service, "db_not_ready", new() { ["attempt"] = attempt, ["error"] = e.Message });
                Thread.Sleep(2000);
            }
        }
    }

    /// <summary>执行一条 SELECT，结果变成"列名 → 值"的列表（Console 直接把它转成 JSON 返回给网页）。</summary>
    public static List<Dictionary<string, object?>> Query(NpgsqlConnection conn, string sql, params (string, object?)[] args)
    {
        using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in args)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        using var r = cmd.ExecuteReader();
        var rows = new List<Dictionary<string, object?>>();
        while (r.Read())
        {
            var row = new Dictionary<string, object?>();
            for (var i = 0; i < r.FieldCount; i++)
                row[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
            rows.Add(row);
        }
        return rows;
    }

    public static int Execute(NpgsqlConnection conn, string sql, params (string, object?)[] args)
    {
        using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var (name, value) in args)
            cmd.Parameters.AddWithValue(name, value ?? DBNull.Value);
        return cmd.ExecuteNonQuery();
    }
}

// ============================================================
// Kafka
// ============================================================
public static class Kafka
{
    public static string Bootstrap => Util.Env("KAFKA_BOOTSTRAP", "kafka:9092");

    // 和 Python 版 common.py 里的 TOPICS 一样
    public static string[] Topics => new[]
    {
        "md.exchange", "md.bars", "orders.events", "orders.parent", "exec.orders", "exec.reports",
    }.Concat(Config.Brokers.Keys.Select(b => $"fix.to.{b}"))
     .Concat(Config.Brokers.Keys.Select(b => $"fix.from.{b}"))
     .Concat(new[] { "fix.log", "ops.commands", "ops.state" }).ToArray();

    /// <summary>等 Kafka 启动好，并确保所有 topic 都已经建好。</summary>
    public static void EnsureTopics(string service)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = Bootstrap }).Build();
        HashSet<string> existing;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                existing = admin.GetMetadata(TimeSpan.FromSeconds(5)).Topics.Select(t => t.Topic).ToHashSet();
                break;
            }
            catch (Exception e) when (attempt < 90)
            {
                Log.Warn(service, "kafka_not_ready", new() { ["attempt"] = attempt, ["error"] = e.Message });
                Thread.Sleep(2000);
            }
        }
        var missing = Topics.Where(t => !existing.Contains(t))
                            .Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 })
                            .ToList();
        if (missing.Count > 0)
        {
            try
            {
                admin.CreateTopicsAsync(missing).GetAwaiter().GetResult();
            }
            catch (CreateTopicsException e) when (e.Results.All(r => r.Error.Code is ErrorCode.NoError or ErrorCode.TopicAlreadyExists))
            {
                // 别的服务已经建好了
            }
        }
        Log.Info(service, "kafka_connected", new() { ["bootstrap"] = Bootstrap });
    }

    /// <summary>订阅 topic。fromEnd = true：每次启动都从最新的消息开始读（OMS、EMS 重启后从数据库恢复状态，不重放旧消息）。</summary>
    public static IConsumer<string, string> Consumer(string service, IEnumerable<string> topics, bool fromEnd, string? group = null)
    {
        var conf = new ConsumerConfig
        {
            BootstrapServers = Bootstrap,
            GroupId = group ?? service,
            ClientId = service,
            AutoOffsetReset = fromEnd ? AutoOffsetReset.Latest : AutoOffsetReset.Earliest,
            EnableAutoCommit = true,
        };
        var builder = new ConsumerBuilder<string, string>(conf)
            .SetErrorHandler((_, e) => Log.Warn(service, "kafka_error", new() { ["error"] = e.Reason }));
        if (fromEnd)
            builder.SetPartitionsAssignedHandler((_, parts) =>
            {
                return parts.Select(p => new TopicPartitionOffset(p, Offset.End));   // 分到的每个分区都从最新的位置开始读
            });
        var c = builder.Build();
        c.Subscribe(topics);
        return c;
    }

    /// <summary>读一条消息：返回 (topic, JSON)；没有消息返回 null。</summary>
    public static (string Topic, JsonNode Value)? Poll(IConsumer<string, string> c, string service, int timeoutMs = 1000)
    {
        ConsumeResult<string, string>? m;
        try
        {
            m = c.Consume(TimeSpan.FromMilliseconds(timeoutMs));
        }
        catch (ConsumeException e)
        {
            Log.Warn(service, "kafka_error", new() { ["error"] = e.Error.Reason });
            return null;
        }
        if (m?.Message?.Value is not { } raw) return null;
        try
        {
            return JsonNode.Parse(raw) is { } v ? (m.Topic, v) : null;
        }
        catch (JsonException)
        {
            Log.Error(service, "bad_message", new() { ["topic"] = m.Topic, ["offset"] = m.Offset.Value, ["raw"] = raw[..Math.Min(200, raw.Length)] });
            return null;
        }
    }
}

/// <summary>往 Kafka 发消息。</summary>
public sealed class Bus : IDisposable
{
    readonly IProducer<string, string> p;

    public Bus(string service)
    {
        p = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = Kafka.Bootstrap,
            ClientId = service,
            LingerMs = 5,
            EnableIdempotence = true,
        }).Build();
    }

    public void Send(string topic, string? key, object value)
    {
        var data = value as string ?? Util.ToJson(value);
        while (true)
        {
            try
            {
                p.Produce(topic, new Message<string, string> { Key = key!, Value = data });
                return;
            }
            catch (ProduceException<string, string> e) when (e.Error.Code == ErrorCode.Local_QueueFull)
            {
                p.Poll(TimeSpan.FromMilliseconds(500));                // 本地发送队列满了，等一下
            }
        }
    }

    public void Flush() => p.Flush(TimeSpan.FromSeconds(10));

    public void Heartbeat(string service, Dictionary<string, object?> detail) =>
        Send("ops.state", service, new Dictionary<string, object?>
        {
            ["kind"] = "heartbeat", ["service"] = service, ["ts"] = Util.Iso(Util.Now()), ["detail"] = Util.ToJson(detail),
        });

    /// <summary>订单发生了一件事：发到 orders.events（order-store 会存进数据库），同时打一行日志。</summary>
    public void Emit(string service, string type, string parentId, string? childId = null, string? detail = null,
                     string level = "INFO", Dictionary<string, object?>? parentNew = null, Dictionary<string, object?>? parent = null,
                     Dictionary<string, object?>? childNew = null, Dictionary<string, object?>? child = null,
                     Dictionary<string, object?>? execution = null, Dictionary<string, object?>? logFields = null)
    {
        var msg = new Dictionary<string, object?>
        {
            ["eid"] = Guid.NewGuid().ToString("N"), ["ts"] = Util.Iso(Util.Now()), ["service"] = service, ["type"] = type,
            ["parent_id"] = parentId, ["child_id"] = childId, ["detail"] = detail,
        };
        if (parentNew != null) msg["parent_new"] = parentNew;
        if (parent != null) msg["parent"] = parent;
        if (childNew != null) msg["child_new"] = childNew;
        if (child != null) msg["child"] = child;
        if (execution != null) msg["execution"] = execution;
        Send("orders.events", parentId, msg);
        var f = new Dictionary<string, object?> { ["parent_id"] = parentId, ["child_id"] = childId, ["detail"] = detail };
        if (logFields != null) foreach (var (k, v) in logFields) f[k] = v;
        Log.Write(service, level, type, f);
    }

    public void Dispose()
    {
        p.Flush(TimeSpan.FromSeconds(5));
        p.Dispose();
    }
}

/// <summary>"现实世界"现在有没有某种意外正在发生（读 chaos 服务写的 data/chaos/current.json）。</summary>
public sealed class ChaosClient
{
    readonly string path = Util.Env("CHAOS_STATE", "data/chaos/current.json");
    JsonNode? cache;
    DateTime readAt = DateTime.MinValue;

    public JsonNode? Current()
    {
        if ((DateTime.UtcNow - readAt).TotalSeconds > 1)
        {
            try { cache = JsonNode.Parse(File.ReadAllText(path)); }
            catch { cache = null; }
            readAt = DateTime.UtcNow;
        }
        return cache;
    }

    public JsonNode? Active(string kind, string? pm = null, string? broker = null, string? symbol = null)
    {
        var f = Current();
        if (f is null || Util.Str(f["kind"]) != kind) return null;
        if (pm != null && Util.Str(f["pm"]) != pm) return null;
        if (broker != null && Util.Str(f["broker"]) != broker) return null;
        if (symbol != null && Util.Str(f["symbol"]) != symbol) return null;
        return f;
    }

    /// <summary>行情消息太旧就跳过（服务重启或者积压后，不要拿旧价格做决定）。</summary>
    public static bool IsStale(JsonNode msg, double maxAgeSeconds = 150) =>
        DateTimeOffset.TryParse(Util.Str(msg["ts"]), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var t)
        && (Util.Now() - t).TotalSeconds > maxAgeSeconds;
}
