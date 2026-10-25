// 交易系统的配置：PM、券商、路由、风控参数（参考数据）
// ⚠️ 和 Python 版的 config.py 必须保持一致：改了一边，另一边也要改
namespace TradeWatch;

public record PmConfig(string Id, string Name, string Strategy, string[] Symbols,
                       double Capital, double MaxGross, double MaxNet, double MaxPosition);

public static class Config
{
    public static readonly string[] Symbols = { "SNDK", "MU", "MSFT", "GOOGL" };

    // 三个 PM 团队，各自一种策略、一笔资金、一套限额（美元）
    public static readonly Dictionary<string, PmConfig> Pms = new[]
    {
        new PmConfig("PM_ALPHA", "Alpha Momentum", "momentum", Symbols, 2_000_000, 1_600_000, 1_000_000, 450_000),
        new PmConfig("PM_BETA", "Beta Mean Reversion", "meanrev", Symbols, 2_000_000, 1_600_000, 1_000_000, 450_000),
        new PmConfig("PM_GAMMA", "Gamma Pairs MU/SNDK", "pairs", new[] { "MU", "SNDK" }, 1_500_000, 1_000_000, 450_000, 500_000),
    }.ToDictionary(p => p.Id);

    // 两家券商（虚构的名字）
    public static readonly Dictionary<string, string> Brokers = new()
    {
        ["NBRG"] = "Northbridge Securities",
        ["HRBR"] = "Harbor Capital Markets",
    };

    // 哪只股票发给哪家券商
    public static readonly Dictionary<string, string> Routing = new()
    {
        ["SNDK"] = "NBRG", ["MU"] = "NBRG", ["MSFT"] = "HRBR", ["GOOGL"] = "HRBR",
    };

    // 每天开盘时券商给的可借券数量（卖空用）
    public static readonly Dictionary<string, int> DailyLocates = new()
    {
        ["SNDK"] = 3_000, ["MU"] = 5_000, ["MSFT"] = 8_000, ["GOOGL"] = 10_000,
    };

    // OMS / 风控
    public const int RebalanceEvery = 5;              // 每 5 根 K 线策略重新算一次目标仓位
    public const double MinOrderNotional = 25_000;    // 目标和现在差不到 2.5 万美元，就不下单
    public const double MaxOrderNotional = 750_000;   // 单个母单最多 75 万美元（防手滑）

    // EMS / TWAP 执行算法
    public const int TwapSlices = 8;                  // 母单拆成 8 个子单
    public const double ChildLimitBps = 5;            // 子单限价 = 当前价 ± 5 个基点
    public const int ChildTimeoutBars = 2;            // 子单 2 根 K 线还没成交完就撤掉，剩下的重新下
    public const int Lot = 10;                        // 股数取 10 的整数倍
}
