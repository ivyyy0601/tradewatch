// PM 的策略：看价格历史，给出"目标仓位"（每只股票想持有多少股，负数 = 卖空）
// 策略只说"想要什么"，怎么买卖由 OMS 和执行算法负责。（和 Python 版 strategies.py 一样）
namespace TradeWatch;

public static class Strategies
{
    static int Shares(double notional, double price, int lot = 10) =>
        (int)Math.Round(notional / price / lot) * lot;          // Math.Round 默认"四舍六入五成双"，和 Python 的 round 一样

    static double Clamp(double x) => Math.Max(-1.0, Math.Min(1.0, x));

    static double Mean(IReadOnlyList<double> xs) => xs.Average();

    static double PStdev(IReadOnlyList<double> xs)             // 总体标准差（Python 的 statistics.pstdev）
    {
        var m = xs.Average();
        return Math.Sqrt(xs.Sum(x => (x - m) * (x - m)) / xs.Count);
    }

    static List<double> Last(List<double> xs, int n) => xs.Skip(Math.Max(0, xs.Count - n)).ToList();

    /// <summary>动量：过去 30 分钟涨得越多，越想持有多头；跌得越多，越想做空。</summary>
    public static Dictionary<string, int> Momentum(Dictionary<string, List<double>> hist, Dictionary<string, double> prices, PmConfig pm)
    {
        var targets = new Dictionary<string, int>();
        foreach (var s in pm.Symbols)
        {
            if (!hist.TryGetValue(s, out var h) || h.Count < 31) continue;
            var r = h[^1] / h[^31] - 1;
            var signal = Clamp(r / 0.01);                         // 涨 1% 就满仓
            targets[s] = Shares(signal * pm.MaxPosition * 0.5, prices[s]);
        }
        return targets;
    }

    /// <summary>均值回归：价格比 60 分钟均价高出越多（z 分数），越想做空，反之做多。</summary>
    public static Dictionary<string, int> MeanRev(Dictionary<string, List<double>> hist, Dictionary<string, double> prices, PmConfig pm)
    {
        var targets = new Dictionary<string, int>();
        foreach (var s in pm.Symbols)
        {
            if (!hist.TryGetValue(s, out var all)) continue;
            var h = Last(all, 60);
            if (h.Count < 60) continue;
            var sd = PStdev(h);
            if (sd == 0) continue;
            var z = (h[^1] - Mean(h)) / sd;
            var signal = Clamp(-z / 2);
            targets[s] = Shares(signal * pm.MaxPosition * 0.5, prices[s]);
        }
        return targets;
    }

    static int pairState;                                       // 1 = 买 MU 卖 SNDK，-1 = 反过来，0 = 空仓

    /// <summary>配对交易：MU 和 SNDK 通常一起涨跌。log(MU/SNDK) 偏离平时太多，就买便宜的、卖贵的，
    /// 两边金额相等（市场中性），等它回到平时的关系再平仓。</summary>
    public static Dictionary<string, int> Pairs(Dictionary<string, List<double>> hist, Dictionary<string, double> prices, PmConfig pm)
    {
        const string a = "MU", b = "SNDK";
        if (!hist.TryGetValue(a, out var ha) || !hist.TryGetValue(b, out var hb) || ha.Count < 60 || hb.Count < 60)
            return new();
        var ratio = Last(ha, 60).Zip(Last(hb, 60), (x, y) => Math.Log(x / y)).ToList();
        var sd = PStdev(ratio);
        if (sd == 0) return new();
        var z = (ratio[^1] - Mean(ratio)) / sd;
        if (z > 1.5) pairState = -1;                            // MU 相对贵了：卖 MU、买 SNDK
        else if (z < -1.5) pairState = 1;
        else if (Math.Abs(z) < 0.3) pairState = 0;              // 回到正常关系，平仓
        var leg = pm.MaxPosition * 0.8;
        return new() { [a] = Shares(pairState * leg, prices[a]), [b] = Shares(-pairState * leg, prices[b]) };
    }

    public static Dictionary<string, int> Run(string strategy, Dictionary<string, List<double>> hist,
                                              Dictionary<string, double> prices, PmConfig pm) => strategy switch
    {
        "momentum" => Momentum(hist, prices, pm),
        "meanrev" => MeanRev(hist, prices, pm),
        "pairs" => Pairs(hist, prices, pm),
        _ => new(),
    };
}
