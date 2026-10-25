// 交易核心的入口：dotnet TradeWatch.Trading.dll oms  或  dotnet TradeWatch.Trading.dll ems
using TradeWatch;

var mode = args.Length > 0 ? args[0] : "";
switch (mode)
{
    case "oms":
        new Oms().Run();
        break;
    case "ems":
        new Ems().Run();
        break;
    default:
        Console.Error.WriteLine("usage: TradeWatch.Trading oms|ems");
        Environment.Exit(2);
        break;
}
