using System.Text;
using System.Text.Json;

Console.OutputEncoding = Encoding.UTF8;
Console.WriteLine("===== 📱 手机号段归属地爬取工具 =====");
Console.WriteLine();

var desktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
var prefixesPhonenumber = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "prefixes_phonenumber.txt");
var savePath = Path.Combine(desktopPath, "phone_traverse.csv");
var cacheFile = Path.Combine(desktopPath, "phone_traverse_cache.txt"); // 缓存文件

var prefixesList = new List<string>();
var lastIndex = 0;
var lastPrefix = 0;
var failedPrefixes = new HashSet<string>();

Console.WriteLine("📂 正在加载缓存...");
// 加载缓存：第一行是进度，第二行是失败序号（逗号分隔）
if (File.Exists(cacheFile))
{
    var lines = await File.ReadAllLinesAsync(cacheFile);
    if (lines.Length > 0 && int.TryParse(lines[0].Trim(), out var idx))
    {
        lastPrefix = idx;
        // 计算上次已处理的索引（读取前缀文件并统计在目标值之前的行数）
        if (File.Exists(prefixesPhonenumber))
        {
            var prefixLines = await File.ReadAllLinesAsync(prefixesPhonenumber);
            lastIndex = prefixLines.IndexOf(lastPrefix.ToString());
        }
        else
        {
            lastIndex = 0;
        }

        Console.WriteLine($"   ✅ 从上次进度继续:  {lastPrefix} - 第 {lastIndex} 个");
    }

    if (lines.Length > 1 && !string.IsNullOrWhiteSpace(lines[1]))
    {
        failedPrefixes = lines[1].Split(',', StringSplitOptions.RemoveEmptyEntries).Select(s => s.Trim()).ToHashSet();
        Console.WriteLine($"   ⚠️  已加载 {failedPrefixes.Count} 个之前失败的序号");
    }
}
else
{
    Console.WriteLine("   ℹ️  未发现缓存文件，将从头开始");
}

Console.WriteLine();
Console.WriteLine("📋 正在加载手机号前缀列表...");
if (File.Exists(prefixesPhonenumber))
{
    var prefixes = await File.ReadAllLinesAsync(prefixesPhonenumber);
    prefixesList.AddRange(prefixes.Select(prefix => prefix.Trim()));
    Console.WriteLine($"   ✅ 已加载 {prefixesList.Count} 个手机号前缀");
}
else
{
    Console.WriteLine($"   ❌ 错误: 未找到手机号前缀文件: {prefixesPhonenumber}");
    return;
}

Console.WriteLine();
Console.WriteLine("🔄 正在准备任务队列...");
// 合并待处理列表：失败的 + 剩余未处理的
var toProcess = new List<string>();
if (failedPrefixes.Count > 0)
{
    Console.WriteLine($"   🔁 将重新爬取 {failedPrefixes.Count} 个之前失败的序号");
    toProcess.AddRange(failedPrefixes);
}

switch (lastIndex)
{
    case > 0 when lastIndex < prefixesList.Count:
    {
        var remaining = prefixesList.Skip(lastIndex).ToList();
        toProcess.AddRange(remaining);
        Console.WriteLine($"   ⏭️  跳过前 {lastIndex} 个，剩余 {remaining.Count} 个待爬取");
        break;
    }
    case 0 when failedPrefixes.Count == 0:
        toProcess = prefixesList;
        break;
}

Console.WriteLine($"   📊 本次共需处理: {toProcess.Count} 个");

if (toProcess.Count == 0)
{
    Console.WriteLine();
    Console.WriteLine("🎉 没有需要处理的任务，程序退出");
    return;
}

Console.WriteLine();
Console.WriteLine("💾 正在加载已有数据...");
// 加载已有数据到内存（用于去重和覆盖）
var existingData = new Dictionary<string, string>();
if (File.Exists(savePath))
{
    var existingLines = await File.ReadAllLinesAsync(savePath);
    foreach (var line in existingLines)
    {
        if (string.IsNullOrWhiteSpace(line)) continue;
        var parts = line.Split(',');
        if (parts.Length > 0) existingData[parts[0].Trim()] = line;
    }

    Console.WriteLine($"   ✅ 已加载 {existingData.Count} 条现有数据");
}
else
{
    Console.WriteLine("   ℹ️  未发现已有数据，将创建新文件");
}

Console.WriteLine();
Console.WriteLine("===== 🚀 开始爬取数据... =====");

// 在循环外创建 HttpClient (复用)
using var client = new HttpClient();
client.DefaultRequestHeaders.Add("Referer", "https://ipw.cn/phone/");
client.DefaultRequestHeaders.Add("User-Agent",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36");

using var semaphore = new SemaphoreSlim(1, 1);

var processedPrefix = lastPrefix;
var processedCount = lastIndex;
var successCount = 0;
var totalToProcess = toProcess.Count;
var cacheLock = new object();

await Parallel.ForEachAsync(toProcess, new ParallelOptions { MaxDegreeOfParallelism = 3 }, async (prefix, ct) =>
{
    // 随机延迟，避免触发反爬机制
    await Task.Delay(Random.Shared.Next(500, 1000), ct);

    var success = false;
    try
    {
        var result = await client.GetStringAsync($"https://ipw.cn/api/phone/query?number={prefix}", ct);

        using var jsonDoc = JsonDocument.Parse(result);
        var root = jsonDoc.RootElement;

        if (root.TryGetProperty("code", out var codeProp))
        {
            lock (cacheLock)
            {
                failedPrefixes.Remove(prefix); // 从失败列表移除
            }

            if (codeProp.GetString() == "success")
            {
                if (root.TryGetProperty("data", out var data))
                {
                    var province = data.GetProperty("province").GetString();
                    var city = data.GetProperty("city").GetString();
                    var isp = data.GetProperty("isp").GetString();
                    var line = $"{prefix},{province},{city},{isp}";

                    await semaphore.WaitAsync(ct);
                    try
                    {
                        // 存在则覆盖，不存在则新增
                        existingData[prefix] = line;
                        SaveData();
                        SaveCache(prefix); // 保存当前处理的号码到缓存
                        Interlocked.Increment(ref successCount);
                        var current = processedCount + 1;
                        var percent = (double)current / totalToProcess * 100;
                        Console.WriteLine(
                            $"[{current}/{totalToProcess}] ({percent:F1}%) ✅ {prefix} → {province}, {city}, {isp}");
                        success = true;
                    }
                    finally
                    {
                        semaphore.Release();
                    }
                }
                else
                {
                    var msg = root.TryGetProperty("message", out var msgProp) ? msgProp.GetString() : "未知原因";
                    Console.WriteLine($"[{processedCount + 1}/{totalToProcess}] ⚠️  {prefix} - API返回成功但缺少数据: {msg}");
                }
            }
        }
    }
    catch (HttpRequestException ex)
    {
        Console.WriteLine($"[{processedCount + 1}/{totalToProcess}] ❌ {prefix} - 网络错误: {ex.Message}");
    }
    catch (TaskCanceledException)
    {
        Console.WriteLine($"[{processedCount + 1}/{totalToProcess}] ⏱️  {prefix} - 请求超时");
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[{processedCount + 1}/{totalToProcess}] ❌ {prefix} - 错误: {ex.Message}");
    }

    // 更新进度并即时保存缓存
    Interlocked.Increment(ref processedCount);
    if (!success)
    {
        lock (cacheLock)
        {
            failedPrefixes.Add(prefix); // 失败则加入失败列表
        }

        SaveCache(prefix);
    }
});

Console.WriteLine(failedPrefixes.Count > 0 ? "⚠️  失败序号已保存到缓存，下次运行将优先重试" : "🎉 全部处理完成！");
return;

// 即时保存缓存的方法
void SaveCache(string? current = null)
{
    lock (cacheLock)
    {
        // 如果传入了当前处理的号码，更新进度记录
        if (current != null && int.TryParse(current, out var p))
        {
            processedPrefix = p;
        }

        var cacheContent = $"{processedPrefix}\n{string.Join(",", failedPrefixes)}";
        File.WriteAllText(cacheFile, cacheContent);
    }
}

// 即时保存数据文件的方法
void SaveData()
{
    lock (cacheLock)
    {
        File.WriteAllLines(savePath, existingData.Values, Encoding.UTF8);
    }
}