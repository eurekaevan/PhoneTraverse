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
if (File.Exists(cacheFile))
{
    var lines = await File.ReadAllLinesAsync(cacheFile);
    if (lines.Length > 0 && int.TryParse(lines[0].Trim(), out var idx))
    {
        lastPrefix = idx;
        if (File.Exists(prefixesPhonenumber))
        {
            var prefixLines = await File.ReadAllLinesAsync(prefixesPhonenumber);
            lastIndex = Array.IndexOf(prefixLines, lastPrefix.ToString());
            if (lastIndex == -1) lastIndex = 0; // 防止找不到导致异常
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

using var client = new HttpClient();
client.DefaultRequestHeaders.UserAgent.ParseAdd(
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
    var phone = prefix + "0000";

    try
    {
        // 按照最新的调用方式，使用 x-www-form-urlencoded 提交 POST 请求
        var content = new FormUrlEncodedContent(new Dictionary<string, string>
        {
            ["no"] = phone
        });

        using var response = await client.PostAsync("https://api.ip33.com/mobile/s", content, ct);
        response.EnsureSuccessStatusCode();

        var result = await response.Content.ReadAsStringAsync(ct);
        using var jsonDoc = JsonDocument.Parse(result);
        var root = jsonDoc.RootElement;

        if (root.TryGetProperty("code", out var codeProp) && codeProp.GetInt32() == 0)
        {
            lock (cacheLock)
            {
                failedPrefixes.Remove(prefix);
            }

            // 适配新返回格式：注意 API 拼写为 provance，运营商字段为 type
            var province = root.TryGetProperty("provance", out var pProp) ? pProp.GetString() ?? "" : "";
            var city = root.TryGetProperty("city", out var cProp) ? cProp.GetString() ?? "" : "";
            var isp = root.TryGetProperty("type", out var tProp) ? tProp.GetString() ?? "" : "";

            var line = $"{prefix},{province},{city},{isp}";

            await semaphore.WaitAsync(ct);
            try
            {
                existingData[prefix] = line;
                SaveData();
            }
            finally
            {
                semaphore.Release();
            }

            Interlocked.Increment(ref successCount);
            var current = Interlocked.Increment(ref processedCount);
            var percent = (double)current / totalToProcess * 100;
            Console.WriteLine($"[{current}/{totalToProcess}] ({percent:F1}%) ✅ {prefix} → {province}, {city}, {isp}");
            
            success = true;
            SaveCache(prefix); // 成功后保存当前处理的号码到缓存
        }
        else
        {
            var msg = root.TryGetProperty("message", out var msgProp) ? msgProp.GetString() : "未返回有效数据";
            var current = Interlocked.Increment(ref processedCount);
            Console.WriteLine($"[{current}/{totalToProcess}] ⚠️ {prefix} - 查询失败: {msg}");
        }
    }
    catch (HttpRequestException ex)
    {
        var current = Interlocked.Increment(ref processedCount);
        Console.WriteLine($"[{current}/{totalToProcess}] ❌ {prefix} - 网络错误: {ex.Message}");
    }
    catch (TaskCanceledException)
    {
        var current = Interlocked.Increment(ref processedCount);
        Console.WriteLine($"[{current}/{totalToProcess}] ⏱️  {prefix} - 请求超时");
    }
    catch (Exception ex)
    {
        var current = Interlocked.Increment(ref processedCount);
        Console.WriteLine($"[{current}/{totalToProcess}] ❌ {prefix} - 错误: {ex.Message}");
    }

    // 失败情况处理
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
    lock (cacheLock) // 复用 cacheLock 或者使用单独的 lock 确保写入安全
    {
        File.WriteAllLines(savePath, existingData.Values, Encoding.UTF8);
    }
}