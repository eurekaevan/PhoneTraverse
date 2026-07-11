import os
import sys
import random
import time
import threading
import queue
import requests

# 确保控制台输出使用 UTF-8 编码
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

def main():
    print("===== 📱 手机号段归属地爬取工具 =====")
    print()

    # 获取当前脚本所在目录，所有文件保存在同一目录下
    script_dir = os.path.dirname(os.path.abspath(__file__))
    prefixes_phonenumber = os.path.join(script_dir, "prefixes_phonenumber.txt")
    save_path = os.path.join(script_dir, "phone_traverse.csv")
    cache_file = os.path.join(script_dir, "phone_traverse_cache.txt")

    prefixes_list = []
    last_index = 0
    last_prefix = 0
    failed_prefixes = set()

    # 1. 正在加载缓存
    print("📂 正在加载缓存...")
    if os.path.exists(cache_file):
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f.readlines() if line.strip()]
            
            if len(lines) > 0:
                try:
                    last_prefix = int(lines[0])
                    if os.path.exists(prefixes_phonenumber):
                        with open(prefixes_phonenumber, "r", encoding="utf-8") as pf:
                            prefix_lines = [line.strip() for line in pf.readlines() if line.strip()]
                        try:
                            last_index = prefix_lines.index(str(last_prefix))
                        except ValueError:
                            last_index = 0
                    else:
                        last_index = 0
                    print(f"   ✅ 从上次进度继续:  {last_prefix} - 第 {last_index} 个")
                except ValueError:
                    last_index = 0
            
            if len(lines) > 1:
                failed_parts = [p.strip() for p in lines[1].split(",") if p.strip()]
                failed_prefixes = set(failed_parts)
                print(f"   ⚠️  已加载 {len(failed_prefixes)} 个之前失败的序号")
        except Exception as e:
            print(f"   ❌ 读取缓存文件出错: {e}")
    else:
        print("   ℹ️  未发现缓存文件，将从头开始")

    print()
    
    # 2. 正在加载手机号前缀列表
    print("📋 正在加载手机号前缀列表...")
    if os.path.exists(prefixes_phonenumber):
        try:
            with open(prefixes_phonenumber, "r", encoding="utf-8") as f:
                prefixes_list = [line.strip() for line in f.readlines() if line.strip()]
            print(f"   ✅ 已加载 {len(prefixes_list)} 个手机号前缀")
        except Exception as e:
            print(f"   ❌ 读取前缀文件错误: {e}")
            return
    else:
        print(f"   ❌ 错误: 未找到手机号前缀文件: {prefixes_phonenumber}")
        return

    print()
    
    # 3. 正在准备任务队列
    print("🔄 正在准备任务队列...")
    to_process = []
    if len(failed_prefixes) > 0:
        print(f"   🔁 将重新爬取 {len(failed_prefixes)} 个之前失败的序号")
        to_process.extend(failed_prefixes)

    if last_index > 0 and last_index < len(prefixes_list):
        remaining = prefixes_list[last_index:]
        to_process.extend(remaining)
        print(f"   ⏭️  跳过前 {last_index} 个，剩余 {len(remaining)} 个待爬取")
    elif last_index == 0 and len(failed_prefixes) == 0:
        to_process = prefixes_list.copy()

    print(f"   📊 本次共需处理: {len(to_process)} 个")

    if len(to_process) == 0:
        print()
        print("🎉 没有需要处理的任务，程序退出")
        return

    print()
    
    # 4. 正在加载已有数据
    print("💾 正在加载已有数据...")
    existing_data = {}
    if os.path.exists(save_path):
        try:
            with open(save_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_str = line.strip()
                    if not line_str:
                        continue
                    parts = line_str.split(",")
                    if len(parts) > 0:
                        existing_data[parts[0].strip()] = line_str
            print(f"   ✅ 已加载 {len(existing_data)} 条现有数据")
        except Exception as e:
            print(f"   ⚠️ 读取已有数据失败: {e}")
    else:
        print("   ℹ️  未发现已有数据，将创建新文件")

    print()
    print("===== 🚀 开始爬取数据... =====")

    # 共享变量与锁
    processed_prefix_lock = threading.RLock()
    nonlocal_vars = {
        'processed_prefix': last_prefix,
        'processed_count': last_index,
        'success_count': 0,
    }
    
    total_to_process = len(to_process)
    
    # 保存缓存的方法
    def save_cache(current_val=None):
        with processed_prefix_lock:
            if current_val is not None:
                try:
                    nonlocal_vars['processed_prefix'] = int(current_val)
                except ValueError:
                    pass
            cache_content = f"{nonlocal_vars['processed_prefix']}\n{','.join(sorted(list(failed_prefixes)))}"
            try:
                with open(cache_file, "w", encoding="utf-8") as cf:
                    cf.write(cache_content)
            except Exception as e:
                pass

    # 保存数据文件的方法
    def save_data():
        with processed_prefix_lock:
            try:
                with open(save_path, "w", encoding="utf-8") as sf:
                    for val in existing_data.values():
                        sf.write(val + "\n")
            except Exception as e:
                pass

    # 使用 queue.Queue 进行轻量级分发
    task_queue = queue.Queue()
    for prefix in to_process:
        task_queue.put(prefix)
    
    # 添加终止信号
    for _ in range(3):
        task_queue.put(None)

    def worker():
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        
        while True:
            prefix = task_queue.get()
            if prefix is None:
                task_queue.task_done()
                break
            
            # 随机延迟，避免触发反爬机制
            time.sleep(random.uniform(0.5, 1.0))

            success = False
            phone = prefix + "0000"

            try:
                # 按照最新的调用方式，使用 x-www-form-urlencoded 提交 POST 请求
                response = session.post("https://api.ip33.com/mobile/s", data={"no": phone}, timeout=10)
                response.raise_for_status()

                result = response.json()
                
                if result.get("code") == 0:
                    with processed_prefix_lock:
                        if prefix in failed_prefixes:
                            failed_prefixes.remove(prefix)

                    # 适配新返回格式：注意 API 拼写为 provance，运营商字段为 type
                    province = result.get("provance", "") or ""
                    city = result.get("city", "") or ""
                    isp = result.get("type", "") or ""

                    line = f"{prefix},{province},{city},{isp}"

                    with processed_prefix_lock:
                        existing_data[prefix] = line
                        save_data()
                        nonlocal_vars['success_count'] += 1
                        nonlocal_vars['processed_count'] += 1
                        current = nonlocal_vars['processed_count']
                        percent = (current / total_to_process) * 100
                        print(f"[{current}/{total_to_process}] ({percent:.1f}%) ✅ {prefix} → {province}, {city}, {isp}")

                    success = True
                    save_cache(prefix)  # 成功后保存当前处理的号码到缓存
                else:
                    msg = result.get("message", "未返回有效数据")
                    with processed_prefix_lock:
                        nonlocal_vars['processed_count'] += 1
                        current = nonlocal_vars['processed_count']
                        print(f"[{current}/{total_to_process}] ⚠️ {prefix} - 查询失败: {msg}")
            except requests.exceptions.Timeout:
                with processed_prefix_lock:
                    nonlocal_vars['processed_count'] += 1
                    current = nonlocal_vars['processed_count']
                    print(f"[{current}/{total_to_process}] ⏱️  {prefix} - 请求超时")
            except requests.exceptions.RequestException as ex:
                with processed_prefix_lock:
                    nonlocal_vars['processed_count'] += 1
                    current = nonlocal_vars['processed_count']
                    print(f"[{current}/{total_to_process}] ❌ {prefix} - 网络错误: {ex}")
            except Exception as ex:
                with processed_prefix_lock:
                    nonlocal_vars['processed_count'] += 1
                    current = nonlocal_vars['processed_count']
                    print(f"[{current}/{total_to_process}] ❌ {prefix} - 错误: {ex}")

            # 失败情况处理
            if not success:
                with processed_prefix_lock:
                    failed_prefixes.add(prefix)
                save_cache(prefix)
            
            task_queue.task_done()

    threads = []
    for _ in range(3):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # 等待所有任务处理完毕
    for t in threads:
        t.join()

    if len(failed_prefixes) > 0:
        print("⚠️  失败序号已保存到缓存，下次运行将优先重试")
    else:
        print("🎉 全部处理完成！")

if __name__ == "__main__":
    main()
