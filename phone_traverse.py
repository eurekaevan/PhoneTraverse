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
    
    # 3. 正在准备任务队列 (拆分为重试和正常两部分)
    print("🔄 正在准备任务队列...")
    retry_tasks = list(failed_prefixes)
    normal_tasks = []
    
    if last_index < len(prefixes_list):
        normal_tasks = prefixes_list[last_index:]
        
    print(f"   🔁 失败重试任务: {len(retry_tasks)} 个")
    print(f"   ⏭️  正常进度任务: {len(normal_tasks)} 个 (跳过前 {last_index} 个)")

    if len(retry_tasks) == 0 and len(normal_tasks) == 0:
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
        'processed_count': 0, 
        'success_count': 0,
    }
    
    # 保存缓存的方法 (增加 update_main_progress 标志，重试时不推进度号)
    def save_cache(current_val=None, update_main_progress=True):
        with processed_prefix_lock:
            if current_val is not None and update_main_progress:
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

    # 核心并发控制逻辑封装
    def process_tasks(tasks, is_retry_phase=False):
        if not tasks:
            return
            
        task_queue = queue.Queue()
        for prefix in tasks:
            task_queue.put(prefix)
        
        # 添加终止信号 (3 个线程)
        for _ in range(3):
            task_queue.put(None)

        nonlocal_vars['processed_count'] = 0
        total_tasks = len(tasks)
        phase_name = "重试阶段" if is_retry_phase else "正常进度"

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
                time.sleep(random.uniform(1, 10))

                success = False
                phone = prefix + "0000"

                try:
                    response = session.post("https://api.ip33.com/mobile/s", data={"no": phone}, timeout=10)
                    response.raise_for_status()
                    result = response.json()
                    
                    if result.get("code") == 0:
                        with processed_prefix_lock:
                            if prefix in failed_prefixes:
                                failed_prefixes.remove(prefix)

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
                            percent = (current / total_tasks) * 100
                            print(f"[{phase_name}] [{current}/{total_tasks}] ({percent:.1f}%) ✅ {prefix} → {province}, {city}, {isp}")

                        success = True
                        save_cache(prefix, update_main_progress=not is_retry_phase)
                    else:
                        msg = result.get("message", "未返回有效数据")
                        with processed_prefix_lock:
                            nonlocal_vars['processed_count'] += 1
                            current = nonlocal_vars['processed_count']
                            print(f"[{phase_name}] [{current}/{total_tasks}] ⚠️ {prefix} - 查询失败: {msg}")
                except requests.exceptions.Timeout:
                    with processed_prefix_lock:
                        nonlocal_vars['processed_count'] += 1
                        current = nonlocal_vars['processed_count']
                        print(f"[{phase_name}] [{current}/{total_tasks}] ⏱️  {prefix} - 请求超时")
                except requests.exceptions.RequestException as ex:
                    with processed_prefix_lock:
                        nonlocal_vars['processed_count'] += 1
                        current = nonlocal_vars['processed_count']
                        print(f"[{phase_name}] [{current}/{total_tasks}] ❌ {prefix} - 网络错误: {ex}")
                except Exception as ex:
                    with processed_prefix_lock:
                        nonlocal_vars['processed_count'] += 1
                        current = nonlocal_vars['processed_count']
                        print(f"[{phase_name}] [{current}/{total_tasks}] ❌ {prefix} - 错误: {ex}")

                # 失败情况处理
                if not success:
                    with processed_prefix_lock:
                        failed_prefixes.add(prefix)
                    save_cache(prefix, update_main_progress=not is_retry_phase)
                
                task_queue.task_done()

        threads = []
        for _ in range(3):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

    # ====== 过程 1：重试失败序号 ======
    if retry_tasks:
        print(f"\n--- 🔄 开始执行 [重试阶段] ---")
        process_tasks(retry_tasks, is_retry_phase=True)
        print(f"--- ⏹️ [重试阶段] 结束，剩余未成功重试数: {len(failed_prefixes)} ---")

    # ====== 过程 2：从上次进度继续 ======
    if normal_tasks:
        print(f"\n--- 🚀 开始执行 [正常进度] ---")
        process_tasks(normal_tasks, is_retry_phase=False)
        print("--- ⏹️ [正常进度] 结束 ---")

    if len(failed_prefixes) > 0:
        print("\n⚠️  部分序号仍查询失败并保存到缓存，下次运行将优先重试")
    else:
        print("\n🎉 全部处理完成！")

if __name__ == "__main__":
    main()