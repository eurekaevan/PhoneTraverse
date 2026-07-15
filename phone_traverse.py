import os
import sys
import random
import time
import threading
import queue
import requests
import argparse
import pandas as pd
from sqlalchemy import create_engine

# 确保控制台输出使用 UTF-8 编码
try:
    sys.stdout.reconfigure(encoding='utf-8')
except AttributeError:
    pass

def parse_args():
    parser = argparse.ArgumentParser(description="分布式手机号段归属地爬取工具")
    parser.add_argument("--vps-id", type=int, required=True, help="当前 VPS 的编号 (从 1 开始)")
    parser.add_argument("--total-vps", type=int, required=True, help="参与计算的 VPS 总台数")
    parser.add_argument("--db-url", type=str, required=True, help="Supabase Connection String (Pooler URL)")
    return parser.parse_args()

def main():
    args = parse_args()
    
    print(f"===== 📱 分布式号段爬取工具 [VPS {args.vps_id} / {args.total_vps}] =====")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    prefixes_phonenumber = os.path.join(script_dir, "prefixes_phonenumber.txt")
    
    # 1. 初始化数据库连接
    try:
        engine = create_engine(args.db_url, pool_size=5, max_overflow=10)
    except Exception as e:
        print(f"❌ 数据库连接失败: {e}")
        return

    # 2. 加载完整的号段前缀
    if not os.path.exists(prefixes_phonenumber):
        print(f"❌ 错误: 未找到手机号前缀文件: {prefixes_phonenumber}")
        return
        
    with open(prefixes_phonenumber, "r", encoding="utf-8") as f:
        all_prefixes = [line.strip() for line in f.readlines() if line.strip()]
    print(f"📋 成功加载本地总前缀数: {len(all_prefixes)} 个")

    # 3. 任务切分：过滤出属于当前 VPS 的任务
    # 取模切分：只有 (索引 % 总台数 == 当前编号-1) 的任务才留给当前机器
    vps_tasks = [p for i, p in enumerate(all_prefixes) if i % args.total_vps == (args.vps_id - 1)]
    print(f"✂️  属于当前 VPS 的分配任务数: {len(vps_tasks)} 个")

    # 4. 云端断点续传：从数据库查询已经成功爬取的数据，进行去重
    print("🔍 正在同步云端进度以实现断点续传...")
    try:
        with engine.connect() as conn:
            # 只查出已存在的 prefix 列即可，速度极快
            completed_df = pd.read_sql("SELECT prefix FROM phone_records", conn)
            completed_set = set(completed_df['prefix'].tolist())
    except Exception as e:
        print(f"⚠️  读取云端现有进度失败(可能是首次建表为空): {e}")
        completed_set = set()

    # 排除掉已经做完的任务
    final_tasks = [p for p in vps_tasks if p not in completed_set]
    print(f"⏭️  过滤掉已完成任务，当前实际需执行: {len(final_tasks)} 个")

    if not final_tasks:
        print("🎉 当前 VPS 分配的任务已全部完成！")
        return

    # 5. 准备队列与内存缓冲区
    task_queue = queue.Queue()
    for p in final_tasks:
        task_queue.put(p)
        
    # 终止信号
    thread_count = 3
    for _ in range(thread_count):
        task_queue.put(None)

    # 内存缓冲区：攒够一定数量再批量写入数据库，暴刷 IO 效率
    db_buffer = []
    buffer_lock = threading.Lock()
    total_to_do = len(final_tasks)
    processed_count = 0
    success_count = 0

    def flush_buffer_to_db():
        """将缓冲区的数据一次性批量写入 Supabase"""
        nonlocal db_buffer
        with buffer_lock:
            if not db_buffer:
                return
            try:
                df = pd.DataFrame(db_buffer)
                # 使用 method='multi' 并指定 chunksize 实现真正的批量 Insert
                df.to_sql('phone_records', con=engine, if_exists='append', index=False, method='multi', chunksize=500)
                db_buffer.clear()
            except Exception as ex:
                print(f"\n❌ 批量写入数据库失败: {ex}，数据仍保留在内存中等待下次重试")

    # 6. 核心爬取工作线程
    def worker():
        nonlocal processed_count, success_count
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })
        
        while True:
            prefix = task_queue.get()
            if prefix is None:
                task_queue.task_done()
                break
            
            # 随机延迟避免反爬
            time.sleep(random.uniform(1, 8))
            phone = prefix + "0000"

            # 区分：是接口正常响应但无数据，还是网络彻底断开
            should_mark_done = False
            record_data = {
                "prefix": prefix,
                "province": None, # Python 的 None 写入 PostgreSQL 后会自动变成标准 NULL
                "city": None,
                "isp": None,
                "vps_id": f"vps-{args.vps_id}"
            }

            try:
                response = session.post("https://api.ip33.com/mobile/s", data={"no": phone}, timeout=10)
                response.raise_for_status()
                result = response.json()
                
                processed_count += 1
                percent = (processed_count / total_to_do) * 100

                if result.get("code") == 0:
                    # 正常跑出数据的情况
                    record_data["province"] = result.get("provance", "") or None
                    record_data["city"] = result.get("city", "") or None
                    record_data["isp"] = result.get("type", "") or None
                    
                    success_count += 1
                    print(f"[{processed_count}/{total_to_do}] ({percent:.1f}%) ✅ {prefix} → {record_data['province']} {record_data['city']}")
                    should_mark_done = True
                else:
                    # 接口明确返回 code != 0 (例如：未分配的号段、空号、非法号段)
                    # 这种属于“有效爬取，明确无数据”，必须写入数据库留空，防止以后重复爬取
                    msg = result.get("message", "接口返回无数据")
                    print(f"[{processed_count}/{total_to_do}] ({percent:.1f}%) 🕳️  {prefix} - 接口无数据({msg})，已留空标记")
                    should_mark_done = True
                    
            except Exception as ex:
                # 这种属于网络超时、代理挂了、或者对方服务器 502 等“非正常响应”
                # 此时不应该写入数据库留空！因为该号段本身可能有数据，只是这次网络坏了。
                # 留空不写入，等下次 VPS 重启或者断点续传时，还会重新爬取，保证数据不漏。
                processed_count += 1
                percent = (processed_count / total_to_do) * 100
                print(f"[{processed_count}/{total_to_do}] ({percent:.1f}%) ❌ {prefix} - 网络或服务器错误: {ex}，将跳过并等待日后重试")
                should_mark_done = False

            # 如果确定需要标记为“已完成”（无论是抓到数据还是明确空号），都塞入缓冲区
            if should_mark_done:
                with buffer_lock:
                    db_buffer.append(record_data)
                
                # 缓冲区攒够 100 条（不管是正常的还是留空的）就往 Supabase 批量冲洗一次
                if len(db_buffer) >= 100:
                    flush_buffer_to_db()
            
            task_queue.task_done()

    # 7. 启动多线程
    threads = []
    for _ in range(thread_count):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # 等待所有线程结束
    for t in threads:
        t.join()

    # 8. 最后收尾：把缓冲区里剩下的数据全部刷入数据库
    flush_buffer_to_db()
    print(f"\n🎉 VPS {args.vps_id} 运行结束。本次共处理 {processed_count} 个任务，成功写入 {success_count} 条数据到云端！")

if __name__ == "__main__":
    main()