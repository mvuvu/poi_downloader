
import psutil
import time
import json
from datetime import datetime
from pathlib import Path

class WorkerMonitor:
    """监控Worker线程状态"""
    
    def __init__(self):
        self.output_dir = Path("performance_analysis/worker_stats")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def monitor_threads(self, pid):
        """监控指定进程的线程状态"""
        try:
            process = psutil.Process(pid)
            threads = process.threads()
            
            stats = {
                'timestamp': datetime.now().isoformat(),
                'pid': pid,
                'thread_count': len(threads),
                'cpu_percent': process.cpu_percent(interval=1),
                'memory_mb': process.memory_info().rss / 1024 / 1024,
                'threads': []
            }
            
            # 分析线程状态
            for thread in threads:
                thread_info = {
                    'id': thread.id,
                    'cpu_time': thread.user_time + thread.system_time
                }
                stats['threads'].append(thread_info)
                
            return stats
            
        except Exception as e:
            print(f"监控错误: {e}")
            return None
            
    def find_crawler_process(self):
        """查找爬虫进程"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'poi_crawler_simple.py' in cmdline:
                    return proc.info['pid']
            except:
                continue
        return None
        
    def run(self, duration=300):
        """运行监控"""
        print("🔍 正在查找爬虫进程...")
        pid = self.find_crawler_process()
        
        if not pid:
            print("❌ 未找到爬虫进程，请先启动poi_crawler_simple.py")
            return
            
        print(f"✅ 找到爬虫进程 PID: {pid}")
        print(f"📊 开始监控 {duration} 秒...")
        
        stats_file = self.output_dir / f"worker_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        all_stats = []
        
        start_time = time.time()
        while time.time() - start_time < duration:
            stats = self.monitor_threads(pid)
            if stats:
                all_stats.append(stats)
                print(f"  线程数: {stats['thread_count']} | CPU: {stats['cpu_percent']:.1f}% | 内存: {stats['memory_mb']:.1f}MB")
            
            time.sleep(5)  # 每5秒采样一次
            
        # 保存统计数据
        with open(stats_file, 'w') as f:
            json.dump(all_stats, f, indent=2)
            
        print(f"\n✅ 监控完成，数据已保存到: {stats_file}")
        
        # 分析结果
        self.analyze_results(all_stats)
        
    def analyze_results(self, stats):
        """分析监控结果"""
        print("\n📈 监控结果分析:")
        
        cpu_values = [s['cpu_percent'] for s in stats]
        thread_counts = [s['thread_count'] for s in stats]
        
        print(f"  平均CPU使用率: {sum(cpu_values)/len(cpu_values):.1f}%")
        print(f"  CPU使用率范围: {min(cpu_values):.1f}% - {max(cpu_values):.1f}%")
        print(f"  平均线程数: {sum(thread_counts)/len(thread_counts):.1f}")
        
        # 检测低CPU使用率
        low_cpu_count = sum(1 for cpu in cpu_values if cpu < 20)
        if low_cpu_count > len(cpu_values) * 0.5:
            print("\n⚠️  警告: 超过50%的时间CPU使用率低于20%")
            print("  可能原因:")
            print("  - Worker线程在等待I/O（网页加载）")
            print("  - 任务队列为空")
            print("  - 过多的同步等待")

if __name__ == "__main__":
    monitor = WorkerMonitor()
    monitor.run(duration=300)  # 监控5分钟
