#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析POI爬虫运行时的性能瓶颈
通过在现有代码中添加监控点来收集数据
"""

import json
import time
import threading
from datetime import datetime
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os

class CrawlerPerformanceAnalyzer:
    """爬虫性能分析器 - 不修改原代码，通过日志分析"""
    
    def __init__(self):
        self.analysis_dir = Path("performance_analysis")
        self.analysis_dir.mkdir(exist_ok=True)
        
    def analyze_queue_status(self):
        """分析任务队列状态 - 通过模拟监控"""
        print("\n📊 分析任务队列状态...")
        
        # 创建模拟监控脚本
        monitor_script = """
import time
import json
from datetime import datetime
from pathlib import Path

# 监控输出目录
output_dir = Path("performance_analysis/queue_monitor")
output_dir.mkdir(parents=True, exist_ok=True)

print("🔍 开始监控任务队列...")
print("⚠️  注意：需要手动观察poi_crawler_simple.py的输出来收集以下数据：")
print("1. 每5分钟记录一次已处理任务数")
print("2. 观察重试任务的出现频率")
print("3. 记录Chrome重启的时间点")
print("4. 注意是否出现长时间无输出的情况")

# 创建数据记录模板
data_template = {
    "timestamp": "",
    "elapsed_minutes": 0,
    "total_processed": 0,
    "retry_count": 0,
    "chrome_restart_count": 0,
    "notes": ""
}

# 保存模板
with open(output_dir / "data_template.json", 'w') as f:
    json.dump(data_template, f, indent=2)

print(f"\\n📝 请手动记录数据到: {output_dir / 'manual_records.json'}")
print("格式示例：")
print(json.dumps(data_template, indent=2))
"""
        
        # 保存监控脚本
        script_path = self.analysis_dir / "queue_monitor.py"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(monitor_script)
            
        print(f"✅ 监控脚本已创建: {script_path}")
        print("📋 使用方法：")
        print("1. 在运行爬虫的同时运行此脚本")
        print("2. 手动记录观察到的数据")
        print("3. 分析记录的数据找出性能瓶颈")
        
    def analyze_task_timing(self):
        """分析任务处理时间 - 通过日志分析"""
        print("\n⏱️  分析任务处理时间...")
        
        # 创建时间分析脚本
        timing_script = """
import re
from datetime import datetime
from pathlib import Path
import json

def analyze_log_timing(log_file):
    \"\"\"分析日志中的时间信息\"\"\"
    
    # 时间模式匹配
    patterns = {
        'task_start': r'🔍 处理地址: (.+)',
        'task_complete': r'✅ (.+) \\| POI: (\\d+) \\| 状态: 已保存',
        'task_retry': r'🔄 非建筑物，使用日文地址重试: (.+)',
        'chrome_restart': r'🔄 Worker (\\d+): 达到1000个任务，重启Chrome驱动',
        'error': r'❌ (.+) \\| 错误: (.+)'
    }
    
    # 读取日志并分析
    task_times = []
    current_tasks = {}
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 提取时间戳（如果日志包含）
            # 这里需要根据实际日志格式调整
            
            for pattern_name, pattern in patterns.items():
                match = re.search(pattern, line)
                if match:
                    if pattern_name == 'task_start':
                        address = match.group(1)
                        current_tasks[address] = {'start': datetime.now()}
                    elif pattern_name == 'task_complete':
                        address = match.group(1)
                        if address in current_tasks:
                            duration = (datetime.now() - current_tasks[address]['start']).seconds
                            task_times.append({
                                'address': address,
                                'duration': duration,
                                'poi_count': int(match.group(2)),
                                'type': 'normal'
                            })
                    elif pattern_name == 'task_retry':
                        # 记录重试任务
                        task_times.append({
                            'address': match.group(1),
                            'type': 'retry'
                        })
    
    return task_times

print("📋 任务时间分析脚本")
print("使用方法：")
print("1. 将爬虫输出重定向到文件: python poi_crawler_simple.py > crawler_output.log 2>&1")
print("2. 运行分析: python analyze_timing.py crawler_output.log")
"""
        
        script_path = self.analysis_dir / "analyze_timing.py"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(timing_script)
            
        print(f"✅ 时间分析脚本已创建: {script_path}")
        
    def create_worker_monitor(self):
        """创建Worker状态监控工具"""
        print("\n👷 创建Worker状态监控...")
        
        worker_monitor = """
import psutil
import time
import json
from datetime import datetime
from pathlib import Path

class WorkerMonitor:
    \"\"\"监控Worker线程状态\"\"\"
    
    def __init__(self):
        self.output_dir = Path("performance_analysis/worker_stats")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def monitor_threads(self, pid):
        \"\"\"监控指定进程的线程状态\"\"\"
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
        \"\"\"查找爬虫进程\"\"\"
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'poi_crawler_simple.py' in cmdline:
                    return proc.info['pid']
            except:
                continue
        return None
        
    def run(self, duration=300):
        \"\"\"运行监控\"\"\"
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
            
        print(f"\\n✅ 监控完成，数据已保存到: {stats_file}")
        
        # 分析结果
        self.analyze_results(all_stats)
        
    def analyze_results(self, stats):
        \"\"\"分析监控结果\"\"\"
        print("\\n📈 监控结果分析:")
        
        cpu_values = [s['cpu_percent'] for s in stats]
        thread_counts = [s['thread_count'] for s in stats]
        
        print(f"  平均CPU使用率: {sum(cpu_values)/len(cpu_values):.1f}%")
        print(f"  CPU使用率范围: {min(cpu_values):.1f}% - {max(cpu_values):.1f}%")
        print(f"  平均线程数: {sum(thread_counts)/len(thread_counts):.1f}")
        
        # 检测低CPU使用率
        low_cpu_count = sum(1 for cpu in cpu_values if cpu < 20)
        if low_cpu_count > len(cpu_values) * 0.5:
            print("\\n⚠️  警告: 超过50%的时间CPU使用率低于20%")
            print("  可能原因:")
            print("  - Worker线程在等待I/O（网页加载）")
            print("  - 任务队列为空")
            print("  - 过多的同步等待")

if __name__ == "__main__":
    monitor = WorkerMonitor()
    monitor.run(duration=300)  # 监控5分钟
"""
        
        script_path = self.analysis_dir / "worker_monitor.py"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(worker_monitor)
            
        print(f"✅ Worker监控脚本已创建: {script_path}")
        
    def create_retry_analyzer(self):
        """创建重试任务分析器"""
        print("\n🔄 创建重试任务分析器...")
        
        retry_analyzer = """
import re
from collections import Counter
from pathlib import Path
import json

def analyze_retry_patterns(log_file):
    \"\"\"分析重试任务模式\"\"\"
    
    retry_addresses = []
    normal_addresses = []
    retry_pattern = r'🔄 非建筑物，使用日文地址重试: (.+)'
    success_pattern = r'✅ (.+) \\| POI: (\\d+) \\| 状态: 已保存'
    
    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            # 收集重试地址
            retry_match = re.search(retry_pattern, line)
            if retry_match:
                retry_addresses.append(retry_match.group(1))
            
            # 收集成功地址
            success_match = re.search(success_pattern, line)
            if success_match:
                normal_addresses.append(success_match.group(1))
    
    # 分析
    total_tasks = len(normal_addresses) + len(retry_addresses)
    retry_rate = len(retry_addresses) / total_tasks * 100 if total_tasks > 0 else 0
    
    print(f"\\n📊 重试任务分析:")
    print(f"  总任务数: {total_tasks}")
    print(f"  重试任务数: {len(retry_addresses)}")
    print(f"  重试率: {retry_rate:.1f}%")
    
    # 分析重试地址特征
    if retry_addresses:
        print(f"\\n🔍 重试地址示例:")
        for addr in retry_addresses[:5]:
            print(f"  - {addr}")
    
    # 保存分析结果
    results = {
        'total_tasks': total_tasks,
        'retry_count': len(retry_addresses),
        'retry_rate': retry_rate,
        'retry_addresses_sample': retry_addresses[:20]
    }
    
    with open('performance_analysis/retry_analysis.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    return results

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        analyze_retry_patterns(sys.argv[1])
    else:
        print("使用方法: python retry_analyzer.py crawler_output.log")
"""
        
        script_path = self.analysis_dir / "retry_analyzer.py"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(retry_analyzer)
            
        print(f"✅ 重试分析脚本已创建: {script_path}")
        
    def create_performance_test(self):
        """创建性能测试脚本"""
        print("\n🧪 创建性能测试脚本...")
        
        test_script = """
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
\"\"\"
POI爬虫性能测试脚本
用于识别性能瓶颈
\"\"\"

import subprocess
import time
import psutil
import json
from datetime import datetime
from pathlib import Path

def run_performance_test():
    \"\"\"运行性能测试\"\"\"
    
    print("🚀 POI爬虫性能测试")
    print("=" * 50)
    
    # 测试配置
    test_configs = [
        {
            'name': '小批量测试',
            'file': 'data/input/test_small.csv',  # 需要准备10条数据的测试文件
            'expected_time': 300  # 预期5分钟内完成
        },
        {
            'name': '中等批量测试',
            'file': 'data/input/test_medium.csv',  # 需要准备50条数据的测试文件
            'expected_time': 1800  # 预期30分钟内完成
        }
    ]
    
    results = []
    
    for config in test_configs:
        print(f"\\n📊 运行测试: {config['name']}")
        print(f"  测试文件: {config['file']}")
        
        # 记录开始状态
        start_time = time.time()
        start_cpu = psutil.cpu_percent(interval=1)
        start_memory = psutil.virtual_memory().percent
        
        # 启动爬虫
        process = subprocess.Popen([
            'python', 'poi_crawler_simple.py', config['file']
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        
        # 监控进程
        cpu_samples = []
        memory_samples = []
        
        while process.poll() is None:
            try:
                proc = psutil.Process(process.pid)
                cpu_samples.append(proc.cpu_percent(interval=1))
                memory_samples.append(proc.memory_info().rss / 1024 / 1024)  # MB
            except:
                pass
            time.sleep(5)
        
        # 记录结束状态
        end_time = time.time()
        duration = end_time - start_time
        
        # 收集结果
        result = {
            'test_name': config['name'],
            'duration_seconds': duration,
            'expected_seconds': config['expected_time'],
            'performance_ratio': duration / config['expected_time'],
            'avg_cpu': sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
            'max_memory_mb': max(memory_samples) if memory_samples else 0,
            'status': 'PASS' if duration <= config['expected_time'] else 'SLOW'
        }
        
        results.append(result)
        
        # 输出结果
        print(f"\\n  测试结果: {result['status']}")
        print(f"  实际用时: {duration:.1f}秒 (预期: {config['expected_time']}秒)")
        print(f"  平均CPU: {result['avg_cpu']:.1f}%")
        print(f"  最大内存: {result['max_memory_mb']:.1f}MB")
        
        if result['status'] == 'SLOW':
            print(f"  ⚠️  性能低于预期 {result['performance_ratio']:.1f}倍")
    
    # 保存测试结果
    output_file = Path('performance_analysis') / f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\\n✅ 测试完成，结果已保存到: {output_file}")
    
    # 性能诊断
    print("\\n🔍 性能诊断:")
    slow_tests = [r for r in results if r['status'] == 'SLOW']
    if slow_tests:
        print("  发现性能问题:")
        for test in slow_tests:
            print(f"  - {test['test_name']}: 比预期慢{test['performance_ratio']:.1f}倍")
        print("\\n  可能的原因:")
        print("  1. 网络延迟增加")
        print("  2. 重试任务过多")
        print("  3. 页面加载超时")
        print("  4. 资源竞争（CPU/内存）")
    else:
        print("  性能符合预期")

if __name__ == "__main__":
    run_performance_test()
"""
        
        script_path = self.analysis_dir / "performance_test.py"
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(test_script)
            
        print(f"✅ 性能测试脚本已创建: {script_path}")
        
    def generate_all_tools(self):
        """生成所有分析工具"""
        print("🛠️  生成性能分析工具集...")
        
        self.analyze_queue_status()
        self.analyze_task_timing()
        self.create_worker_monitor()
        self.create_retry_analyzer()
        self.create_performance_test()
        
        # 创建使用说明
        readme_content = """# POI爬虫性能分析工具集

## 🎯 目的
识别POI爬虫长时间运行后CPU和内存利用率下降的原因

## 🛠️ 工具列表

### 1. queue_monitor.py - 任务队列监控
手动记录任务处理进度，分析队列状态

### 2. analyze_timing.py - 任务时间分析
分析每个任务的处理时间，找出慢任务

### 3. worker_monitor.py - Worker线程监控
实时监控Worker线程的CPU使用率和状态

### 4. retry_analyzer.py - 重试任务分析
统计重试任务的比例和特征

### 5. performance_test.py - 性能基准测试
运行标准测试用例，对比性能

## 📋 使用步骤

1. **准备测试数据**
   ```bash
   # 创建小批量测试文件（10条数据）
   head -n 11 data/input/你的文件.csv > data/input/test_small.csv
   
   # 创建中等批量测试文件（50条数据）
   head -n 51 data/input/你的文件.csv > data/input/test_medium.csv
   ```

2. **运行爬虫并记录输出**
   ```bash
   python poi_crawler_simple.py --all > crawler_output.log 2>&1
   ```

3. **同时运行监控工具**
   ```bash
   # 在另一个终端运行Worker监控
   python performance_analysis/worker_monitor.py
   ```

4. **分析结果**
   ```bash
   # 分析重试模式
   python performance_analysis/retry_analyzer.py crawler_output.log
   
   # 运行性能测试
   python performance_analysis/performance_test.py
   ```

## 🔍 重点关注

1. **CPU使用率下降时间点** - 记录何时开始出现低CPU
2. **重试任务比例变化** - 观察重试是否逐渐增多
3. **单任务处理时间** - 是否有任务特别慢
4. **Worker线程状态** - 是否大部分时间在等待

## 📊 预期发现

- 任务队列后期以重试任务为主
- Worker线程大部分时间在等待网页加载
- 某些特定地址导致处理时间过长
- Chrome进程资源占用逐渐增加
"""
        
        readme_path = self.analysis_dir / "README.md"
        with open(readme_path, 'w', encoding='utf-8') as f:
            f.write(readme_content)
            
        print(f"\n✅ 所有分析工具已生成到: {self.analysis_dir}")
        print(f"📖 查看使用说明: {readme_path}")


if __name__ == "__main__":
    analyzer = CrawlerPerformanceAnalyzer()
    analyzer.generate_all_tools()