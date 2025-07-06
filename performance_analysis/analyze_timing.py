
import re
from datetime import datetime
from pathlib import Path
import json

def analyze_log_timing(log_file):
    """分析日志中的时间信息"""
    
    # 时间模式匹配
    patterns = {
        'task_start': r'🔍 处理地址: (.+)',
        'task_complete': r'✅ (.+) \| POI: (\d+) \| 状态: 已保存',
        'task_retry': r'🔄 非建筑物，使用日文地址重试: (.+)',
        'chrome_restart': r'🔄 Worker (\d+): 达到1000个任务，重启Chrome驱动',
        'error': r'❌ (.+) \| 错误: (.+)'
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
