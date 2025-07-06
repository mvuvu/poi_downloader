
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POI爬虫性能测试脚本
用于识别性能瓶颈
"""

import subprocess
import time
import psutil
import json
from datetime import datetime
from pathlib import Path

def run_performance_test():
    """运行性能测试"""
    
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
        print(f"\n📊 运行测试: {config['name']}")
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
        print(f"\n  测试结果: {result['status']}")
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
    
    print(f"\n✅ 测试完成，结果已保存到: {output_file}")
    
    # 性能诊断
    print("\n🔍 性能诊断:")
    slow_tests = [r for r in results if r['status'] == 'SLOW']
    if slow_tests:
        print("  发现性能问题:")
        for test in slow_tests:
            print(f"  - {test['test_name']}: 比预期慢{test['performance_ratio']:.1f}倍")
        print("\n  可能的原因:")
        print("  1. 网络延迟增加")
        print("  2. 重试任务过多")
        print("  3. 页面加载超时")
        print("  4. 资源竞争（CPU/内存）")
    else:
        print("  性能符合预期")

if __name__ == "__main__":
    run_performance_test()
