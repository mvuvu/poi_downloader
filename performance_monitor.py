#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
性能监控脚本 - 用于分析POI爬虫运行时的性能问题
"""

import time
import threading
import psutil
import pandas as pd
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import argparse

class PerformanceMonitor:
    """POI爬虫性能监控器"""
    
    def __init__(self, output_dir="performance_logs", interval=30):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.interval = interval  # 监控间隔（秒）
        self.start_time = time.time()
        self.monitoring = False
        self.monitor_thread = None
        
        # 性能数据
        self.performance_data = []
        self.task_stats = []
        
        # 创建日志文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.perf_log = self.output_dir / f"performance_{timestamp}.csv"
        self.task_log = self.output_dir / f"task_stats_{timestamp}.json"
        
    def start(self):
        """启动监控"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()
        print(f"🚀 性能监控已启动，数据将保存到: {self.output_dir}")
        
    def stop(self):
        """停止监控"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join()
        self._save_data()
        print(f"📊 性能监控已停止，数据已保存")
        
    def _monitor_loop(self):
        """监控主循环"""
        while self.monitoring:
            try:
                # 收集系统性能数据
                perf_data = self._collect_performance_data()
                self.performance_data.append(perf_data)
                
                # 收集任务统计数据
                task_data = self._collect_task_stats()
                if task_data:
                    self.task_stats.append(task_data)
                
                # 实时输出关键指标
                self._print_realtime_stats(perf_data, task_data)
                
                # 定期保存数据
                if len(self.performance_data) % 10 == 0:
                    self._save_data()
                
                time.sleep(self.interval)
                
            except Exception as e:
                print(f"❌ 监控错误: {e}")
                
    def _collect_performance_data(self):
        """收集系统性能数据"""
        # 获取POI爬虫进程
        crawler_processes = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if 'poi_crawler_simple.py' in ' '.join(proc.info['cmdline'] or []):
                    crawler_processes.append(proc)
            except:
                continue
        
        # 收集数据
        data = {
            'timestamp': datetime.now().isoformat(),
            'elapsed_time': time.time() - self.start_time,
            'cpu_percent': psutil.cpu_percent(interval=1),
            'memory_percent': psutil.virtual_memory().percent,
            'memory_used_gb': psutil.virtual_memory().used / (1024**3),
            'crawler_count': len(crawler_processes),
            'crawler_cpu': 0,
            'crawler_memory_mb': 0,
            'chrome_processes': 0,
            'chrome_memory_mb': 0
        }
        
        # 爬虫进程统计
        for proc in crawler_processes:
            try:
                data['crawler_cpu'] += proc.cpu_percent()
                data['crawler_memory_mb'] += proc.memory_info().rss / (1024**2)
            except:
                pass
        
        # Chrome进程统计
        for proc in psutil.process_iter(['name', 'memory_info']):
            try:
                if 'chrome' in proc.info['name'].lower():
                    data['chrome_processes'] += 1
                    data['chrome_memory_mb'] += proc.memory_info().rss / (1024**2)
            except:
                pass
        
        return data
        
    def _collect_task_stats(self):
        """收集任务统计数据（从进度文件读取）"""
        progress_dir = Path("data/progress")
        if not progress_dir.exists():
            return None
            
        stats = {
            'timestamp': datetime.now().isoformat(),
            'files': []
        }
        
        # 读取所有进度文件
        for progress_file in progress_dir.glob("*_simple_progress.json"):
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    progress = json.load(f)
                    
                # 计算任务速度
                if 'last_updated' in progress and 'timestamp' in progress:
                    duration = progress['last_updated'] - progress['timestamp']
                    speed = progress['processed_tasks'] / duration if duration > 0 else 0
                else:
                    speed = 0
                    
                file_stats = {
                    'file_name': progress['file_name'],
                    'total_tasks': progress.get('total_tasks', 0),
                    'processed_tasks': progress.get('processed_tasks', 0),
                    'success_count': progress.get('success_count', 0),
                    'error_count': progress.get('error_count', 0),
                    'progress_percent': (progress['processed_tasks'] / progress['total_tasks'] * 100) if progress.get('total_tasks', 0) > 0 else 0,
                    'success_rate': (progress['success_count'] / progress['processed_tasks'] * 100) if progress.get('processed_tasks', 0) > 0 else 0,
                    'speed_per_second': speed
                }
                stats['files'].append(file_stats)
                
            except Exception as e:
                continue
                
        # 计算总体统计
        if stats['files']:
            stats['total_processed'] = sum(f['processed_tasks'] for f in stats['files'])
            stats['total_success'] = sum(f['success_count'] for f in stats['files'])
            stats['total_errors'] = sum(f['error_count'] for f in stats['files'])
            stats['avg_success_rate'] = (stats['total_success'] / stats['total_processed'] * 100) if stats['total_processed'] > 0 else 0
            stats['avg_speed'] = sum(f['speed_per_second'] for f in stats['files']) / len(stats['files'])
        
        return stats
        
    def _print_realtime_stats(self, perf_data, task_data):
        """打印实时统计信息"""
        elapsed_min = perf_data['elapsed_time'] / 60
        
        print(f"\n⏱️  运行时间: {elapsed_min:.1f} 分钟")
        print(f"💻 系统状态: CPU {perf_data['cpu_percent']:.1f}% | 内存 {perf_data['memory_percent']:.1f}% ({perf_data['memory_used_gb']:.1f}GB)")
        print(f"🕷️  爬虫进程: {perf_data['crawler_count']} 个 | CPU {perf_data['crawler_cpu']:.1f}% | 内存 {perf_data['crawler_memory_mb']:.0f}MB")
        print(f"🌐 Chrome进程: {perf_data['chrome_processes']} 个 | 内存 {perf_data['chrome_memory_mb']:.0f}MB")
        
        if task_data and 'total_processed' in task_data:
            print(f"📊 任务统计: 已处理 {task_data['total_processed']} | 成功率 {task_data['avg_success_rate']:.1f}% | 速度 {task_data['avg_speed']:.2f}/秒")
            
            # 检测性能下降
            if elapsed_min > 30:  # 运行超过30分钟后开始检测
                if perf_data['cpu_percent'] < 20 and perf_data['crawler_cpu'] < 50:
                    print("⚠️  警告: CPU利用率过低，可能存在性能瓶颈")
                if task_data['avg_speed'] < 0.1:  # 每秒处理少于0.1个任务
                    print("⚠️  警告: 任务处理速度过慢")
                    
    def _save_data(self):
        """保存监控数据"""
        # 保存性能数据
        if self.performance_data:
            df = pd.DataFrame(self.performance_data)
            df.to_csv(self.perf_log, index=False)
            
        # 保存任务统计
        if self.task_stats:
            with open(self.task_log, 'w', encoding='utf-8') as f:
                json.dump(self.task_stats, f, ensure_ascii=False, indent=2)
                
    def analyze_performance_degradation(self):
        """分析性能下降原因"""
        if not self.performance_data:
            print("❌ 没有足够的性能数据进行分析")
            return
            
        df = pd.DataFrame(self.performance_data)
        
        print("\n📈 性能趋势分析:")
        
        # CPU使用率趋势
        cpu_start = df['crawler_cpu'].iloc[:10].mean()
        cpu_end = df['crawler_cpu'].iloc[-10:].mean()
        cpu_change = ((cpu_end - cpu_start) / cpu_start * 100) if cpu_start > 0 else 0
        print(f"   CPU使用率: {cpu_start:.1f}% → {cpu_end:.1f}% (变化: {cpu_change:+.1f}%)")
        
        # 内存使用趋势
        mem_start = df['crawler_memory_mb'].iloc[:10].mean()
        mem_end = df['crawler_memory_mb'].iloc[-10:].mean()
        mem_change = ((mem_end - mem_start) / mem_start * 100) if mem_start > 0 else 0
        print(f"   内存使用: {mem_start:.0f}MB → {mem_end:.0f}MB (变化: {mem_change:+.1f}%)")
        
        # Chrome进程数趋势
        chrome_start = df['chrome_processes'].iloc[:10].mean()
        chrome_end = df['chrome_processes'].iloc[-10:].mean()
        print(f"   Chrome进程数: {chrome_start:.0f} → {chrome_end:.0f}")
        
        # 任务处理速度分析
        if self.task_stats and len(self.task_stats) > 10:
            speeds = [s.get('avg_speed', 0) for s in self.task_stats]
            speed_start = sum(speeds[:5]) / 5
            speed_end = sum(speeds[-5:]) / 5
            speed_change = ((speed_end - speed_start) / speed_start * 100) if speed_start > 0 else 0
            print(f"   处理速度: {speed_start:.2f}/秒 → {speed_end:.2f}/秒 (变化: {speed_change:+.1f}%)")
            
            # 分析重试任务占比（需要额外数据支持）
            print("\n🔍 可能的原因:")
            if cpu_change < -50:
                print("   - CPU使用率大幅下降，可能是任务队列枯竭或等待时间过长")
            if speed_change < -50:
                print("   - 处理速度显著下降，可能是遇到复杂任务或重试任务增多")
            if mem_change > 100:
                print("   - 内存使用大幅增加，可能存在内存泄漏")
            if chrome_end > chrome_start * 1.5:
                print("   - Chrome进程数增加，可能是进程清理不及时")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='POI爬虫性能监控工具')
    parser.add_argument('--interval', type=int, default=30, help='监控间隔（秒）')
    parser.add_argument('--analyze', action='store_true', help='分析已有的性能数据')
    parser.add_argument('--output', default='performance_logs', help='输出目录')
    
    args = parser.parse_args()
    
    monitor = PerformanceMonitor(output_dir=args.output, interval=args.interval)
    
    if args.analyze:
        # 加载并分析已有数据
        print("📊 正在分析性能数据...")
        # 这里需要实现加载已有数据的逻辑
        monitor.analyze_performance_degradation()
    else:
        # 实时监控
        try:
            monitor.start()
            print("🎯 性能监控运行中... 按 Ctrl+C 停止")
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n⏹️  正在停止监控...")
            monitor.stop()
            monitor.analyze_performance_degradation()


if __name__ == "__main__":
    main()