#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
验证POI爬虫性能下降原因的自动化脚本
通过分析现有数据和日志来识别问题
"""

import json
import os
import re
import glob
from pathlib import Path
from datetime import datetime
import pandas as pd
import time

class PerformanceVerifier:
    """性能问题验证器"""
    
    def __init__(self):
        self.results = {
            'timestamp': datetime.now().isoformat(),
            'findings': [],
            'recommendations': []
        }
        
    def verify_progress_files(self):
        """验证进度文件，分析任务处理速度"""
        print("\n📊 分析进度文件...")
        
        progress_dir = Path("data/progress")
        if not progress_dir.exists():
            print("❌ 进度目录不存在")
            return
            
        progress_files = list(progress_dir.glob("*_simple_progress.json"))
        
        if not progress_files:
            print("❌ 没有找到进度文件")
            return
            
        analysis = []
        
        for pf in progress_files:
            try:
                with open(pf, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # 计算处理速度
                if 'timestamp' in data and 'last_updated' in data:
                    duration = data['last_updated'] - data['timestamp']
                    if duration > 0:
                        speed = data['processed_tasks'] / duration
                        
                        analysis.append({
                            'file': data['file_name'],
                            'total_tasks': data['total_tasks'],
                            'processed': data['processed_tasks'],
                            'success_rate': (data['success_count'] / data['processed_tasks'] * 100) if data['processed_tasks'] > 0 else 0,
                            'speed_per_second': speed,
                            'duration_minutes': duration / 60
                        })
                        
            except Exception as e:
                print(f"⚠️  读取文件失败 {pf}: {e}")
                
        if analysis:
            # 分析结果
            df = pd.DataFrame(analysis)
            
            print(f"\n✅ 分析了 {len(analysis)} 个文件的进度")
            print(f"📈 平均处理速度: {df['speed_per_second'].mean():.3f} 任务/秒")
            print(f"📊 速度范围: {df['speed_per_second'].min():.3f} - {df['speed_per_second'].max():.3f} 任务/秒")
            print(f"✅ 平均成功率: {df['success_rate'].mean():.1f}%")
            
            # 检测性能问题
            slow_files = df[df['speed_per_second'] < 0.1]
            if len(slow_files) > 0:
                print(f"\n⚠️  发现 {len(slow_files)} 个处理缓慢的文件 (< 0.1 任务/秒):")
                for _, row in slow_files.iterrows():
                    print(f"   - {row['file']}: {row['speed_per_second']:.3f} 任务/秒")
                    
                self.results['findings'].append({
                    'issue': '任务处理速度过慢',
                    'detail': f'{len(slow_files)} 个文件处理速度低于 0.1 任务/秒',
                    'severity': 'high'
                })
                
            # 长时间运行的文件
            long_running = df[df['duration_minutes'] > 120]  # 超过2小时
            if len(long_running) > 0:
                print(f"\n⏱️  发现 {len(long_running)} 个长时间运行的文件 (> 2小时):")
                for _, row in long_running.iterrows():
                    print(f"   - {row['file']}: {row['duration_minutes']:.1f} 分钟")
                    
            return df
        
    def analyze_output_files(self):
        """分析输出文件，查看POI数量分布"""
        print("\n📁 分析输出文件...")
        
        output_dir = Path("data/output")
        if not output_dir.exists():
            print("❌ 输出目录不存在")
            return
            
        csv_files = list(output_dir.glob("*.csv"))
        
        if not csv_files:
            print("❌ 没有找到输出文件")
            return
            
        poi_stats = []
        
        for cf in csv_files[:10]:  # 只分析前10个文件
            try:
                df = pd.read_csv(cf, encoding='utf-8-sig')
                if not df.empty:
                    # 统计POI分布
                    poi_counts = df.groupby('blt_name').size()
                    
                    poi_stats.append({
                        'file': cf.name,
                        'total_pois': len(df),
                        'unique_buildings': len(poi_counts),
                        'avg_pois_per_building': poi_counts.mean(),
                        'max_pois': poi_counts.max()
                    })
                    
            except Exception as e:
                continue
                
        if poi_stats:
            df_stats = pd.DataFrame(poi_stats)
            
            print(f"\n✅ 分析了 {len(poi_stats)} 个输出文件")
            print(f"📊 平均POI数/文件: {df_stats['total_pois'].mean():.1f}")
            print(f"🏢 平均建筑物数/文件: {df_stats['unique_buildings'].mean():.1f}")
            print(f"📍 平均POI数/建筑物: {df_stats['avg_pois_per_building'].mean():.1f}")
            
            # 检测高POI建筑物
            high_poi_files = df_stats[df_stats['max_pois'] > 100]
            if len(high_poi_files) > 0:
                print(f"\n⚠️  发现含有大量POI的建筑物 (> 100 POIs):")
                for _, row in high_poi_files.iterrows():
                    print(f"   - {row['file']}: 最大 {row['max_pois']} POIs")
                    
                self.results['findings'].append({
                    'issue': '存在高POI密度建筑物',
                    'detail': f'部分建筑物包含超过100个POI，会导致滚动时间过长',
                    'severity': 'medium'
                })
                
    def check_system_resources(self):
        """检查系统资源使用情况"""
        print("\n💻 检查系统资源...")
        
        try:
            import psutil
            
            # CPU使用率
            cpu_percent = psutil.cpu_percent(interval=2)
            print(f"CPU使用率: {cpu_percent}%")
            
            # 内存使用
            memory = psutil.virtual_memory()
            print(f"内存使用: {memory.percent}% ({memory.used / (1024**3):.1f}GB / {memory.total / (1024**3):.1f}GB)")
            
            # Chrome进程
            chrome_count = 0
            chrome_memory = 0
            
            for proc in psutil.process_iter(['name', 'memory_info']):
                try:
                    if 'chrome' in proc.info['name'].lower():
                        chrome_count += 1
                        chrome_memory += proc.memory_info().rss / (1024**2)
                except:
                    pass
                    
            print(f"Chrome进程: {chrome_count} 个，总内存: {chrome_memory:.0f}MB")
            
            if chrome_count > 20:
                self.results['findings'].append({
                    'issue': 'Chrome进程过多',
                    'detail': f'发现 {chrome_count} 个Chrome进程，可能存在进程泄漏',
                    'severity': 'high'
                })
                
            if chrome_memory > 4000:  # 4GB
                self.results['findings'].append({
                    'issue': 'Chrome内存占用过高',
                    'detail': f'Chrome总内存占用 {chrome_memory:.0f}MB',
                    'severity': 'high'
                })
                
        except ImportError:
            print("⚠️  psutil未安装，跳过系统资源检查")
            
    def analyze_warnings(self):
        """分析警告文件"""
        print("\n⚠️  分析警告文件...")
        
        warning_dirs = [
            "data/warnings",
            "data/no_poi_warnings",
            "data/non_building_warnings"
        ]
        
        warning_stats = {}
        
        for wd in warning_dirs:
            wd_path = Path(wd)
            if wd_path.exists():
                files = list(wd_path.glob("*.json"))
                if files:
                    total_warnings = 0
                    for wf in files:
                        try:
                            with open(wf, 'r', encoding='utf-8') as f:
                                warnings = json.load(f)
                                total_warnings += len(warnings)
                        except:
                            pass
                            
                    warning_stats[wd] = total_warnings
                    print(f"   {wd}: {total_warnings} 条警告")
                    
        if sum(warning_stats.values()) > 1000:
            self.results['findings'].append({
                'issue': '大量警告产生',
                'detail': f'总警告数: {sum(warning_stats.values())}',
                'severity': 'medium'
            })
            
    def check_retry_patterns(self):
        """检查重试模式（通过模拟）"""
        print("\n🔄 分析重试模式...")
        
        # 由于无法直接访问运行时数据，我们通过进度文件推断
        progress_dir = Path("data/progress")
        if progress_dir.exists():
            progress_files = list(progress_dir.glob("*_simple_progress.json"))
            
            for pf in progress_files:
                try:
                    with open(pf, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        
                    # 检查错误率
                    if data.get('processed_tasks', 0) > 0:
                        error_rate = data.get('error_count', 0) / data['processed_tasks'] * 100
                        
                        if error_rate > 20:
                            print(f"   ⚠️  {data['file_name']}: 错误率 {error_rate:.1f}%")
                            
                            self.results['findings'].append({
                                'issue': '高错误率',
                                'detail': f'{data["file_name"]} 错误率达到 {error_rate:.1f}%',
                                'severity': 'high'
                            })
                            
                except:
                    pass
                    
    def generate_report(self):
        """生成验证报告"""
        print("\n" + "="*60)
        print("📋 性能问题验证报告")
        print("="*60)
        
        if not self.results['findings']:
            print("\n✅ 未发现明显的性能问题")
        else:
            print(f"\n🔍 发现 {len(self.results['findings'])} 个问题:\n")
            
            # 按严重程度排序
            high_severity = [f for f in self.results['findings'] if f['severity'] == 'high']
            medium_severity = [f for f in self.results['findings'] if f['severity'] == 'medium']
            
            if high_severity:
                print("🔴 高严重度问题:")
                for f in high_severity:
                    print(f"   - {f['issue']}: {f['detail']}")
                    
            if medium_severity:
                print("\n🟡 中等严重度问题:")
                for f in medium_severity:
                    print(f"   - {f['issue']}: {f['detail']}")
                    
        # 最可能的原因
        print("\n🎯 最可能的性能下降原因:")
        
        findings_text = str(self.results['findings'])
        
        if '任务处理速度过慢' in findings_text:
            print("1. ⏱️  任务处理时间增加")
            print("   - 原因: 重试任务累积，每个重试需要完整的滚动流程")
            print("   - 影响: CPU空闲等待网页加载")
            
        if 'Chrome进程过多' in findings_text or 'Chrome内存占用过高' in findings_text:
            print("2. 🌐 Chrome资源问题")
            print("   - 原因: 长时间运行导致内存泄漏")
            print("   - 影响: 系统资源耗尽，处理变慢")
            
        if '高错误率' in findings_text:
            print("3. 🔄 重试任务堆积")
            print("   - 原因: 错误和非建筑物地址触发大量重试")
            print("   - 影响: 重试队列占用Worker，正常任务等待")
            
        if '高POI密度建筑物' in findings_text:
            print("4. 📍 复杂任务增多")
            print("   - 原因: 后期遇到POI密集的大型建筑")
            print("   - 影响: 单任务滚动时间可达3-4分钟")
            
        # 通用原因
        print("\n5. 🔧 系统性瓶颈")
        print("   - 同步等待累积: WebDriverWait(10秒) + 滚动等待(2秒×N次)")
        print("   - 任务分布不均: 简单任务先完成，剩余都是困难任务")
        print("   - 网络限流: Google Maps可能对频繁请求降速")
        
        # 保存报告
        report_path = Path("performance_analysis") / f"verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path.parent.mkdir(exist_ok=True)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
            
        print(f"\n📄 详细报告已保存到: {report_path}")
        
    def run_verification(self):
        """运行完整验证"""
        print("🚀 开始验证POI爬虫性能问题...")
        print(f"⏰ 时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 执行各项检查
        self.verify_progress_files()
        self.analyze_output_files()
        self.check_system_resources()
        self.analyze_warnings()
        self.check_retry_patterns()
        
        # 生成报告
        self.generate_report()


if __name__ == "__main__":
    verifier = PerformanceVerifier()
    verifier.run_verification()