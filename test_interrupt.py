#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
测试Ctrl+C中断处理的脚本
"""

import time
import pandas as pd
from pathlib import Path
from poi_crawler_simple import SimplePOICrawler

def create_test_csv():
    """创建一个测试CSV文件"""
    test_data = {
        'District': ['测试区'] * 10,
        'Latitude': [35.6895 + i*0.001 for i in range(10)],
        'Longitude': [139.6917 + i*0.001 for i in range(10)],
        'Address': [f'东京都测试区测试地址{i+1}丁目' for i in range(10)]
    }
    
    df = pd.DataFrame(test_data)
    test_file = 'data/input/test_interrupt.csv'
    Path(test_file).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(test_file, index=False, encoding='utf-8-sig')
    print(f"✅ 创建测试文件: {test_file}")
    return test_file

def main():
    print("🧪 测试Ctrl+C中断处理")
    print("⚠️  请在程序运行时按 Ctrl+C 来测试中断处理")
    print("📝 注意观察程序是否能够安全快速退出，不继续更新文件")
    print("="*60)
    
    # 创建测试文件
    test_file = create_test_csv()
    
    # 创建爬虫实例
    crawler = SimplePOICrawler(
        num_workers=2,  # 少一些工作线程便于测试
        batch_size=5,
        verbose=True,
        show_progress=True
    )
    
    # 开始爬取
    output_file = "data/output/test_interrupt_output.csv"
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)
    
    try:
        crawler.crawl_from_csv(test_file, output_file)
    except KeyboardInterrupt:
        print("\n🚨 主程序收到中断信号")
    
    print("\n🏁 测试完成")
    
    # 清理测试文件
    try:
        Path(test_file).unlink()
        print(f"🧹 清理测试输入文件: {test_file}")
    except:
        pass

if __name__ == "__main__":
    main()