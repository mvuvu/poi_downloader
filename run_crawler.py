#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POI爬虫快速启动脚本
支持交互式文件选择和多种运行模式
"""

import sys
import os
from pathlib import Path

def main():
    """主函数"""
    print("🎯 POI爬虫快速启动器")
    print("=" * 50)
    
    print("📋 选择运行模式:")
    print("1. 🖱️  交互式文件选择模式")
    print("2. ⌨️  命令行参数模式") 
    print("3. 📓 Jupyter Notebook模式")
    print("4. 🔧 测试模式 (前5个地址)")
    
    try:
        mode = input("\n请选择模式 (1-4, 默认1): ").strip()
        
        if not mode:
            mode = '1'
        
        if mode == '1':
            # 交互式文件选择模式
            print("\n🚀 启动交互式文件选择...")
            from final_crawler import main
            main()
            
        elif mode == '2':
            # 命令行参数模式
            print("\n📋 命令行参数模式:")
            print("可用参数:")
            print("  --input, -i      输入CSV文件路径")
            print("  --output, -o     输出CSV文件路径")
            print("  --workers, -w    并发线程数 (默认: 4)")
            print("  --headless       无头模式运行")
            print("  --no-headless    显示Chrome窗口")
            print("  --interactive    交互式文件选择")
            
            print(f"\n示例命令:")
            print(f"python final_crawler.py --input data/input/your_file.csv --workers 2")
            print(f"python final_crawler.py --interactive")
            
            choice = input("\n直接运行交互模式？(y/n): ").lower()
            if choice == 'y':
                os.system("python final_crawler.py --interactive")
            else:
                print("请使用上述命令行参数运行")
                
        elif mode == '3':
            # Jupyter Notebook模式
            print("\n📓 启动Jupyter Notebook...")
            
            try:
                import subprocess
                subprocess.run(["jupyter", "notebook", "enhanced_poi_crawler.ipynb"])
            except FileNotFoundError:
                print("❌ Jupyter未安装，请先安装: pip install jupyter")
                print("或手动打开文件: enhanced_poi_crawler.ipynb")
            except Exception as e:
                print(f"❌ 启动Jupyter失败: {e}")
                print("请手动打开文件: enhanced_poi_crawler.ipynb")
                
        elif mode == '4':
            # 测试模式
            print("\n🧪 启动测试模式...")
            run_test_mode()
            
        else:
            print("❌ 无效选择")
            
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
    except Exception as e:
        print(f"❌ 运行失败: {e}")

def run_test_mode():
    """测试模式 - 处理前5个地址"""
    from file_selector import FileSelector
    from final_crawler import FinalPOICrawler
    import pandas as pd
    import time
    
    print("🧪 测试模式 - 处理前5个地址")
    
    # 文件选择
    selector = FileSelector()
    csv_files = selector.scan_csv_files()
    
    if not csv_files:
        print("❌ 未找到CSV文件")
        return
    
    print(f"📋 使用文件: {csv_files[0][0]}")
    input_file = f"data/input/{csv_files[0][0]}"
    output_file = "test_poi_results.csv"
    
    # 读取数据
    try:
        df = pd.read_csv(input_file)
        addresses = df['Address'].dropna().tolist()[:5]
        
        print(f"📊 测试地址:")
        for i, addr in enumerate(addresses, 1):
            print(f"  {i}. {addr}")
        
        # 测试配置
        config = {
            'max_workers': 2,
            'driver_pool_size': 2,
            'batch_size': 5,
            'timeout': 15,
            'retry_times': 2,
            'headless': True,
            'checkpoint_interval': 5,
            'input_file': input_file,
            'output_file': output_file
        }
        
        print(f"\n🚀 开始测试爬取...")
        start_time = time.time()
        
        crawler = FinalPOICrawler(config)
        try:
            crawler.process_addresses(addresses)
            
            elapsed = time.time() - start_time
            print(f"\n✅ 测试完成！")
            print(f"⏱️ 耗时: {elapsed:.1f} 秒")
            print(f"📈 平均: {elapsed/len(addresses):.1f} 秒/地址")
            
            # 查看结果
            if Path(output_file).exists():
                results = pd.read_csv(output_file)
                print(f"📊 获得 {len(results)} 个POI")
                print(f"📁 结果文件: {output_file}")
            
        finally:
            crawler.close()
            
    except Exception as e:
        print(f"❌ 测试失败: {e}")

if __name__ == "__main__":
    main()