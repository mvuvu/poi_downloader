#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单文件选择器 - 无交互式命令
自动选择最大的CSV文件或使用默认文件
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Tuple
import glob

class SimpleFileSelector:
    """简单文件选择器 - 无需用户交互"""
    
    def __init__(self, input_dir: str = "data/input", output_dir: str = "data/output"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        
        # 确保目录存在
        self.input_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def auto_select_input_file(self) -> Optional[str]:
        """自动选择输入文件 - 优先选择最大的有效CSV文件"""
        
        # 扫描CSV文件
        csv_files = []
        
        for csv_path in self.input_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_path)
                row_count = len(df)
                
                # 检查是否有Address列
                has_address = 'Address' in df.columns
                
                if has_address and row_count > 0:
                    csv_files.append((str(csv_path), row_count))
                    print(f"✅ 发现有效文件: {csv_path.name} ({row_count:,} 行)")
                else:
                    print(f"⚠️ 跳过无效文件: {csv_path.name} (无Address列或为空)")
                    
            except Exception as e:
                print(f"❌ 读取失败: {csv_path.name} - {e}")
        
        if not csv_files:
            print("❌ 未找到有效的CSV文件")
            return None
        
        # 按行数排序，选择最大的文件
        csv_files.sort(key=lambda x: x[1], reverse=True)
        selected_file = csv_files[0][0]
        
        print(f"🎯 自动选择: {Path(selected_file).name} ({csv_files[0][1]:,} 行)")
        return selected_file
    
    def generate_output_filename(self, input_file: str, suffix: str = "poi_results") -> str:
        """生成输出文件名"""
        if not input_file:
            return str(self.output_dir / f"{suffix}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M')}.csv")
        
        input_path = Path(input_file)
        base_name = input_path.stem
        
        # 清理基础名称
        base_name = base_name.replace('_complete', '').replace('_input', '').replace('_addresses', '')
        
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        output_filename = f"{base_name}_{suffix}_{timestamp}.csv"
        
        return str(self.output_dir / output_filename)
    
    def get_files(self, suffix: str = "poi_results") -> Tuple[Optional[str], str]:
        """获取输入和输出文件路径"""
        input_file = self.auto_select_input_file()
        output_file = self.generate_output_filename(input_file, suffix)
        
        print(f"📋 文件配置:")
        print(f"  📥 输入: {input_file or '未找到'}")
        print(f"  📤 输出: {output_file}")
        
        return input_file, output_file

def get_simple_file_config(input_dir: str = "data/input", output_dir: str = "data/output", suffix: str = "poi_results") -> dict:
    """获取简单的文件配置"""
    selector = SimpleFileSelector(input_dir, output_dir)
    input_file, output_file = selector.get_files(suffix)
    
    return {
        'input_file': input_file,
        'output_file': output_file,
        'has_input': input_file is not None
    }

def test_selector():
    """测试选择器"""
    print("🧪 测试简单文件选择器")
    print("=" * 40)
    
    config = get_simple_file_config()
    
    if config['has_input']:
        print("✅ 文件选择成功")
    else:
        print("❌ 文件选择失败")
    
    return config

if __name__ == "__main__":
    test_selector()