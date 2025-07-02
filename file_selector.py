#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件选择器模块
提供交互式文件选择功能，支持GUI和命令行两种模式
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Optional, List, Tuple

class FileSelector:
    """文件选择器类"""
    
    def __init__(self, default_input_dir: str = "data/input"):
        self.default_input_dir = Path(default_input_dir)
        self.default_output_dir = Path("data/output")
        
        # 确保目录存在
        self.default_input_dir.mkdir(parents=True, exist_ok=True)
        self.default_output_dir.mkdir(parents=True, exist_ok=True)
    
    def scan_csv_files(self, directory: str = None) -> List[Tuple[str, str, int]]:
        """扫描目录中的CSV文件"""
        if directory is None:
            directory = self.default_input_dir
        else:
            directory = Path(directory)
        
        csv_files = []
        
        if not directory.exists():
            print(f"❌ 目录不存在: {directory}")
            return csv_files
        
        try:
            for file_path in directory.glob("*.csv"):
                try:
                    # 尝试读取文件获取基本信息
                    df = pd.read_csv(file_path)
                    row_count = len(df)
                    
                    # 检查是否包含Address列
                    has_address = 'Address' in df.columns
                    status = "✅" if has_address else "⚠️"
                    
                    csv_files.append((
                        str(file_path.name),
                        f"{status} {row_count:,} 行, 列: {list(df.columns)}",
                        row_count
                    ))
                    
                except Exception as e:
                    csv_files.append((
                        str(file_path.name),
                        f"❌ 读取失败: {str(e)[:50]}...",
                        0
                    ))
        
        except Exception as e:
            print(f"❌ 扫描目录失败: {e}")
        
        return sorted(csv_files, key=lambda x: x[2], reverse=True)  # 按行数排序
    
    def select_input_file_interactive(self) -> Optional[str]:
        """交互式选择输入文件"""
        print("📂 文件选择器")
        print("=" * 50)
        
        # 扫描默认目录
        csv_files = self.scan_csv_files()
        
        if not csv_files:
            print(f"❌ 在 {self.default_input_dir} 目录中未找到CSV文件")
            
            # 询问是否选择其他目录
            choice = input("\n是否选择其他目录？(y/n): ").lower()
            if choice == 'y':
                custom_dir = input("请输入目录路径: ").strip()
                if custom_dir:
                    csv_files = self.scan_csv_files(custom_dir)
                    if not csv_files:
                        print("❌ 指定目录中也未找到CSV文件")
                        return None
                else:
                    return None
            else:
                return None
        
        print(f"\n📋 发现 {len(csv_files)} 个CSV文件:")
        print("-" * 70)
        
        # 显示文件列表
        for i, (filename, info, _) in enumerate(csv_files, 1):
            print(f"{i:2d}. {filename}")
            print(f"    {info}")
            print()
        
        # 添加手动输入选项
        print(f"{len(csv_files) + 1:2d}. 📝 手动输入文件路径")
        print(f"{len(csv_files) + 2:2d}. ❌ 取消")
        
        while True:
            try:
                choice = input(f"\n请选择文件 (1-{len(csv_files) + 2}): ").strip()
                
                if not choice:
                    continue
                
                choice_num = int(choice)
                
                if 1 <= choice_num <= len(csv_files):
                    selected_file = csv_files[choice_num - 1][0]
                    full_path = self.default_input_dir / selected_file
                    
                    # 验证文件
                    if self.validate_input_file(str(full_path)):
                        print(f"✅ 已选择: {selected_file}")
                        return str(full_path)
                    else:
                        print(f"❌ 文件验证失败: {selected_file}")
                        continue
                
                elif choice_num == len(csv_files) + 1:
                    # 手动输入路径
                    custom_path = input("请输入完整文件路径: ").strip()
                    if custom_path and self.validate_input_file(custom_path):
                        print(f"✅ 已选择: {custom_path}")
                        return custom_path
                    else:
                        print("❌ 文件路径无效或验证失败")
                        continue
                
                elif choice_num == len(csv_files) + 2:
                    print("❌ 已取消文件选择")
                    return None
                
                else:
                    print(f"❌ 无效选择，请输入 1-{len(csv_files) + 2}")
                    
            except ValueError:
                print("❌ 请输入有效的数字")
            except KeyboardInterrupt:
                print("\n❌ 用户中断")
                return None
    
    def validate_input_file(self, file_path: str) -> bool:
        """验证输入文件"""
        try:
            path = Path(file_path)
            
            if not path.exists():
                print(f"❌ 文件不存在: {file_path}")
                return False
            
            if not path.suffix.lower() == '.csv':
                print(f"❌ 不是CSV文件: {file_path}")
                return False
            
            # 尝试读取文件
            df = pd.read_csv(file_path)
            
            if len(df) == 0:
                print(f"❌ 文件为空: {file_path}")
                return False
            
            if 'Address' not in df.columns:
                print(f"⚠️ 警告: 文件中没有'Address'列")
                print(f"   可用列: {list(df.columns)}")
                
                # 询问是否继续
                choice = input("是否继续使用此文件？(y/n): ").lower()
                if choice != 'y':
                    return False
            
            print(f"✅ 文件验证通过: {len(df):,} 行数据")
            return True
            
        except Exception as e:
            print(f"❌ 文件验证失败: {e}")
            return False
    
    def generate_output_filename(self, input_file: str, suffix: str = "poi_results") -> str:
        """生成输出文件名"""
        input_path = Path(input_file)
        base_name = input_path.stem
        
        # 移除常见的输入文件标识
        base_name = base_name.replace('_complete', '').replace('_input', '').replace('_addresses', '')
        
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M")
        output_filename = f"{base_name}_{suffix}_{timestamp}.csv"
        
        return str(self.default_output_dir / output_filename)
    
    def select_output_file_interactive(self, input_file: str) -> str:
        """交互式选择输出文件"""
        default_output = self.generate_output_filename(input_file)
        
        print(f"\n📁 输出文件设置")
        print(f"默认输出文件: {default_output}")
        
        choice = input("使用默认输出文件名？(y/n): ").lower()
        
        if choice == 'y' or choice == '':
            return default_output
        else:
            custom_output = input("请输入自定义输出文件路径: ").strip()
            if custom_output:
                # 确保是CSV文件
                if not custom_output.lower().endswith('.csv'):
                    custom_output += '.csv'
                
                # 确保输出目录存在
                output_path = Path(custom_output)
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
                return custom_output
            else:
                return default_output
    
    def select_files_gui(self) -> Tuple[Optional[str], Optional[str]]:
        """使用GUI选择文件 (可选功能)"""
        try:
            import tkinter as tk
            from tkinter import filedialog, messagebox
            
            # 创建隐藏的根窗口
            root = tk.Tk()
            root.withdraw()
            
            # 选择输入文件
            input_file = filedialog.askopenfilename(
                title="选择POI爬虫输入文件",
                initialdir=str(self.default_input_dir),
                filetypes=[
                    ("CSV文件", "*.csv"),
                    ("所有文件", "*.*")
                ]
            )
            
            if not input_file:
                root.destroy()
                return None, None
            
            # 验证输入文件
            if not self.validate_input_file(input_file):
                messagebox.showerror("错误", "选择的文件无效")
                root.destroy()
                return None, None
            
            # 选择输出文件
            default_output = self.generate_output_filename(input_file)
            output_file = filedialog.asksaveasfilename(
                title="选择输出文件位置",
                initialdir=str(self.default_output_dir),
                initialfile=Path(default_output).name,
                defaultextension=".csv",
                filetypes=[
                    ("CSV文件", "*.csv"),
                    ("所有文件", "*.*")
                ]
            )
            
            root.destroy()
            
            if not output_file:
                return input_file, default_output
            
            return input_file, output_file
            
        except ImportError:
            print("⚠️ 未安装tkinter，无法使用GUI模式")
            return None, None
        except Exception as e:
            print(f"❌ GUI文件选择失败: {e}")
            return None, None

def select_files_command_line() -> Tuple[Optional[str], Optional[str]]:
    """命令行文件选择入口函数"""
    selector = FileSelector()
    
    print("🎯 POI爬虫文件选择器")
    print("选择输入和输出文件")
    
    # 询问使用模式
    print("\n📋 选择模式:")
    print("1. 🖱️  GUI模式 (图形界面)")
    print("2. ⌨️  命令行模式")
    
    try:
        mode_choice = input("请选择模式 (1/2，默认2): ").strip()
        
        if mode_choice == '1':
            input_file, output_file = selector.select_files_gui()
            if input_file is None:
                print("❌ GUI模式失败，切换到命令行模式")
            else:
                return input_file, output_file
        
        # 命令行模式
        input_file = selector.select_input_file_interactive()
        if input_file is None:
            return None, None
        
        output_file = selector.select_output_file_interactive(input_file)
        
        return input_file, output_file
        
    except KeyboardInterrupt:
        print("\n❌ 用户中断")
        return None, None

def main():
    """测试文件选择器"""
    input_file, output_file = select_files_command_line()
    
    if input_file and output_file:
        print(f"\n✅ 文件选择完成:")
        print(f"📥 输入文件: {input_file}")
        print(f"📤 输出文件: {output_file}")
    else:
        print("❌ 文件选择失败或被取消")

if __name__ == "__main__":
    main()