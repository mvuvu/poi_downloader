#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import json
import re
import sys
from pathlib import Path

class AddressConverter:
    def __init__(self, mapping_file="data/archive/tokyo_complete_mapping.json"):
        """
        基于mapping.json文件进行地址转换
        """
        self.mapping_file = Path(mapping_file)
        self.load_mapping()
    
    def load_mapping(self):
        """加载映射数据"""
        try:
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
            
            self.area_mapping = mapping_data.get('area_mapping', {})
            self.postal_mapping = mapping_data.get('postal_mapping', {})
            self.ward_mapping = mapping_data.get('ward_mapping', {})
            
            print(f"成功加载映射数据:")
            print(f"  - 地名映射: {len(self.area_mapping)} 条")
            print(f"  - 邮编映射: {len(self.postal_mapping)} 条") 
            print(f"  - 区名映射: {len(self.ward_mapping)} 条")
            
        except Exception as e:
            print(f"加载映射文件失败: {e}")
            sys.exit(1)
    
    def parse_japanese_address(self, address):
        """
        解析日文地址
        格式1: 東京都千代田区神田駿河台3丁目1-1
        格式2: 東京都渋谷区鶯谷町1-1 (没有丁目)
        """
        # 移除东京都前缀
        address = address.replace('東京都', '')
        
        # 提取区名
        ward_match = re.search(r'([^区]+区)', address)
        if not ward_match:
            return None, None, None, None
        
        ward = ward_match.group(1)
        remaining = address[ward_match.end():]
        
        # 检查是否有丁目
        if '丁目' in remaining:
            # 有丁目的情况: 神田駿河台3丁目1-1
            area_match = re.search(r'^([^0-9]+?)([0-9]+丁目.*)$', remaining)
            if area_match:
                area = area_match.group(1)
                chome_info = area_match.group(2)
            else:
                area = remaining
                chome_info = ""
        else:
            # 没有丁目的情况: 鶯谷町1-1
            area_match = re.search(r'^([^0-9]+?)([0-9].*)$', remaining)
            if area_match:
                area = area_match.group(1)
                chome_info = area_match.group(2)  # 直接是数字部分，如 1-1
            else:
                area = remaining
                chome_info = ""
        
        return ward, area, chome_info, remaining
    
    def convert_chome_info(self, chome_info):
        """
        转换丁目信息
        3丁目1-1 -> 3-chōme−1-1
        1-1 -> 1−1 (没有丁目的情况)
        """
        if not chome_info:
            return ""
        
        # 检查是否有丁目
        if '丁目' in chome_info:
            # 处理丁目: 3丁目1-1 -> 3-chōme−1-1
            chome_info = re.sub(r'([0-9]+)丁目', r'\1-chōme−', chome_info)
        
        # 将所有连接符统一为−
        chome_info = re.sub(r'-([0-9])', r'−\1', chome_info)
        
        return chome_info
    
    def convert_address(self, japanese_address):
        """
        将日文地址转换为ConvertedAddress格式
        """
        try:
            ward, area, chome_info, remaining = self.parse_japanese_address(japanese_address)
            
            if not ward or not area:
                return None
            
            # 获取区名英文
            ward_en = self.ward_mapping.get(ward, ward)
            
            # 获取地名英文
            area_en = self.area_mapping.get(area, area)
            
            # 获取邮编
            postal_code = '100-0000'  # 默认邮编
            
            # 先尝试在区的映射中查找地名
            if ward in self.postal_mapping:
                ward_postal_mapping = self.postal_mapping[ward]
                if area in ward_postal_mapping:
                    postal_code = ward_postal_mapping[area]
                elif 'default' in ward_postal_mapping:
                    postal_code = ward_postal_mapping['default']
            
            # 转换丁目信息
            chome_en = self.convert_chome_info(chome_info)
            
            # 组装ConvertedAddress
            # 格式: 〒101-0062+Tokyo,+Chiyoda+City,+Kandasurugadai,+3-chōme−1-1
            parts = [f"〒{postal_code}", "Tokyo", ward_en, area_en]
            if chome_en:
                parts.append(chome_en)
            
            # 使用,+作为分隔符（与现有格式保持一致）
            converted_address = ",+".join(parts).replace(" ", "+")
            
            return converted_address
            
        except Exception as e:
            print(f"转换地址失败: {japanese_address} - {e}")
            return None
    
    def convert_csv_file(self, input_file, output_file=None):
        """
        转换CSV文件中的地址
        """
        input_path = Path(input_file)
        if not input_path.exists():
            print(f"输入文件不存在: {input_file}")
            return False
        
        if output_file is None:
            output_file = input_path.parent / f"{input_path.stem}_converted.csv"
        
        try:
            # 读取CSV
            df = pd.read_csv(input_file)
            
            if 'Address' not in df.columns:
                print(f"CSV文件中没有找到Address列: {input_file}")
                return False
            
            print(f"开始转换 {len(df)} 个地址...")
            
            # 转换地址
            converted_addresses = []
            success_count = 0
            
            for idx, address in enumerate(df['Address']):
                converted = self.convert_address(address)
                converted_addresses.append(converted)
                
                if converted:
                    success_count += 1
                
                if (idx + 1) % 1000 == 0:
                    print(f"已处理 {idx + 1}/{len(df)} 个地址...")
            
            # 添加ConvertedAddress列
            df['ConvertedAddress'] = converted_addresses
            
            # 保存结果
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"转换完成!")
            print(f"成功转换: {success_count}/{len(df)} 个地址")
            print(f"输出文件: {output_file}")
            
            return True
            
        except Exception as e:
            print(f"处理CSV文件失败: {e}")
            return False
    
    def convert_all_input_files(self, input_dir="data/oring_add"):
        """
        批量转换input目录中的所有CSV文件，直接覆盖原文件
        """
        input_path = Path(input_dir)
        if not input_path.exists():
            print(f"输入目录不存在: {input_dir}")
            return
        
        csv_files = list(input_path.glob("*.csv"))
        if not csv_files:
            print(f"在 {input_dir} 中没有找到CSV文件")
            return
        
        print(f"发现 {len(csv_files)} 个CSV文件，开始批量转换...")
        
        success_count = 0
        
        for csv_file in csv_files:
            print(f"\n处理文件: {csv_file.name}")
            
            # 直接转换并覆盖原文件
            if self.convert_csv_file(csv_file, csv_file):
                success_count += 1
                print(f"已覆盖更新: {csv_file.name}")
        
        print(f"\n批量转换完成! 成功处理 {success_count}/{len(csv_files)} 个文件")

def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  转换单个文件: python address_converter.py <input_csv>")
        print("  转换所有文件: python address_converter.py --all")
        print("  重新转换所有: python address_converter.py --regenerate")
        print("")
        print("示例:")
        print("  python address_converter.py origin_add/test.csv")
        print("  python address_converter.py --all")
        print("")
        print("注意: --all 命令会处理 origin_add/ 目录中的所有CSV文件")
        return
    
    converter = AddressConverter()
    
    if sys.argv[1] == "--all":
        # 转换所有文件，直接覆盖
        converter.convert_all_input_files()
    
    elif sys.argv[1] == "--regenerate":
        # 强制重新生成所有文件的ConvertedAddress
        converter.convert_all_input_files()
    
    else:
        # 转换单个文件
        input_file = sys.argv[1]
        output_file = sys.argv[2] if len(sys.argv) > 2 else None
        converter.convert_csv_file(input_file, output_file)

if __name__ == "__main__":
    main()