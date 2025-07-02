#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import multiprocessing as mp
import os
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException
import sys

from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class ParallelPOICrawler:
    def __init__(self, max_workers=None, output_dir="data/output", batch_size=50):
        self.max_workers = max_workers or max(1, mp.cpu_count() - 1)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.output_file = None
        
    def create_driver(self):


        options = webdriver.ChromeOptions()
        
        # 基础静默配置
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        
        # 彻底禁用日志和警告
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-images')
        options.add_argument('--disable-javascript')
        options.add_argument('--disable-web-security')
        options.add_argument('--disable-features=VizDisplayCompositor')
        
        # GPU和WebGL错误抑制
        options.add_argument('--disable-gl-error-limit')
        options.add_argument('--disable-webgl')
        options.add_argument('--disable-webgl2')
        options.add_argument('--use-gl=disabled')
        
        # DevTools和调试信息禁用
        options.add_argument('--remote-debugging-port=0')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        
        # 实验性选项
        options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
    
        # 完全静默Service
        service = Service(
            ChromeDriverManager().install(),
            log_path='NUL',
            service_args=['--silent']
        )

        
        return webdriver.Chrome(service=service, options=options)

    def crawl_single_address(self, address_data):
        worker_id, address, idx = address_data
        driver = None
        
        try:
            driver = self.create_driver()
            result = self._crawl_poi_info(address, driver)
            
            if result is not None and not result.empty:
                return {
                    'success': True,
                    'data': result,
                    'address': address,
                    'worker_id': worker_id,
                    'index': idx
                }
            else:
                return {
                    'success': False,
                    'error': '未找到POI数据',
                    'address': address,
                    'worker_id': worker_id,
                    'index': idx
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'address': address,
                'worker_id': worker_id,
                'index': idx
            }
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass

    def _crawl_poi_info(self, address, driver):
        url = f'https://www.google.com/maps/place/{address}'
        driver.get(url)
        
        place_type = get_building_type(driver)
        is_building = place_type == '建筑物'
        has_scrolled = False
        poi_count = 0
        comment_count = 0
        
        if is_building:
            place_name = get_building_name(driver)
            
            more_button = driver.find_elements('class name', 'M77dve')
            if more_button:
                click_on_more_button(driver)
                scroll_poi_section(driver)
                has_scrolled = True
            
            df = get_all_poi_info(driver)
            
            if df is not None and not df.empty:
                poi_count = len(df)
                final_url = wait_for_coords_url(driver)
                if final_url:
                    lat, lng = get_coords(final_url)
                else:
                    print("❌ 没有拿到有效的坐标 URL")
                df['blt_name'] = place_name
                df['lat'] = lat
                df['lng'] = lng
                # comment_count已经在get_all_poi_info中为每个POI单独设置
                
                

                return df
            
            # 单地址完成总结
            print(f"{address} | 建筑物: {'是' if is_building else '否'} | 滑动: {'有' if has_scrolled else '无'} | POI: {poi_count}")
        
        else:# 非建筑物也输出总结
            print(f"{address} | 建筑物: {'是' if is_building else '否'}")
            return None

    def process_batch(self, addresses_batch, batch_id):
        success_count = 0
        error_count = 0
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            address_tasks = [
                (worker_id % self.max_workers, addr, idx) 
                for idx, (worker_id, addr) in enumerate(addresses_batch)
            ]
            
            future_to_address = {
                executor.submit(self.crawl_single_address, task): task 
                for task in address_tasks
            }
            
            for future in as_completed(future_to_address):
                result = future.result()
                
                if result['success']:
                    # 实时追加到输出文件
                    self._append_to_output_file(result['data'])
                    success_count += 1
                else:
                    error_count += 1
        
        return success_count, error_count, success_count

    def _append_to_output_file(self, data):
        """实时追加数据到输出文件"""
        if self.output_file is None:
            return
            
        # 追加模式写入CSV
        data.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')

    def _extract_district_name(self, input_file):
        """从输入文件名提取区名"""
        filename = Path(input_file).stem
        if '区' in filename:
            # 提取区名，如 "千代田区_complete_1751433587" -> "千代田区"
            return filename.split('区')[0] + '区'
        return 'unknown_district'

    def crawl_from_csv(self, input_file):
        df = pd.read_csv(input_file)
        addresses = df['Address'].tolist() if 'Address' in df.columns else df.iloc[:, -1].tolist()
        
        # 设置输出文件路径（以区命名）
        district_name = self._extract_district_name(input_file)
        self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
        
        # 创建CSV文件并写入表头
        header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
        header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        total_addresses = len(addresses)
        total_success = 0
        total_errors = 0
        total_batches = (total_addresses + self.batch_size - 1) // self.batch_size
        
        print(f"开始爬取 {district_name} {total_addresses} 个地址，分 {total_batches} 批次处理")
        print(f"输出文件: {self.output_file}\n")
        
        for batch_id in range(total_batches):
            start_idx = batch_id * self.batch_size
            end_idx = min(start_idx + self.batch_size, total_addresses)
            
            batch_addresses = [
                (i, addresses[i]) 
                for i in range(start_idx, end_idx)
            ]
            
            batch_start_time = time.time()
            success, errors, data_count = self.process_batch(batch_addresses, batch_id)
            batch_end_time = time.time()
            
            total_success += success
            total_errors += errors
            
            print(f"\n批次 {batch_id + 1}/{total_batches} 完成: "
                  f"成功 {success}, 失败 {errors}, "
                  f"耗时 {batch_end_time - batch_start_time:.2f}s, "
                  f"总进度 {end_idx}/{total_addresses}\n")
        
        print(f"\n{district_name} 爬取完成！总成功: {total_success}, 总失败: {total_errors}")
        print(f"数据已保存到: {self.output_file}")
        return total_success, total_errors

    def crawl_all_districts(self, input_dir="data/input"):
        """批量处理input目录中的所有区文件"""
        input_path = Path(input_dir)
        csv_files = list(input_path.glob("*.csv"))
        
        if not csv_files:
            print(f"在 {input_dir} 目录中没有找到CSV文件")
            return
        
        print(f"发现 {len(csv_files)} 个区文件，开始批量处理...\n")
        
        all_success = 0
        all_errors = 0
        processed_districts = []
        
        start_time = time.time()
        
        for i, csv_file in enumerate(csv_files):
            district_name = self._extract_district_name(csv_file)
            print(f"{'='*60}")
            print(f"处理第 {i+1}/{len(csv_files)} 个区: {district_name}")
            print(f"{'='*60}")
            
            try:
                success, errors = self.crawl_from_csv(csv_file)
                all_success += success
                all_errors += errors
                processed_districts.append(f"{district_name}: 成功{success}, 失败{errors}")
                
            except Exception as e:
                print(f"处理 {district_name} 时发生错误: {e}")
                all_errors += 1
                processed_districts.append(f"{district_name}: 处理失败")
            
            print(f"\n{district_name} 处理完成\n")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"{'='*60}")
        print(f"全部区域处理完成！")
        print(f"{'='*60}")
        print(f"总耗时: {total_time/60:.1f} 分钟")
        print(f"总成功: {all_success}")
        print(f"总失败: {all_errors}")
        print(f"处理了 {len(processed_districts)} 个区:")
        
        for district_summary in processed_districts:
            print(f"  {district_summary}")

    def _merge_results(self):
        # 不再需要合并，因为数据实时写入单个文件
        pass


def main():
    if len(sys.argv) < 2:
        print("用法:")
        print("  单个文件: python parallel_poi_crawler.py <输入CSV文件>")
        print("  批量处理: python parallel_poi_crawler.py --all")
        print("")
        print("示例:")
        print("  python parallel_poi_crawler.py data/input/千代田区_complete.csv")
        print("  python parallel_poi_crawler.py --all")
        return
    
    crawler = ParallelPOICrawler(max_workers=4, batch_size=20)
    
    if sys.argv[1] == "--all":
        # 批量处理所有区文件
        crawler.crawl_all_districts()
    else:
        # 处理单个文件
        input_file = sys.argv[1]
        if not os.path.exists(input_file):
            print(f"文件不存在: {input_file}")
            return
        
        crawler.crawl_from_csv(input_file)


if __name__ == "__main__":
    main()