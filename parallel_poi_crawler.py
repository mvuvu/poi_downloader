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
import json
import argparse

from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class ParallelPOICrawler:
    def __init__(self, max_workers=None, output_dir="data/output", batch_size=50, enable_resume=True):
        self.max_workers = max_workers or max(1, mp.cpu_count() - 1)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.output_file = None
        self._driver_pool = {}  # 驱动池缓存
        self.enable_resume = enable_resume
        self.progress_dir = Path("data/progress")
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = None
        
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

    def _save_progress(self, district_name, completed_batches, total_batches, total_success, total_errors):
        """保存进度到JSON文件"""
        if not self.enable_resume:
            return
            
        progress_data = {
            'district_name': district_name,
            'completed_batches': completed_batches,
            'total_batches': total_batches,
            'total_success': total_success,
            'total_errors': total_errors,
            'output_file': str(self.output_file),
            'timestamp': time.time()
        }
        
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    def _load_progress(self, district_name):
        """加载进度文件"""
        if not self.enable_resume or not self.progress_file.exists():
            return None
            
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                
            # 检查是否是同一个区的进度
            if progress_data.get('district_name') == district_name:
                return progress_data
        except (json.JSONDecodeError, KeyError, FileNotFoundError):
            pass
            
        return None
    
    def _cleanup_progress(self):
        """清理进度文件"""
        if self.progress_file and self.progress_file.exists():
            try:
                self.progress_file.unlink()
            except:
                pass
    
    def list_pending_tasks(self):
        """列出所有未完成的任务"""
        progress_files = list(self.progress_dir.glob("*_progress.json"))
        if not progress_files:
            print("没有发现未完成的任务")
            return
        
        print("未完成的任务:")
        print("=" * 60)
        for progress_file in progress_files:
            try:
                with open(progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                district = data['district_name']
                completed = data['completed_batches']
                total = data['total_batches']
                success = data['total_success']
                errors = data['total_errors']
                timestamp = data['timestamp']
                
                progress_percent = (completed / total) * 100
                time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                
                print(f"区域: {district}")
                print(f"进度: {completed}/{total} 批次 ({progress_percent:.1f}%)")
                print(f"成功: {success}, 失败: {errors}")
                print(f"最后更新: {time_str}")
                print(f"输出文件: {data['output_file']}")
                print("-" * 60)
                
            except Exception as e:
                print(f"读取进度文件 {progress_file} 失败: {e}")
    
    def clean_all_progress(self):
        """清理所有进度文件"""
        progress_files = list(self.progress_dir.glob("*_progress.json"))
        if not progress_files:
            print("没有进度文件需要清理")
            return
        
        for progress_file in progress_files:
            try:
                progress_file.unlink()
                print(f"已清理: {progress_file.name}")
            except Exception as e:
                print(f"清理失败 {progress_file.name}: {e}")
        
        print(f"共清理了 {len(progress_files)} 个进度文件")

    def crawl_batch_addresses(self, addresses_batch):
        """批量处理多个地址，减少进程间通信开销"""
        batch_results = []
        driver = None
        
        try:
            driver = self.create_driver()
            
            for worker_id, address, idx in addresses_batch:
                try:
                    result = self._crawl_poi_info(address, driver)
                    
                    if result is not None and not result.empty:
                        batch_results.append({
                            'success': True,
                            'data': result,
                            'address': address,
                            'worker_id': worker_id,
                            'index': idx
                        })
                    else:
                        batch_results.append({
                            'success': False,
                            'error': '未找到POI数据或不是建筑物',
                            'address': address,
                            'worker_id': worker_id,
                            'index': idx
                        })
                        
                except Exception as e:
                    batch_results.append({
                        'success': False,
                        'error': str(e),
                        'address': address,
                        'worker_id': worker_id,
                        'index': idx
                    })
                    
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
                    
        return batch_results

    def crawl_single_address(self, address_data):
        """兼容性保持：单地址处理"""
        result = self.crawl_batch_addresses([address_data])
        return result[0] if result else {'success': False, 'error': '处理失败'}

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
                #has_scrolled = True
            
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
                
                # 单地址完成总结
                print(f"{address}  | POI: {poi_count}")

                return df
            
            else:
            # 单地址完成总结
                print(f"{address}  | POI: {poi_count}")
                return None
        else:# 非建筑物也输出总结
            print(f"{address} | 建筑物: {'是' if is_building else '否'} | POI: {poi_count}")
            return None

    def process_batch(self, addresses_batch, batch_id):
        success_count = 0
        error_count = 0
        
        # 将地址分组，每个进程处理更多地址以减少Chrome启动开销
        addresses_per_worker = max(1, len(addresses_batch) // self.max_workers)
        worker_batches = []
        
        for i in range(0, len(addresses_batch), addresses_per_worker):
            worker_batch = [
                (idx, addr, idx) 
                for idx, (_, addr) in enumerate(addresses_batch[i:i+addresses_per_worker], i)
            ]
            if worker_batch:
                worker_batches.append(worker_batch)
        
        # 使用异步方式提交任务，不等待单个任务完成
        with ProcessPoolExecutor(max_workers=min(self.max_workers, len(worker_batches))) as executor:
            future_to_batch = {
                executor.submit(self.crawl_batch_addresses, batch): batch 
                for batch in worker_batches
            }
            
            # 批量收集结果并异步写入
            all_results = []
            for future in as_completed(future_to_batch):
                batch_results = future.result()
                all_results.extend(batch_results)
                
                # 分批写入以减少IO阻塞
                success_data = [r['data'] for r in batch_results if r['success']]
                if success_data:
                    self._batch_append_to_output_file(success_data)
                    
                success_count += sum(1 for r in batch_results if r['success'])
                error_count += sum(1 for r in batch_results if not r['success'])
        
        # 检查是否整个批次都失败了（都不是建筑物）
        if success_count == 0 and error_count == len(addresses_batch):
            # 检查所有错误是否都是"不是建筑"导致的
            not_building_count = sum(1 for r in all_results 
                                   if not r['success'] and 
                                   ('不是建筑' in str(r.get('error', '')) or 
                                    '未找到POI数据' in str(r.get('error', ''))))
            
            if not_building_count == len(addresses_batch):
                # 获取区名
                district_name = getattr(self, 'current_district_name', '当前区域')
                
                # 输出醒目的警告信息
                print(f"\n{'='*70}")
                print(f"⚠️  警告: 批次异常检测！")
                print(f"{'='*70}")
                print(f"区域: {district_name}")
                print(f"批次: {batch_id + 1}")
                print(f"地址数量: {len(addresses_batch)}")
                print(f"状态: 所有地址都不是建筑物（100%失败）")
                print(f"\n可能的原因:")
                print(f"  1. 地址格式不正确")
                print(f"  2. 地址数据已过期或无效")
                print(f"  3. 该区域可能主要是非建筑物地址（如公园、道路等）")
                print(f"  4. Google Maps API响应异常")
                print(f"\n建议操作:")
                print(f"  1. 检查输入CSV文件中的地址格式")
                print(f"  2. 验证几个样本地址是否能在Google Maps上正确定位")
                print(f"  3. 如果问题持续，考虑跳过该批次或更新地址数据")
                print(f"{'='*70}\n")
                
                # 记录详细信息到日志文件
                log_file = f"non_building_warnings/{district_name}_batch_{batch_id + 1}_warning.log"
                os.makedirs("non_building_warnings", exist_ok=True)
                
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.write(f"警告日志 - {district_name} 批次 {batch_id + 1}\n")
                    f.write(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"批次大小: {len(addresses_batch)}\n")
                    f.write(f"失败率: 100%\n")
                    f.write(f"\n失败的地址列表:\n")
                    f.write("-" * 50 + "\n")
                    for i, r in enumerate(all_results, 1):
                        f.write(f"{i}. {r['address']}\n")
                
                print(f"详细信息已保存到: {log_file}\n")
        
        return success_count, error_count, success_count

    def _append_to_output_file(self, data):
        """实时追加数据到输出文件"""
        if self.output_file is None:
            return
            
        # 追加模式写入CSV
        data.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
        
    def _batch_append_to_output_file(self, data_list):
        """批量追加多个DataFrame到输出文件，减少IO次数"""
        if self.output_file is None or not data_list:
            return
            
        # 合并所有DataFrame然后一次性写入
        combined_df = pd.concat(data_list, ignore_index=True)
        combined_df.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')

    def _extract_district_name(self, input_file):
        """从输入文件名提取区名"""
        filename = Path(input_file).stem
        if '区' in filename:
            # 提取区名，如 "千代田区_complete_1751433587" -> "千代田区"
            return filename.split('区')[0] + '区'
        return 'unknown_district'

    def crawl_from_csv(self, input_file):
        df = pd.read_csv(input_file)
        # 优先使用ConvertedAddress字段，如果不存在则使用Address字段
        if 'ConvertedAddress' in df.columns:
            addresses = df['ConvertedAddress'].tolist()
        elif 'Address' in df.columns:
            addresses = df['Address'].tolist()
        else:
            addresses = df.iloc[:, -1].tolist()
        
        # 设置输出文件路径（以区命名）
        district_name = self._extract_district_name(input_file)
        self.current_district_name = district_name  # 保存当前区域名称
        self.progress_file = self.progress_dir / f"{district_name}_progress.json"
        
        # 检查是否有未完成的进度
        progress_data = self._load_progress(district_name)
        start_batch_id = 0
        total_success = 0
        total_errors = 0
        
        if progress_data:
            print(f"发现未完成的{district_name}爬取任务，从第{progress_data['completed_batches']+1}批次继续")
            start_batch_id = progress_data['completed_batches']
            total_success = progress_data['total_success']
            total_errors = progress_data['total_errors']
            self.output_file = Path(progress_data['output_file'])
            print(f"恢复输出文件: {self.output_file}")
        else:
            # 新任务，创建新的输出文件
            self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
            # 创建CSV文件并写入表头
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        total_addresses = len(addresses)
        
        # 使用更大的批次以减少批次间等待时间
        optimized_batch_size = min(self.batch_size * 2, 100)  # 动态调整批次大小
        total_batches = (total_addresses + optimized_batch_size - 1) // optimized_batch_size
        
        if start_batch_id == 0:
            print(f"开始爬取 {district_name} {total_addresses} 个地址，分 {total_batches} 批次处理")
        else:
            print(f"继续爬取 {district_name}，剩余 {total_batches - start_batch_id} 批次")
        print(f"优化批次大小: {optimized_batch_size}, 最大并发: {self.max_workers}")
        print(f"输出文件: {self.output_file}\n")
        
        for batch_id in range(start_batch_id, total_batches):
            start_idx = batch_id * optimized_batch_size
            end_idx = min(start_idx + optimized_batch_size, total_addresses)
            
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
            
            # 保存进度
            self._save_progress(district_name, batch_id, total_batches, total_success, total_errors)
        
        print(f"\n{district_name} 爬取完成！总成功: {total_success}, 总失败: {total_errors}")
        print(f"数据已保存到: {self.output_file}")
        
        # 清理进度文件
        self._cleanup_progress()
        
        return total_success, total_errors

    def _warm_up_drivers(self):
        """预热Chrome驱动实例，减少首次启动开销"""
        print("正在预热Chrome驱动实例...")
        test_driver = None
        try:
            test_driver = self.create_driver()
            test_driver.get("https://www.google.com/maps")
            time.sleep(2)  # 让页面完全加载
            print("Chrome驱动预热完成")
        except Exception as e:
            print(f"Chrome驱动预热失败: {e}")
        finally:
            if test_driver:
                try:
                    test_driver.quit()
                except:
                    pass

    def crawl_all_districts(self, input_dir="data/input", resume_single_district=None):
        """批量处理input目录中的所有区文件"""
        input_path = Path(input_dir)
        csv_files = list(input_path.glob("*.csv"))
        
        if not csv_files:
            print(f"在 {input_dir} 目录中没有找到CSV文件")
            return
        
        print(f"发现 {len(csv_files)} 个区文件，开始批量处理...\n")
        
        # 预热驱动
        self._warm_up_drivers()
        
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
    parser = argparse.ArgumentParser(description='POI爬虫 - 支持断点续传')
    parser.add_argument('input_file', nargs='?', help='输入CSV文件路径')
    parser.add_argument('--all', action='store_true', help='批量处理所有区文件')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传功能')
    parser.add_argument('--workers', type=int, default=max(4, mp.cpu_count()), help='并发工作进程数')
    parser.add_argument('--batch-size', type=int, default=30, help='批次大小')
    parser.add_argument('--status', action='store_true', help='查看未完成任务状态')
    parser.add_argument('--clean-progress', action='store_true', help='清理所有进度文件')
    
    args = parser.parse_args()
    
    # 创建爬虫实例用于管理功能
    crawler = ParallelPOICrawler(enable_resume=True)
    
    # 处理管理命令
    if args.status:
        crawler.list_pending_tasks()
        return
    
    if args.clean_progress:
        crawler.clean_all_progress()
        return
    
    if not args.input_file and not args.all:
        print("用法:")
        print("  单个文件: python parallel_poi_crawler.py <输入CSV文件> [选项]")
        print("  批量处理: python parallel_poi_crawler.py --all [选项]")
        print("  进度管理: python parallel_poi_crawler.py --status | --clean-progress")
        print("")
        print("选项:")
        print("  --no-resume        禁用断点续传功能")
        print("  --workers N        设置并发工作进程数 (默认: CPU核心数)")
        print("  --batch-size N     设置批次大小 (默认: 30)")
        print("  --status          查看未完成任务状态")
        print("  --clean-progress  清理所有进度文件")
        print("")
        print("示例:")
        print("  python parallel_poi_crawler.py data/input/千代田区_complete.csv")
        print("  python parallel_poi_crawler.py --all")
        print("  python parallel_poi_crawler.py --all --no-resume")
        print("  python parallel_poi_crawler.py --status")
        return
    
    # 创建爬虫实例
    enable_resume = not args.no_resume
    crawler = ParallelPOICrawler(
        max_workers=args.workers, 
        batch_size=args.batch_size,
        enable_resume=enable_resume
    )
    
    if args.all:
        # 批量处理所有区文件
        crawler.crawl_all_districts()
    else:
        # 处理单个文件
        input_file = args.input_file
        if not os.path.exists(input_file):
            print(f"文件不存在: {input_file}")
            return
        
        crawler.crawl_from_csv(input_file)


if __name__ == "__main__":
    main()