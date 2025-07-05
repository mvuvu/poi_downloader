#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import logging
import multiprocessing as mp
import os
import time
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException
import sys
import json
import argparse
import threading
from queue import Queue, Empty, LifoQueue
import signal
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from threading import Lock, RLock
from collections import deque
import gc
import re

from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section

# 全局锁和状态管理
_global_stats_lock = Lock()
_global_stats = {'success': 0, 'errors': 0, 'processing': 0}

class HybridDriverPool:
    """混合Chrome驱动池管理器 - 结合turbo的高性能和原版的稳定性"""
    def __init__(self, max_drivers=16, max_usage_per_driver=40):
        self.max_drivers = max_drivers
        self.max_usage_per_driver = max_usage_per_driver
        self.pool = deque()
        self.usage_count = {}
        self.lock = RLock()
        self.total_created = 0
        
    def create_optimized_driver(self):
        """创建高度优化的Chrome实例 - 基于safe版本的稳定配置"""
        try:
            options = webdriver.ChromeOptions()
            
            # Windows兼容的基础配置
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            
            # 减少资源占用
            options.add_argument('--disable-extensions')
            options.add_argument('--disable-plugins')
            options.add_argument('--disable-images')
            options.add_argument('--disable-javascript')
            
            # Windows特定优化
            options.add_argument('--disable-logging')
            options.add_argument('--log-level=3')
            options.add_argument('--silent')
            
            # 内存优化
            options.add_argument('--memory-pressure-off')
            options.add_argument('--max_old_space_size=512')
            
            # 禁用资源加载
            prefs = {
                'profile.default_content_setting_values': {
                    'cookies': 2, 'images': 2, 'plugins': 2, 'popups': 2,
                    'geolocation': 2, 'notifications': 2, 'media_stream': 2,
                }
            }
            options.add_experimental_option('prefs', prefs)
            options.page_load_strategy = 'eager'
            
            # Windows兼容的Service配置
            try:
                driver_path = ChromeDriverManager().install()
                print(f"Chrome实例#{self.total_created + 1}: {driver_path}")
                
                service = Service(
                    driver_path,
                    log_path='nul',
                    service_args=['--silent', '--log-level=OFF']
                )
            except Exception as e:
                print(f"ChromeDriver安装失败: {e}")
                return None

            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(10)  # 适中的超时时间
            driver.implicitly_wait(2)
            
            return driver
            
        except Exception as e:
            print(f"创建Chrome驱动失败: {e}")
            return None
    
    def get_driver(self):
        """获取一个可用的驱动实例"""
        with self.lock:
            # 尝试从池中获取
            while self.pool:
                driver = self.pool.popleft()
                driver_id = id(driver)
                
                if self.usage_count.get(driver_id, 0) < self.max_usage_per_driver:
                    try:
                        # 健康检查
                        driver.current_url
                        self.usage_count[driver_id] = self.usage_count.get(driver_id, 0) + 1
                        return driver
                    except:
                        # 驱动已损坏
                        try:
                            driver.quit()
                        except:
                            pass
                        self.usage_count.pop(driver_id, None)
                else:
                    # 使用次数超限
                    try:
                        driver.quit()
                    except:
                        pass
                    self.usage_count.pop(driver_id, None)
            
            # 创建新驱动（如果未达到上限）
            if len(self.usage_count) < self.max_drivers:
                print(f"创建第 {self.total_created + 1} 个Chrome实例...")
                driver = self.create_optimized_driver()
                if driver:
                    driver_id = id(driver)
                    self.usage_count[driver_id] = 1
                    self.total_created += 1
                    print(f"成功创建Chrome实例 #{self.total_created}")
                    return driver
            
            # 达到上限或创建失败
            return None
    
    def return_driver(self, driver):
        """归还驱动到池中"""
        if driver is None:
            return
            
        with self.lock:
            driver_id = id(driver)
            if driver_id in self.usage_count:
                try:
                    # 清理操作（基于原版经验）
                    driver.delete_all_cookies()
                    driver.execute_script("window.gc();")  # 强制垃圾回收
                except:
                    pass
                
                self.pool.append(driver)
            else:
                # 无效驱动
                try:
                    driver.quit()
                except:
                    pass
    
    def cleanup_all(self):
        """清理所有驱动"""
        with self.lock:
            while self.pool:
                driver = self.pool.popleft()
                try:
                    driver.quit()
                except:
                    pass
            
            self.usage_count.clear()
            print(f"Chrome驱动池已清理，共创建了 {self.total_created} 个实例")


class HybridTaskScheduler:
    """混合任务调度器 - turbo的高性能调度"""
    def __init__(self, max_threads=48):
        self.max_threads = max_threads
        self.task_queue = LifoQueue(maxsize=max_threads * 20)  # 使用LIFO提高缓存命中率
        self.result_queue = Queue()
        self.pending_tasks = deque()
        self.completed_count = 0
        self.lock = Lock()
        self.stop_event = threading.Event()
        
    def add_tasks(self, tasks):
        """批量添加任务"""
        self.pending_tasks.extend(tasks)
        
    def feed_tasks(self):
        """持续向队列中添加任务"""
        while self.pending_tasks and not self.stop_event.is_set():
            try:
                if not self.task_queue.full():
                    # 批量添加多个任务提高效率
                    for _ in range(min(10, len(self.pending_tasks))):
                        if self.pending_tasks:
                            task = self.pending_tasks.popleft()
                            self.task_queue.put(task, timeout=1)
                        else:
                            break
                time.sleep(0.05)  # 短暂休眠减少CPU占用
            except:
                time.sleep(0.5)
    
    def get_task(self):
        """获取一个任务"""
        try:
            return self.task_queue.get(timeout=1)
        except Empty:
            return None
    
    def put_result(self, result):
        """提交结果"""
        try:
            self.result_queue.put(result, timeout=1)
            with self.lock:
                self.completed_count += 1
        except:
            pass  # 丢弃结果避免阻塞
    
    def get_result(self):
        """获取结果"""
        try:
            return self.result_queue.get(timeout=0.1)
        except Empty:
            return None
    
    def stop(self):
        """停止调度器"""
        self.stop_event.set()


class HybridWorker(threading.Thread):
    """混合工作线程 - turbo的高性能 + 原版的错误处理"""
    def __init__(self, worker_id, driver_pool, task_scheduler, crawler):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.driver_pool = driver_pool
        self.task_scheduler = task_scheduler
        self.crawler = crawler
        self.processed_count = 0
        self.current_driver = None
        
    def run(self):
        """线程主循环"""
        print(f"Worker {self.worker_id} 启动")
        
        while not self.task_scheduler.stop_event.is_set():
            # 获取任务
            task = self.task_scheduler.get_task()
            if task is None:
                time.sleep(0.1)
                continue
            
            # 获取Chrome驱动
            if self.current_driver is None:
                self.current_driver = self.driver_pool.get_driver()
                if self.current_driver is None:
                    # 无法获取驱动，将任务放回队列
                    try:
                        self.task_scheduler.task_queue.put(task, timeout=1)
                    except:
                        pass
                    time.sleep(1)
                    continue
            
            # 处理任务
            try:
                result = self.process_task(task)
                self.task_scheduler.put_result(result)
                self.processed_count += 1
                
                # 更新全局统计
                with _global_stats_lock:
                    if result['success']:
                        _global_stats['success'] += 1
                    else:
                        _global_stats['errors'] += 1
                
                # 定期报告进度
                if self.processed_count % 20 == 0:
                    print(f"Worker {self.worker_id}: 已处理 {self.processed_count} 个任务")
                
            except Exception as e:
                # 处理失败，生成错误结果
                error_result = {
                    'success': False,
                    'error': str(e),
                    'address': str(task.get('address', 'unknown')),
                    'worker_id': self.worker_id,
                    'index': task.get('index', -1)
                }
                self.task_scheduler.put_result(error_result)
                
                # Chrome可能有问题，释放并重新获取
                if self.current_driver:
                    try:
                        self.current_driver.quit()
                    except:
                        pass
                    self.current_driver = None
        
        # 归还驱动
        if self.current_driver:
            self.driver_pool.return_driver(self.current_driver)
            self.current_driver = None
        
        print(f"Worker {self.worker_id} 完成，共处理 {self.processed_count} 个任务")
    
    def process_task(self, task):
        """处理单个任务 - 使用原版的稳定爬取逻辑"""
        address_obj = task['address']
        idx = task['index']
        
        start_time = time.time()
        
        # 解析地址对象（兼容原版和turbo版本）
        if isinstance(address_obj, dict):
            # 优先使用formatted address
            used_address = None
            retry_info = ""
            
            if address_obj.get('primary') and address_obj['primary'].strip():
                used_address = address_obj['primary']
                retry_info = "primary"
            elif address_obj.get('secondary') and address_obj['secondary'].strip():
                used_address = address_obj['secondary']
                retry_info = "secondary"
            elif address_obj.get('fallback') and address_obj['fallback'].strip():
                used_address = address_obj['fallback']
                retry_info = "fallback"
        else:
            used_address = address_obj
            retry_info = "direct"
        
        if not used_address:
            return {
                'success': False,
                'error': '地址为空',
                'address': 'empty',
                'worker_id': self.worker_id,
                'index': idx
            }
        
        # 使用原版的稳定爬取逻辑
        try:
            result = self.crawler._crawl_poi_info_hybrid(used_address, self.current_driver)
            processing_time = time.time() - start_time
            
            # 构建结果（基于turbo版本的结果结构）
            if (isinstance(result, dict) and 
                result.get('status') == 'success' and 
                result.get('data') is not None):
                return {
                    'success': True,
                    'data': result['data'],
                    'address': used_address,
                    'worker_id': self.worker_id,
                    'index': idx,
                    'retry_info': retry_info,
                    'processing_time': processing_time
                }
            else:
                # 检查是否是chōme格式地址的特殊情况
                warning_info = None
                chome_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'
                if re.search(chome_pattern, used_address, re.IGNORECASE):
                    warning_info = {
                        'type': 'chome_format_no_poi', 
                        'message': f"chōme格式地址无POI数据: {used_address}"
                    }
                
                error_msg = '未找到POI数据' if isinstance(result, dict) and result.get('poi_count', 0) == 0 else '处理异常'
                return {
                    'success': False,
                    'error': error_msg,
                    'address': used_address,
                    'worker_id': self.worker_id,
                    'index': idx,
                    'retry_info': retry_info,
                    'processing_time': processing_time,
                    'warning_info': warning_info
                }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'address': used_address,
                'worker_id': self.worker_id,
                'index': idx,
                'retry_info': retry_info,
                'processing_time': time.time() - start_time
            }


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class ParallelPOICrawler:
    def __init__(self, max_workers=None, output_dir="data/output", batch_size=50, enable_resume=True):
        # 智能工作线程数配置（基于turbo版本的CPU优化）
        cpu_count = mp.cpu_count()
        if max_workers is None:
            if cpu_count >= 12:
                # 12核心或以上：使用CPU数量*4，最大48线程
                self.max_workers = min(48, cpu_count * 4)
            elif cpu_count >= 8:
                # 8-11核心：使用CPU数量*3
                self.max_workers = min(36, cpu_count * 3)
            else:
                # 8核心以下：使用CPU数量*2
                self.max_workers = min(24, cpu_count * 2)
        else:
            self.max_workers = max_workers
            
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.batch_size = batch_size
        self.output_file = None
        self.enable_resume = enable_resume
        self.progress_dir = Path("data/progress")
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = None
        
        # 警告保存系统（turbo版本的功能）
        self.warnings_dir = Path("data/warnings")
        self.warnings_dir.mkdir(parents=True, exist_ok=True)
        self.warnings_file = None
        self.warning_batch = []
        self.warning_lock = Lock()
        
        # no_poi_warnings 系统（原版 + turbo的增强）
        self.no_poi_warnings_dir = Path("no_poi_warnings")
        self.no_poi_warnings_dir.mkdir(exist_ok=True)
        self.no_poi_batch_tracker = {}
        self.no_poi_tracker_lock = Lock()
        
        # 智能Chrome驱动池大小配置
        chrome_pool_size = min(16, max(8, self.max_workers // 3))
        
        # 初始化组件
        self.driver_pool = HybridDriverPool(max_drivers=chrome_pool_size)
        self.task_scheduler = HybridTaskScheduler(max_threads=self.max_workers)
        
        print(f"Hybrid模式配置: {self.max_workers}线程 + {chrome_pool_size}Chrome实例")
        
    def _crawl_poi_info_hybrid(self, address, driver, check_building_type=True):
        """混合模式的POI信息爬取 - 基于原版的稳定逻辑"""
        url = f'https://www.google.com/maps/place/{address}'
        
        try:
            driver.get(url)
            
            # 等待页面加载（原版的稳定等待）
            time.sleep(2)
            
            # 检查酒店类别页面（原版逻辑）
            try:
                category_headers = driver.find_elements("css selector", "h2.kPvgOb.fontHeadlineSmall")
                for header in category_headers:
                    if header.text.strip() == "酒店":
                        return {
                            'data': None,
                            'is_building': False,
                            'poi_count': 0,
                            'status': 'hotel_category_page_skipped'
                        }
            except:
                pass
            
            # 获取地点名称（原版逻辑）
            try:
                place_name = get_building_name(driver)
            except:
                try:
                    title = driver.title
                    if title and title != "Google Maps" and "Google" not in title:
                        place_name = title.replace(" - Google Maps", "").replace(" - Google 地图", "").strip()
                    else:
                        place_name = 'Unknown Location'
                except:
                    place_name = 'Unknown Location'
            
            # 尝试展开POI列表（原版逻辑）
            try:
                more_button = driver.find_elements('class name', 'M77dve')
                if more_button:
                    click_on_more_button(driver)
                    scroll_poi_section(driver)
            except:
                pass
            
            # 获取POI信息
            df = get_all_poi_info(driver)
            
            if df is not None and not df.empty:
                poi_count = len(df)
                
                # 获取坐标
                try:
                    final_url = wait_for_coords_url(driver, timeout=3)
                    if final_url:
                        lat, lng = get_coords(final_url)
                    else:
                        lat, lng = None, None
                except:
                    lat, lng = None, None
                
                # 安全地添加列（turbo版本的安全处理）
                try:
                    df = df.copy()
                    df['blt_name'] = place_name
                    df['lat'] = lat
                    df['lng'] = lng
                    
                    # 验证列完整性
                    required_cols = ['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng']
                    if not all(col in df.columns for col in required_cols):
                        print(f"警告: DataFrame列不完整，地址: {address[:50]}...")
                        return {
                            'data': None,
                            'is_building': True,
                            'poi_count': 0,
                            'status': 'column_error'
                        }
                    
                    print(f"{address[:50]}... | POI: {poi_count} | 状态: 已保存")
                    
                    return {
                        'data': df,
                        'is_building': True,
                        'poi_count': poi_count,
                        'status': 'success'
                    }
                except Exception as e:
                    print(f"警告: 数据处理失败，地址: {address[:50]}..., 错误: {e}")
                    return {
                        'data': None,
                        'is_building': True,
                        'poi_count': 0,
                        'status': 'data_processing_error'
                    }
            else:
                poi_count = 0
                is_building = True
                
                if check_building_type:
                    try:
                        place_type = get_building_type(driver)
                        is_building = place_type == '建筑物'
                        print(f"{address[:50]}... | 类型: {place_type} | POI: {poi_count}")
                    except:
                        print(f"{address[:50]}... | POI: {poi_count}")
                else:
                    print(f"{address[:50]}... | POI: {poi_count}")
                
                return {
                    'data': None,
                    'is_building': is_building,
                    'poi_count': 0,
                    'status': 'no_poi_data'
                }
                
        except Exception as e:
            return {
                'data': None,
                'is_building': False,
                'poi_count': 0,
                'status': f'error: {str(e)}'
            }
    
    # 保存警告系统（从turbo版本复制）
    def _save_warning(self, warning_type, address, error_msg, worker_id=None, extra_info=None):
        """保存警告信息到文件"""
        with self.warning_lock:
            warning_record = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'warning_type': warning_type,
                'address': address[:100] if address else 'unknown',
                'error_message': str(error_msg)[:200],
                'worker_id': worker_id,
                'extra_info': extra_info
            }
            self.warning_batch.append(warning_record)
            
            # 批量写入警告（每50条或程序结束时）
            if len(self.warning_batch) >= 50:
                self._flush_warnings()
    
    def _flush_warnings(self):
        """批量写入警告到CSV文件"""
        if not self.warning_batch or self.warnings_file is None:
            return
            
        try:
            warning_df = pd.DataFrame(self.warning_batch)
            file_exists = self.warnings_file.exists()
            warning_df.to_csv(
                self.warnings_file, 
                mode='a', 
                header=not file_exists, 
                index=False, 
                encoding='utf-8-sig'
            )
            print(f"已保存 {len(self.warning_batch)} 条警告记录到: {self.warnings_file}")
            self.warning_batch.clear()
        except Exception as e:
            print(f"警告文件写入失败: {e}")
    
    # no_poi_warnings系统（从turbo版本复制并优化）
    def _track_no_poi_result(self, result, batch_size=50):
        """跟踪no_poi结果，检测大范围非建筑物情况"""
        with self.no_poi_tracker_lock:
            total_processed = sum(len(batch_results) for batch_results in self.no_poi_batch_tracker.values())
            batch_id = total_processed // batch_size
            
            if batch_id not in self.no_poi_batch_tracker:
                self.no_poi_batch_tracker[batch_id] = []
            
            self.no_poi_batch_tracker[batch_id].append(result)
            
            # 当批次满了，检查是否需要生成警告
            if len(self.no_poi_batch_tracker[batch_id]) >= batch_size:
                self._check_no_poi_batch_warning(batch_id)
    
    def _check_no_poi_batch_warning(self, batch_id):
        """检查是否需要生成no_poi批次警告"""
        batch_results = self.no_poi_batch_tracker.get(batch_id, [])
        if not batch_results:
            return
        
        # 统计结果
        success_count = sum(1 for r in batch_results if r.get('success', False))
        total_count = len(batch_results)
        
        # 检查无POI数据的情况
        no_poi_count = sum(1 for r in batch_results 
                          if not r.get('success', False) and 
                          ('no_poi_data' in str(r.get('error', '')) or 
                           'status' in r and r['status'] == 'no_poi_data'))
        
        # 检查chōme格式地址的特殊情况
        chome_format_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'
        chome_format_no_poi = sum(1 for r in batch_results 
                                if not r.get('success', False) and
                                re.search(chome_format_pattern, str(r.get('address', '')), re.IGNORECASE) and
                                'no_poi_data' in str(r.get('error', '')))
        
        # 触发警告的条件：整个批次100%失败 或 chōme格式地址80%以上失败
        should_warn = (success_count == 0 and no_poi_count == total_count) or \
                     (chome_format_no_poi >= total_count * 0.8)
        
        if should_warn:
            self._generate_no_poi_warning(batch_id, batch_results, chome_format_no_poi > 0)
    
    def _generate_no_poi_warning(self, batch_id, batch_results, is_chome_format=False):
        """生成no_poi批次警告（从原版和turbo版本结合）"""
        district_name = getattr(self, 'current_district_name', '当前区域')
        
        # 输出醒目的警告信息
        warning_type = "chōme格式地址大范围非建筑物" if is_chome_format else "批次全部失败"
        print(f"\n{'='*70}")
        print(f"⚠️  警告: {warning_type}检测！")
        print(f"{'='*70}")
        print(f"区域: {district_name}")
        print(f"虚拟批次: {batch_id + 1}")
        print(f"地址数量: {len(batch_results)}")
        
        if is_chome_format:
            print(f"状态: chōme格式地址大范围非建筑物情况")
            print(f"\n特征:")
            print(f"  - 匹配格式: 数字-chōme-数字-数字+地名")
            print(f"  - 典型示例: 3-chōme-5-23+Ikejiri")
            print(f"  - 大部分地址无POI数据")
            print(f"  - 通常是住宅区或非商业区域")
        else:
            print(f"状态: 所有地址都未找到POI数据（100%失败）")
        
        print(f"\n可能的原因:")
        if is_chome_format:
            print(f"  1. chōme格式地址在Google Maps中定位困难")
            print(f"  2. 这类地址通常指向住宅区，商业POI稀少")
            print(f"  3. 地址数据可能需要格式转换")
            print(f"  4. Google Maps API对此类格式支持有限")
        else:
            print(f"  1. 地址格式不正确")
            print(f"  2. 地址数据已过期或无效") 
            print(f"  3. 该区域可能没有POI数据")
            print(f"  4. Google Maps API响应异常")
        
        print(f"\n建议操作:")
        if is_chome_format:
            print(f"  1. 检查CSV文件中chōme格式地址的占比")
            print(f"  2. 尝试将地址转换为标准日式地址格式")
            print(f"  3. 考虑过滤或预处理这类格式的地址")
            print(f"  4. 验证样本地址在Google Maps上的可访问性")
        else:
            print(f"  1. 检查输入CSV文件中的地址格式")
            print(f"  2. 验证几个样本地址是否能在Google Maps上正确定位")
            print(f"  3. 如果问题持续，考虑跳过该批次或更新地址数据")
        print(f"{'='*70}\n")
        
        # 记录详细信息到日志文件
        area_suffix = "_chome_format" if is_chome_format else ""
        log_file = self.no_poi_warnings_dir / f"{district_name}_batch_{batch_id + 1}{area_suffix}_warning.log"
        
        try:
            with open(log_file, 'w', encoding='utf-8') as f:
                f.write(f"警告日志 - {district_name} 批次 {batch_id + 1}\n")
                f.write(f"时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"批次大小: {len(batch_results)}\n")
                f.write(f"警告类型: {warning_type}\n")
                
                # 分析chōme格式地址的统计
                if is_chome_format:
                    chome_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'
                    chome_addresses = [r for r in batch_results 
                                     if re.search(chome_pattern, str(r.get('address', '')), re.IGNORECASE)]
                    f.write(f"chōme格式地址数量: {len(chome_addresses)}\n")
                    f.write(f"chōme格式占比: {(len(chome_addresses)/len(batch_results)*100):.1f}%\n")
                
                success_count = sum(1 for r in batch_results if r.get('success', False))
                f.write(f"成功率: {(success_count/len(batch_results)*100):.1f}%\n")
                f.write(f"失败率: {((len(batch_results)-success_count)/len(batch_results)*100):.1f}%\n")
                
                f.write(f"\n地址列表及结果:\n")
                f.write("-" * 80 + "\n")
                for i, r in enumerate(batch_results, 1):
                    status = "成功" if r.get('success', False) else "失败"
                    address = str(r.get('address', 'unknown'))[:60]
                    error = str(r.get('error', ''))[:40] if not r.get('success', False) else ""
                    f.write(f"{i:3d}. [{status}] {address}\n")
                    if error:
                        f.write(f"     错误: {error}\n")
            
            print(f"详细信息已保存到: {log_file}\n")
        except Exception as e:
            print(f"无法保存警告日志: {e}")
    
    # 进度管理（从原版复制，保持断点续传兼容性）
    def _save_progress(self, district_name, completed_count, total_count, total_success, total_errors):
        """保存进度"""
        if not self.enable_resume:
            return
            
        progress_data = {
            'district_name': district_name,
            'completed_count': completed_count,
            'total_count': total_count,
            'total_success': total_success,
            'total_errors': total_errors,
            'output_file': str(self.output_file),
            'timestamp': time.time()
        }
        
        with open(self.progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    def _load_progress(self, district_name):
        """加载进度"""
        if not self.enable_resume or not self.progress_file.exists():
            return None
            
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            if progress_data.get('district_name') == district_name:
                return progress_data
        except:
            pass
        return None
    
    def _cleanup_progress(self):
        """清理进度文件"""
        if self.progress_file and self.progress_file.exists():
            try:
                self.progress_file.unlink()
            except:
                pass
    
    def _extract_district_name(self, input_file):
        """提取区名"""
        filename = Path(input_file).stem
        if '区' in filename:
            return filename.split('区')[0] + '区'
        return 'unknown_district'
    
    def _batch_append_to_output_file(self, data_list):
        """批量写入文件（turbo版本的安全写入）"""
        if self.output_file is None or not data_list:
            return
        
        # 过滤和验证数据
        valid_data = []
        for data in data_list:
            if data is not None and isinstance(data, pd.DataFrame) and not data.empty:
                # 确保DataFrame有必要的列
                required_cols = ['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng']
                if all(col in data.columns for col in required_cols):
                    valid_data.append(data)
                else:
                    missing_cols = [col for col in required_cols if col not in data.columns]
                    error_msg = f"数据缺少必要列: {missing_cols}"
                    print(f"警告: {error_msg}，跳过")
                    self._save_warning('batch_write_column_missing', 'batch_data', error_msg)
        
        if not valid_data:
            return
            
        try:
            combined_df = pd.concat(valid_data, ignore_index=True)
            combined_df = combined_df.drop_duplicates(subset=['name', 'lat', 'lng'], keep='first')
            
            if not combined_df.empty:
                combined_df.to_csv(self.output_file, mode='a', header=False, index=False, encoding='utf-8-sig')
        except Exception as e:
            error_msg = f"批量写入失败: {str(e)}, 有效数据量: {len(valid_data)}, 原始数据量: {len(data_list)}"
            print(error_msg)
            self._save_warning('batch_write_failed', 'batch_operation', error_msg)
    
    def _final_deduplication(self):
        """最终去重"""
        if self.output_file is None or not self.output_file.exists():
            return
            
        try:
            print("正在进行最终去重处理...")
            df = pd.read_csv(self.output_file, encoding='utf-8-sig')
            original_count = len(df)
            
            df_deduped = df.drop_duplicates(subset=['name', 'lat', 'lng'], keep='first')
            final_count = len(df_deduped)
            
            if original_count > final_count:
                df_deduped.to_csv(self.output_file, index=False, encoding='utf-8-sig')
                print(f"去重完成: {original_count} → {final_count} (删除了 {original_count - final_count} 个重复项)")
            else:
                print("未发现重复数据")
        except Exception as e:
            print(f"去重处理失败: {e}")
    
    def crawl_from_csv_hybrid(self, input_file):
        """混合模式爬取CSV文件 - 结合原版和turbo的优势"""
        try:
            df = pd.read_csv(input_file)
        except Exception as e:
            print(f"读取CSV文件失败: {e}")
            return 0, 1
        
        # 创建地址对象（兼容原版的格式）
        addresses = []
        for index, row in df.iterrows():
            address_obj = {
                'primary': None,
                'secondary': None,
                'fallback': None,
                'index': index
            }
            
            # 优先级：FormattedAddress > ConvertedAddress > Address
            if 'FormattedAddress' in df.columns and pd.notna(row['FormattedAddress']) and row['FormattedAddress'].strip():
                address_obj['primary'] = row['FormattedAddress'].strip()
            elif 'ConvertedAddress' in df.columns and pd.notna(row['ConvertedAddress']) and row['ConvertedAddress'].strip():
                address_obj['primary'] = row['ConvertedAddress'].strip()
            elif 'Address' in df.columns and pd.notna(row['Address']):
                address_obj['primary'] = row['Address']
            
            # 设置备用地址
            if 'Address' in df.columns and pd.notna(row['Address']):
                address_obj['secondary'] = row['Address']
            
            if address_obj['primary'] or address_obj['secondary']:
                addresses.append(address_obj)
        
        district_name = self._extract_district_name(input_file)
        self.current_district_name = district_name
        self.progress_file = self.progress_dir / f"{district_name}_progress.json"
        
        # 设置警告文件
        self.warnings_file = Path(self.warnings_dir) / f"{district_name}_warnings_{int(time.time())}.csv"
        
        total_addresses = len(addresses)
        
        # 断点续传逻辑（保持原版兼容性）
        progress_data = self._load_progress(district_name)
        start_idx = 0
        
        if progress_data:
            # 兼容新旧格式
            completed_count = progress_data.get('completed_count') or progress_data.get('completed_batches', 0)
            if completed_count < total_addresses:
                start_idx = completed_count
                print(f"恢复进度: 从第 {start_idx + 1} 个地址继续")
                output_file_path = progress_data.get('output_file')
                if output_file_path and os.path.exists(output_file_path):
                    self.output_file = Path(output_file_path)
                else:
                    self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
                    header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
                    header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            else:
                print(f"{district_name} 任务已完成")
                return progress_data.get('total_success', 0), progress_data.get('total_errors', 0)
        else:
            self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        remaining_addresses = total_addresses - start_idx
        
        print(f"开始爬取 {district_name} {total_addresses} 个地址")
        print(f"使用Hybrid模式，{self.max_workers} 个高性能线程")
        print(f"Chrome驱动池: {self.driver_pool.max_drivers} 个实例")
        print(f"输出文件: {self.output_file}\n")
        
        # 重置全局统计
        with _global_stats_lock:
            _global_stats['success'] = 0
            _global_stats['errors'] = 0
            _global_stats['processing'] = 0
        
        # 准备任务
        tasks = [{'address': addresses[idx], 'index': idx} for idx in range(start_idx, total_addresses)]
        
        # 添加任务到调度器
        self.task_scheduler.add_tasks(tasks)
        
        # 启动任务分发
        feeder_thread = threading.Thread(target=self.task_scheduler.feed_tasks, daemon=True)
        feeder_thread.start()
        
        # 启动工作线程
        workers = []
        for i in range(self.max_workers):
            worker = HybridWorker(i, self.driver_pool, self.task_scheduler, self)
            worker.start()
            workers.append(worker)
        
        # 结果收集
        def collect_results():
            success_count = 0
            error_count = 0
            batch_data = []
            processed_count = 0
            last_save_time = time.time()
            last_progress_save = time.time()
            
            while processed_count < len(tasks):
                result = self.task_scheduler.get_result()
                if result is None:
                    time.sleep(0.1)
                    continue
                
                processed_count += 1
                
                if result['success']:
                    success_count += 1
                    # 验证数据有效性
                    data = result.get('data')
                    if data is not None and isinstance(data, pd.DataFrame) and not data.empty:
                        batch_data.append(data)
                    else:
                        address = result.get('address', 'unknown')
                        error_msg = f"成功结果但数据无效，数据类型: {type(data)}"
                        print(f"警告: {error_msg}，地址: {address[:50]}...")
                        self._save_warning('result_data_invalid', address, error_msg, result.get('worker_id'))
                else:
                    error_count += 1
                    # 保存来自worker的警告信息
                    warning_info = result.get('warning_info')
                    if warning_info:
                        self._save_warning(
                            warning_info.get('type', 'unknown'), 
                            result.get('address', 'unknown'), 
                            warning_info.get('message', 'unknown error'),
                            result.get('worker_id')
                        )
                
                # 跟踪no_poi结果，检测chōme等格式的大范围非建筑物情况
                self._track_no_poi_result(result)
                
                # 批量写入
                current_time = time.time()
                if len(batch_data) >= 20 or (batch_data and (current_time - last_save_time) > 5):
                    self._batch_append_to_output_file(batch_data)
                    batch_data = []
                    last_save_time = current_time
                
                # 进度保存
                if current_time - last_progress_save > 30:
                    self._save_progress(district_name, start_idx + processed_count, total_addresses, 
                                      success_count, error_count)
                    last_progress_save = current_time
                
                # 进度显示
                if processed_count % 50 == 0:
                    progress_percent = ((start_idx + processed_count) / total_addresses) * 100
                    print(f"进度: {start_idx + processed_count}/{total_addresses} ({progress_percent:.1f}%) - "
                          f"成功: {success_count}, 失败: {error_count}")
            
            # 写入剩余数据
            if batch_data:
                self._batch_append_to_output_file(batch_data)
            
            self._save_progress(district_name, start_idx + processed_count, total_addresses, 
                              success_count, error_count)
            
            return success_count, error_count
        
        # 运行结果收集
        try:
            final_success, final_errors = collect_results()
            
            # 停止调度器
            self.task_scheduler.stop()
            
            # 等待工作线程完成
            for worker in workers:
                worker.join(timeout=10)
            
        except KeyboardInterrupt:
            print("\n收到中断信号，正在安全退出...")
            self.task_scheduler.stop()
            for worker in workers:
                worker.join(timeout=5)
        finally:
            # 清理资源
            self.driver_pool.cleanup_all()
        
        # 获取全局统计（来自turbo版本）
        with _global_stats_lock:
            total_success = _global_stats['success']
            total_errors = _global_stats['errors']
        
        print(f"\n{district_name} 爬取完成！")
        print(f"总成功: {total_success}, 总失败: {total_errors}")
        print(f"数据已保存到: {self.output_file}")
        
        # 刷新剩余的警告信息
        if self.warning_batch:
            self._flush_warnings()
            print(f"警告记录已保存到: {self.warnings_file}")
        
        # 检查剩余的no_poi批次
        with self.no_poi_tracker_lock:
            for batch_id, batch_results in self.no_poi_batch_tracker.items():
                if batch_results:  # 有未检查的结果
                    self._check_no_poi_batch_warning(batch_id)
        
        self._final_deduplication()
        self._cleanup_progress()
        
        return total_success, total_errors

    # 状态管理方法（从原版复制，保持兼容性）
    def list_pending_tasks(self):
        """列出未完成任务"""
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
                # 兼容新旧格式
                completed = data.get('completed_count') or data.get('completed_batches', 0)
                total = data.get('total_count') or data.get('total_batches', 0)
                success = data['total_success']
                errors = data['total_errors']
                timestamp = data['timestamp']
                
                progress_percent = (completed / total) * 100 if total > 0 else 0
                time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
                
                print(f"区域: {district}")
                print(f"进度: {completed}/{total} ({progress_percent:.1f}%)")
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


def main():
    parser = argparse.ArgumentParser(description='POI爬虫 - Hybrid模式（高性能+稳定性）')
    parser.add_argument('input_files', nargs='*', help='输入CSV文件路径')
    parser.add_argument('--all', action='store_true', help='批量处理所有区文件')
    parser.add_argument('--pattern', type=str, help='使用通配符模式选择文件')
    parser.add_argument('--file-list', type=str, help='从文件中读取要处理的文件列表')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传功能')
    parser.add_argument('--workers', type=int, default=None, help='并发工作线程数（12核心默认：48线程）')
    parser.add_argument('--batch-size', type=int, default=50, help='批次大小')
    parser.add_argument('--status', action='store_true', help='查看未完成任务状态')
    parser.add_argument('--clean-progress', action='store_true', help='清理所有进度文件')
    
    args = parser.parse_args()
    
    # 创建爬虫实例
    crawler = ParallelPOICrawler(
        enable_resume=not args.no_resume, 
        max_workers=args.workers,
        batch_size=args.batch_size
    )
    
    if args.status:
        crawler.list_pending_tasks()
        return
    
    if args.clean_progress:
        crawler.clean_all_progress()
        return
    
    if not args.input_files and not args.all and not args.pattern and not args.file_list:
        print("用法:")
        print("  单个文件: python parallel_poi_crawler_hybrid.py <CSV文件>")
        print("  多个文件: python parallel_poi_crawler_hybrid.py <文件1> <文件2> ...")
        print('  通配符:   python parallel_poi_crawler_hybrid.py --pattern "*区_complete*.csv"')
        print("  文件列表: python parallel_poi_crawler_hybrid.py --file-list files.txt")
        print("  批量处理: python parallel_poi_crawler_hybrid.py --all")
        print("")
        print("Hybrid模式特性（最佳性能+稳定性）:")
        print("  - turbo版本的高性能线程池架构")
        print("  - 原版验证的稳定Chrome配置")
        print("  - 智能CPU核心数自适应（12核心=48线程）")
        print("  - 完整的断点续传和no_poi_warnings系统")
        print("  - chōme格式地址智能检测")
        print("  - 兼容原版输入输出格式")
        return
    
    # 收集要处理的文件列表（与原版保持一致）
    files_to_process = []
    
    if args.all:
        input_path = Path("data/input")
        csv_files = list(input_path.glob("*.csv"))
        files_to_process.extend([str(f) for f in csv_files])
        print(f"发现 {len(csv_files)} 个区文件")
    
    if args.input_files:
        files_to_process.extend(args.input_files)
    
    if args.pattern:
        import glob
        pattern_files = glob.glob(args.pattern)
        if pattern_files:
            files_to_process.extend(pattern_files)
            print(f"通配符 '{args.pattern}' 匹配到 {len(pattern_files)} 个文件")
        else:
            print(f"警告: 通配符 '{args.pattern}' 没有匹配到任何文件")
    
    if args.file_list:
        if os.path.exists(args.file_list):
            with open(args.file_list, 'r', encoding='utf-8') as f:
                list_files = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                files_to_process.extend(list_files)
                print(f"从 '{args.file_list}' 读取了 {len(list_files)} 个文件")
        else:
            print(f"警告: 文件列表 '{args.file_list}' 不存在")
    
    # 去重并验证文件
    files_to_process = list(dict.fromkeys(files_to_process))
    valid_files = []
    
    for file_path in files_to_process:
        if os.path.exists(file_path):
            valid_files.append(file_path)
        else:
            print(f"警告: 文件不存在 - {file_path}")
    
    if not valid_files:
        print("错误: 没有找到有效的文件进行处理")
        return
    
    # 处理文件
    print(f"准备处理 {len(valid_files)} 个文件...")
    for i, file_path in enumerate(valid_files):
        file_name = os.path.basename(file_path)
        print(f"\n{'='*60}")
        print(f"处理第 {i+1}/{len(valid_files)} 个文件: {file_name}")
        print(f"{'='*60}")
        
        try:
            success, errors = crawler.crawl_from_csv_hybrid(file_path)
            print(f"\n{file_name} 完成 - 成功: {success}, 失败: {errors}")
        except Exception as e:
            print(f"处理文件 {file_name} 时出错: {e}")
        
        print(f"{file_name} 处理完成\n")


if __name__ == "__main__":
    main()