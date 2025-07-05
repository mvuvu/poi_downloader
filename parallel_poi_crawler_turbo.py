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
from queue import Queue, Empty
import signal
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import random
from threading import Lock, RLock
from collections import deque
import gc

from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section

# 线程本地统计管理
thread_local_stats = threading.local()

class ChromeDriverPool:
    """Chrome驱动池管理器 - 线程安全"""
    def __init__(self, max_drivers=20):
        self.max_drivers = max_drivers
        self.pool = deque()
        self.lock = RLock()
        self.total_created = 0
        
    def create_optimized_driver(self):
        """创建高性能稳定的Chrome实例"""
        options = webdriver.ChromeOptions()
        
        # 高性能稳定配置
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        
        # 网络和性能优化
        options.add_argument('--disable-extensions')
        options.add_argument('--disable-default-apps')
        options.add_argument('--disable-sync')
        options.add_argument('--disable-translate')
        options.add_argument('--no-first-run')
        options.add_argument('--disable-popup-blocking')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-client-side-phishing-detection')
        
        # 内存优化
        options.add_argument('--memory-pressure-off')
        
        # 禁用资源加载提高速度
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,
                'plugins': 2,
                'popups': 2,
                'geolocation': 2,
                'notifications': 2,
                'media_stream': 2,
            }
        }
        options.add_experimental_option('prefs', prefs)
        options.page_load_strategy = 'eager'
        
        # 静默服务
        service = Service(
            ChromeDriverManager().install(),
            log_path='NUL' if os.name == 'nt' else '/dev/null',
            service_args=['--silent']
        )

        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(8)
        driver.implicitly_wait(1.5)
        
        return driver
    
    def get_driver(self):
        """获取一个可用的驱动实例"""
        with self.lock:
            # 尝试从池中获取可用的驱动
            while self.pool:
                driver = self.pool.popleft()
                try:
                    # 简单的健康检查
                    driver.window_handles
                    return driver
                except:
                    # 驱动已损坏，尝试关闭
                    try:
                        driver.quit()
                    except:
                        pass
            
            # 池中没有可用驱动，创建新的（如果未达到上限）
            if self.total_created < self.max_drivers:
                driver = self.create_optimized_driver()
                self.total_created += 1
                print(f"创建新Chrome实例 #{self.total_created}")
                return driver
            
            # 达到上限，等待其他线程释放驱动
            return None
    
    def return_driver(self, driver):
        """归还驱动到池中"""
        if driver is None:
            return
            
        with self.lock:
            try:
                # 清理浏览器状态
                driver.delete_all_cookies()
                driver.execute_script("window.localStorage.clear();")
                driver.execute_script("window.sessionStorage.clear();")
            except:
                # 清理失败，不归还到池中
                try:
                    driver.quit()
                except:
                    pass
                return
            
            self.pool.append(driver)
    
    def cleanup_all(self):
        """清理所有驱动"""
        with self.lock:
            # 清理池中的驱动
            while self.pool:
                driver = self.pool.popleft()
                try:
                    driver.quit()
                except:
                    pass
            
            print(f"Chrome驱动池已清理，共创建了 {self.total_created} 个实例")


class TurboTaskScheduler:
    """Turbo任务调度器 - 高并发任务分发"""
    def __init__(self, max_threads=48):
        self.max_threads = max_threads
        # 高效队列配置
        queue_size = max_threads * 8
        self.task_queue = Queue(maxsize=queue_size)  # FIFO队列确保公平处理
        self.result_queue = Queue()
        self.pending_tasks = deque()
        self.active_threads = 0
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
                # 尝试向队列添加任务（非阻塞）
                if not self.task_queue.full():
                    for _ in range(min(10, len(self.pending_tasks))):
                        if self.pending_tasks:
                            task = self.pending_tasks.popleft()
                            self.task_queue.put(task, timeout=0.1)
                        else:
                            break
                time.sleep(0.01)  # 短暂休息
            except:
                time.sleep(0.1)
    
    def get_task(self):
        """获取一个任务"""
        try:
            return self.task_queue.get(timeout=0.5)
        except Empty:
            return None
    
    def put_result(self, result):
        """提交结果"""
        self.result_queue.put(result)
        with self.lock:
            self.completed_count += 1
    
    def get_result(self):
        """获取结果"""
        try:
            return self.result_queue.get(timeout=0.1)
        except Empty:
            return None
    
    def stop(self):
        """停止调度器"""
        self.stop_event.set()


class TurboWorker(threading.Thread):
    """Turbo工作线程"""
    def __init__(self, worker_id, driver_pool, task_scheduler, crawler):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.driver_pool = driver_pool
        self.task_scheduler = task_scheduler
        self.crawler = crawler
        self.processed_count = 0
        self.current_driver = None
        self.stats = {'success': 0, 'errors': 0}  # 线程本地统计
        
    def run(self):
        """线程主循环"""
        print(f"Worker {self.worker_id} 启动")
        
        while not self.task_scheduler.stop_event.is_set():
            # 获取任务
            task = self.task_scheduler.get_task()
            if task is None:
                # 没有任务，短暂休息后继续
                time.sleep(0.05)
                continue
            
            # 获取Chrome驱动
            if self.current_driver is None:
                self.current_driver = self.driver_pool.get_driver()
                if self.current_driver is None:
                    # 无法获取驱动，将任务放回队列
                    try:
                        self.task_scheduler.task_queue.put(task, timeout=0.1)
                    except:
                        pass
                    time.sleep(0.1)
                    continue
            
            # 处理任务
            try:
                result = self.process_task(task)
                self.task_scheduler.put_result(result)
                self.processed_count += 1
                
                # 更新线程本地统计
                if result['success']:
                    self.stats['success'] += 1
                else:
                    self.stats['errors'] += 1
                
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
                
                self.stats['errors'] += 1
                
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
        """处理单个任务"""
        address_obj = task['address']
        idx = task['index']
        
        start_time = time.time()
        
        # 获取地址
        current_address = address_obj.get('primary') if isinstance(address_obj, dict) else address_obj
        used_address = current_address
        retry_info = []
        
        # 处理地址
        result = self.crawler._crawl_poi_info_turbo(current_address, self.current_driver)
        
        # 检查是否需要重试
        should_retry = (isinstance(result, dict) and 
                       not result.get('is_building', False) and
                       result.get('poi_count', 0) == 0 and
                       result.get('status') != 'hotel_category_page_skipped')
        
        if should_retry and isinstance(address_obj, dict) and address_obj.get('secondary'):
            retry_result = self.crawler._crawl_poi_info_turbo(address_obj['secondary'], self.current_driver, check_building_type=False)
            retry_info.append(f"重试: {address_obj['secondary']}")
            
            if (isinstance(retry_result, dict) and 
                retry_result.get('status') == 'success'):
                result = retry_result
                used_address = address_obj['secondary']
        
        processing_time = time.time() - start_time
        
        # 构建结果
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
            error_msg = '未找到POI数据' if isinstance(result, dict) and result.get('poi_count', 0) == 0 else '处理异常'
            return {
                'success': False,
                'error': error_msg,
                'address': used_address,
                'worker_id': self.worker_id,
                'index': idx,
                'retry_info': retry_info,
                'processing_time': processing_time
            }


logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')


class ParallelPOICrawler:
    def __init__(self, max_workers=None, output_dir="data/output", batch_size=50, enable_resume=True):
        # 科学的高性能配置
        cpu_count = mp.cpu_count()
        if max_workers is None:
            if cpu_count >= 12:
                # 12核心以上：使用CPU数量*4，最多48线程
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
        
        # 警告保存系统
        self.warnings_dir = Path("data/warnings")
        self.warnings_dir.mkdir(parents=True, exist_ok=True)
        self.warnings_file = None
        self.warning_batch = []
        self.warning_lock = Lock()
        
        # no_poi_warnings 系统
        self.no_poi_warnings_dir = Path("no_poi_warnings")
        self.no_poi_warnings_dir.mkdir(exist_ok=True)
        self.no_poi_batch_tracker = {}  # 跟踪每个批次的结果
        self.no_poi_tracker_lock = Lock()
        
        # 高效Chrome驱动池配置
        chrome_pool_size = min(20, max(8, self.max_workers // 2))
        
        # 初始化组件
        self.driver_pool = ChromeDriverPool(max_drivers=chrome_pool_size)
        self.task_scheduler = TurboTaskScheduler(max_threads=self.max_workers)
        
    def _crawl_poi_info_turbo(self, address, driver, check_building_type=True):
        """Turbo模式的POI信息爬取"""
        url = f'https://www.google.com/maps/place/{address}'
        
        try:
            driver.get(url)
            
            # 快速检查酒店类别页面
            if self._has_category_header(driver):
                return {
                    'data': None,
                    'is_building': False,
                    'poi_count': 0,
                    'status': 'hotel_category_page_skipped'
                }
            
            # 恢复充足等待时间确保页面完全加载
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            poi_count = 0
            place_type = 'unknown'
            is_building = False
            
            # 获取地点名称
            try:
                place_name = get_building_name(driver)
            except Exception:
                place_name = self._get_fallback_location_name(driver, address) or 'Unknown Location'
            
            # 尝试展开POI列表
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
                
                # 快速获取坐标
                try:
                    final_url = wait_for_coords_url(driver, timeout=2)
                    if final_url:
                        lat, lng = get_coords(final_url)
                    else:
                        lat, lng = None, None
                except:
                    lat, lng = None, None
                
                # 添加列信息
                try:
                    df['blt_name'] = place_name
                    df['lat'] = lat
                    df['lng'] = lng
                    
                    # 验证列完整性
                    required_cols = ['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng']
                    if not all(col in df.columns for col in required_cols):
                        error_msg = f"DataFrame列不完整，缺少: {[col for col in required_cols if col not in df.columns]}"
                        print(f"警告: {error_msg}，地址: {address[:50]}...")
                        # 注意：这里无法直接调用self._save_warning，因为这是在worker中
                        return {
                            'data': None,
                            'is_building': True,
                            'poi_count': 0,
                            'status': 'column_error',
                            'warning_info': {'type': 'column_incomplete', 'message': error_msg}
                        }
                    
                    print(f"{address}  | POI: {poi_count} | 状态: 已保存")
                    
                    return {
                        'data': df,
                        'is_building': True,
                        'poi_count': poi_count,
                        'status': 'success'
                    }
                except Exception as e:
                    error_msg = f"数据处理失败: {str(e)}"
                    print(f"警告: {error_msg}，地址: {address[:50]}...")
                    return {
                        'data': None,
                        'is_building': True,
                        'poi_count': 0,
                        'status': 'data_processing_error',
                        'warning_info': {'type': 'data_processing_error', 'message': error_msg}
                    }
            else:
                if check_building_type:
                    try:
                        place_type = get_building_type(driver)
                        is_building = place_type == '建筑物'
                        print(f"{address}  | 类型: {place_type} | POI: {poi_count}")
                        
                        # 检查是否是chōme格式地址的非建筑物情况
                        import re
                        chome_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'
                        if not is_building and re.search(chome_pattern, address, re.IGNORECASE):
                            warning_info = {
                                'type': 'chome_format_non_building', 
                                'message': f"chōme格式地址非建筑物: {place_type}, 地址格式: {address}"
                            }
                            return {
                                'data': None,
                                'is_building': is_building,
                                'poi_count': 0,
                                'status': 'no_poi_data',
                                'warning_info': warning_info
                            }
                    except:
                        print(f"{address}  | POI: {poi_count}")
                else:
                    print(f"{address}  | POI: {poi_count}")
                
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
    
    def _has_category_header(self, driver):
        """检查是否是酒店类别页面"""
        try:
            category_headers = driver.find_elements("css selector", "h2.kPvgOb.fontHeadlineSmall")
            for header in category_headers:
                if header.text.strip() == "酒店":
                    return True
            return False
        except:
            return False
    
    def _get_fallback_location_name(self, driver, address):
        """获取备用地点名称"""
        try:
            title = driver.title
            if title and title != "Google Maps" and "Google" not in title:
                clean_title = title.replace(" - Google Maps", "").replace(" - Google 地图", "").strip()
                if clean_title:
                    return clean_title
            return None
        except:
            return None
    
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
        """批量写入文件"""
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
    
    def _save_warning(self, warning_type, address, error_msg, worker_id=None, extra_info=None):
        """保存警告信息到文件"""
        with self.warning_lock:
            warning_record = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'warning_type': warning_type,
                'address': address[:100] if address else 'unknown',  # 限制地址长度
                'error_message': str(error_msg)[:200],  # 限制错误信息长度
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
            # 检查文件是否存在以决定是否写入头部
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
    
    def _track_no_poi_result(self, result, batch_size=50):
        """跟踪no_poi结果，检测大范围非建筑物情况"""
        with self.no_poi_tracker_lock:
            # 计算虚拟批次ID（基于处理顺序）
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
        
        # 检查特定格式地址的非建筑物情况 (如: 3-chōme-5-23+地名)
        import re
        chome_format_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'  # 匹配 "数字-chōme-数字-数字+地名" 格式
        
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
        """生成no_poi批次警告"""
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
        print(f"  1. chōme格式地址在Google Maps中定位困难")
        print(f"  2. 这类地址通常指向住宅区，商业POI稀少")
        print(f"  3. 地址数据可能需要格式转换")
        print(f"  4. Google Maps API对此类格式支持有限")
        print(f"\n建议操作:")
        print(f"  1. 检查CSV文件中chōme格式地址的占比")
        print(f"  2. 尝试将地址转换为标准日式地址格式")
        print(f"  3. 考虑过滤或预处理这类格式的地址")
        print(f"  4. 验证样本地址在Google Maps上的可访问性")
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
                    import re
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
    
    def crawl_from_csv_turbo(self, input_file):
        """Turbo模式爬取CSV文件"""
        try:
            df = pd.read_csv(input_file)
        except Exception as e:
            print(f"读取CSV文件失败: {e}")
            return 0, 1
        
        # 创建地址对象
        addresses = []
        for index, row in df.iterrows():
            address_obj = {
                'primary': None,
                'secondary': None,
                'fallback': None,
                'index': index
            }
            
            if 'FormattedAddress' in df.columns and pd.notna(row['FormattedAddress']) and row['FormattedAddress'].strip():
                address_obj['primary'] = row['FormattedAddress'].strip()
            
            if 'Address' in df.columns and pd.notna(row['Address']):
                address_obj['secondary'] = row['Address']
            
            if 'ConvertedAddress' in df.columns and pd.notna(row['ConvertedAddress']) and row['ConvertedAddress'].strip():
                address_obj['fallback'] = row['ConvertedAddress'].strip()
            
            if not address_obj['primary'] and address_obj['secondary']:
                address_obj['primary'] = address_obj['secondary']
                address_obj['secondary'] = address_obj['fallback']
                address_obj['fallback'] = None
            elif not address_obj['primary'] and address_obj['fallback']:
                address_obj['primary'] = address_obj['fallback']
                address_obj['fallback'] = None
            
            addresses.append(address_obj)
        
        district_name = self._extract_district_name(input_file)
        self.current_district_name = district_name
        self.progress_file = self.progress_dir / f"{district_name}_progress.json"
        
        # 设置警告文件
        self.warnings_file = Path(self.warnings_dir) / f"{district_name}_warnings_{int(time.time())}.csv"
        
        total_addresses = len(addresses)
        
        # 断点续传逻辑
        progress_data = self._load_progress(district_name)
        start_idx = 0
        total_success = 0
        total_errors = 0
        
        try:
            if progress_data:
                completed_count = progress_data.get('completed_count') or progress_data.get('completed_batches', 0)
                
                if 'completed_batches' in progress_data and 'completed_count' not in progress_data:
                    batch_size = progress_data.get('batch_size', 150)
                    completed_count = progress_data['completed_batches'] * batch_size
                    print(f"检测到旧版本进度文件，估算已处理地址数: {completed_count}")
                
                completed_count = min(completed_count, total_addresses - 1)
                
                if completed_count >= total_addresses:
                    print(f"{district_name} 任务已完成，跳过")
                    self._cleanup_progress()
                    return progress_data.get('total_success', 0), progress_data.get('total_errors', 0)
                
                print(f"发现未完成的{district_name}爬取任务")
                print(f"上次进度: {completed_count}/{total_addresses} ({completed_count/total_addresses*100:.1f}%)")
                print(f"已完成: 成功 {progress_data.get('total_success', 0)}, 失败 {progress_data.get('total_errors', 0)}")
                print(f"从第 {completed_count+1} 个地址继续...")
                
                start_idx = completed_count
                total_success = progress_data.get('total_success', 0)
                total_errors = progress_data.get('total_errors', 0)
                
                output_file_path = progress_data.get('output_file')
                if output_file_path and os.path.exists(output_file_path):
                    self.output_file = Path(output_file_path)
                    print(f"恢复输出文件: {self.output_file}")
                else:
                    self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
                    header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
                    header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            else:
                print(f"开始新的{district_name}爬取任务")
                self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
                header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
                header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        except Exception as e:
            print(f"断点续传恢复失败: {e}")
            print(f"将重新开始处理 {district_name}")
            self._cleanup_progress()
            start_idx = 0
            total_success = 0
            total_errors = 0
            self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        remaining_addresses = total_addresses - start_idx
        
        print(f"开始爬取 {district_name} {total_addresses} 个地址")
        print(f"使用优化模式，{self.max_workers} 个并发线程")
        print(f"Chrome驱动池: {self.driver_pool.max_drivers} 个实例")
        print(f"输出文件: {self.output_file}\n")
        
        # 准备任务
        tasks = []
        for idx in range(start_idx, total_addresses):
            tasks.append({
                'address': addresses[idx],
                'index': idx
            })
        
        # 添加任务到调度器
        self.task_scheduler.add_tasks(tasks)
        
        # 启动任务分发线程
        feeder_thread = threading.Thread(target=self.task_scheduler.feed_tasks, daemon=True)
        feeder_thread.start()
        
        # 启动工作线程
        workers = []
        for i in range(self.max_workers):
            worker = TurboWorker(i, self.driver_pool, self.task_scheduler, self)
            worker.start()
            workers.append(worker)
        
        # 启动结果收集线程
        def collect_results():
            success_count = 0
            error_count = 0
            batch_data = []
            processed_count = 0
            last_save_time = time.time()
            last_progress_save = time.time()
            
            while processed_count < len(tasks) or not self.task_scheduler.result_queue.empty():
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
                        error_msg = f"成功结果但数据无效，数据类型: {type(data)}, 是否为空: {data is None or (hasattr(data, 'empty') and data.empty)}"
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
                
                # 跟踪no_poi结果，检测ikejiri等区域的大范围非建筑物情况
                self._track_no_poi_result(result)
                
                # 批量写入 - 平衡性能和IO
                current_time = time.time()
                if len(batch_data) >= 25 or (batch_data and (current_time - last_save_time) > 8):
                    self._batch_append_to_output_file(batch_data)
                    batch_data = []
                    last_save_time = current_time
                
                # 进度保存 - 2分钟间隔
                if current_time - last_progress_save > 120:
                    self._save_progress(district_name, start_idx + processed_count, total_addresses, 
                                      total_success + success_count, total_errors + error_count)
                    last_progress_save = current_time
                
                # 进度显示
                if processed_count % 100 == 0:
                    progress_percent = ((start_idx + processed_count) / total_addresses) * 100
                    print(f"进度: {start_idx + processed_count}/{total_addresses} ({progress_percent:.1f}%) - "
                          f"成功: {success_count}, 失败: {error_count}")
            
            # 写入剩余数据
            if batch_data:
                self._batch_append_to_output_file(batch_data)
            
            self._save_progress(district_name, start_idx + processed_count, total_addresses, 
                              total_success + success_count, total_errors + error_count)
            
            return success_count, error_count
        
        # 运行结果收集
        try:
            final_success, final_errors = collect_results()
            
            # 停止调度器
            self.task_scheduler.stop()
            
            # 等待工作线程完成
            for worker in workers:
                worker.join(timeout=5)
            
        except KeyboardInterrupt:
            print("\n收到中断信号，正在安全退出...")
            self.task_scheduler.stop()
            for worker in workers:
                worker.join(timeout=3)
        finally:
            # 清理资源
            self.driver_pool.cleanup_all()
        
        # 汇总所有worker的统计
        total_success = final_success
        total_errors = final_errors
        
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
                completed = data['completed_count']
                total = data['total_count']
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

    def crawl_all_districts(self, input_dir="data/input"):
        """批量处理所有区文件"""
        input_path = Path(input_dir)
        csv_files = list(input_path.glob("*.csv"))
        
        if not csv_files:
            print(f"在 {input_dir} 目录中没有找到CSV文件")
            return
        
        print(f"发现 {len(csv_files)} 个区文件，开始批量处理（优化模式）...\n")
        
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
                success, errors = self.crawl_from_csv_turbo(csv_file)
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

    def crawl_multiple_files(self, file_paths):
        """处理多个指定文件"""
        print(f"准备处理 {len(file_paths)} 个文件（优化模式）...\n")
        
        all_success = 0
        all_errors = 0
        processed_files = []
        
        start_time = time.time()
        
        for i, file_path in enumerate(file_paths):
            file_name = os.path.basename(file_path)
            print(f"{'='*60}")
            print(f"处理第 {i+1}/{len(file_paths)} 个文件: {file_name}")
            print(f"{'='*60}")
            
            try:
                success, errors = self.crawl_from_csv_turbo(file_path)
                all_success += success
                all_errors += errors
                processed_files.append(f"{file_name}: 成功{success}, 失败{errors}")
                print(f"\n{file_name} 完成 - 成功: {success}, 失败: {errors}\n")
            except Exception as e:
                print(f"处理文件 {file_name} 时出错: {e}")
                processed_files.append(f"{file_name}: 处理失败")
                continue
        
        total_time = time.time() - start_time
        
        print(f"{'='*60}")
        print(f"所有文件处理完成！")
        print(f"{'='*60}")
        print(f"总耗时: {total_time/60:.1f} 分钟")
        print(f"总成功: {all_success}")
        print(f"总失败: {all_errors}")
        print(f"处理了 {len(processed_files)} 个文件:")
        
        for file_summary in processed_files:
            print(f"  {file_summary}")


def main():
    parser = argparse.ArgumentParser(description='POI爬虫 - 优化版本')
    parser.add_argument('input_files', nargs='*', help='输入CSV文件路径（可以指定多个）')
    parser.add_argument('--all', action='store_true', help='批量处理所有区文件')
    parser.add_argument('--pattern', type=str, help='使用通配符模式选择文件，如 "*区_complete*.csv"')
    parser.add_argument('--file-list', type=str, help='从文件中读取要处理的文件列表（每行一个文件路径）')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传功能')
    parser.add_argument('--workers', type=int, default=None, help='并发工作线程数（默认：CPU核心数×2）')
    parser.add_argument('--batch-size', type=int, default=150, help='批次大小')
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
    
    if not args.input_files and not args.all and not args.pattern and not args.file_list:
        print("用法:")
        print("  单个文件: python parallel_poi_crawler_turbo.py <输入CSV文件> [选项]")
        print("  多个文件: python parallel_poi_crawler_turbo.py <文件1> <文件2> ... [选项]")
        print('  通配符:   python parallel_poi_crawler_turbo.py --pattern "*区_complete*.csv" [选项]')
        print("  文件列表: python parallel_poi_crawler_turbo.py --file-list files.txt [选项]")
        print("  批量处理: python parallel_poi_crawler_turbo.py --all [选项]")
        print("  进度管理: python parallel_poi_crawler_turbo.py --status | --clean-progress")
        print("")
        print("选项:")
        print("  --pattern PATTERN  使用通配符模式选择文件")
        print("  --file-list FILE   从文件中读取文件列表")
        print("  --no-resume        禁用断点续传功能")
        print("  --workers N        设置并发工作线程数（推荐：CPU核心数*4-8）")
        print("  --batch-size N     设置批次大小")
        print("  --status          查看未完成任务状态")
        print("  --clean-progress  清理所有进度文件")
        print("")
        print("优化特性：")
        print("  - 合理的并发线程数（CPU核心数×2）")
        print("  - 稳定的Chrome驱动池管理")
        print("  - FIFO任务队列确保公平处理")
        print("  - 高效的资源利用和内存管理")
        print("  - 完整的重试机制和错误处理")
        return
    
    # 创建爬虫实例
    enable_resume = not args.no_resume
    crawler = ParallelPOICrawler(
        max_workers=args.workers, 
        batch_size=args.batch_size,
        enable_resume=enable_resume
    )
    
    print(f"启动优化模式，使用 {crawler.max_workers} 个并发线程")
    print(f"Chrome驱动池: {crawler.driver_pool.max_drivers} 个实例")
    
    # 收集要处理的文件列表
    files_to_process = []
    
    if args.all:
        crawler.crawl_all_districts()
        return
    
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
    
    if len(valid_files) == 1:
        crawler.crawl_from_csv_turbo(valid_files[0])
    else:
        crawler.crawl_multiple_files(valid_files)


if __name__ == "__main__":
    main()