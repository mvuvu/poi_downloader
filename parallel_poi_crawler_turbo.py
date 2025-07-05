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
import concurrent.futures
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
import psutil  # 用于内存监控
from multiprocessing import Process, Queue as MPQueue, Event as MPEvent
import queue

from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section

# 线程本地统计管理
thread_local_stats = threading.local()

class ChromeDriverPool:
    """Chrome驱动池管理器 - 线程安全，预分配机制"""
    def __init__(self, max_drivers=30):
        self.max_drivers = max_drivers
        self.pool = deque()
        self.lock = RLock()
        self.total_created = 0
        self.reserved_drivers = {}  # worker_id -> driver 预分配映射
        self.driver_usage_count = {}  # driver -> usage_count 使用计数
        self.driver_max_tasks = 100  # 每个driver最多处理100个任务后重启
        
    def create_optimized_driver(self):
        """创建高性能稳定的Chrome实例"""
        # 设置环境变量禁用GPU
        import os
        os.environ['LIBGL_ALWAYS_SOFTWARE'] = '1'  # 强制使用软件渲染
        
        options = webdriver.ChromeOptions()
        
        # 高性能稳定配置
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        # 强力禁用GPU相关功能
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-gpu-sandbox')
        options.add_argument('--disable-gpu-rasterization')
        options.add_argument('--disable-accelerated-2d-canvas')
        options.add_argument('--disable-accelerated-video-decode')
        options.add_argument('--disable-accelerated-video-encode')
        options.add_argument('--disable-gpu-memory-buffer-video-frames')
        options.add_argument('--disable-gpu-compositing')
        options.add_argument('--use-gl=swiftshader')  # 使用SwiftShader软件渲染
        options.add_argument('--enable-software-rasterizer')  # 强制使用软件渲染
        
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
        
        # 内存优化 - 严格控制每个Chrome实例内存使用
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max-memory-in-mb=350')  # 保守的350MB限制，30个实例约10.5GB
        options.add_argument('--aggressive-cache-discard')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-ipc-flooding-protection')
        options.add_argument('--max-old-space-size=256')  # V8引擎内存限制
        # VizDisplayCompositor已被新的GPU禁用参数覆盖，移除避免冲突
        options.add_argument('--disable-canvas-aa')  # 减少画布内存
        options.add_argument('--disable-2d-canvas-clip-aa')  # 减少2D画布内存
        
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

        try:
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(8)
            driver.implicitly_wait(1.5)
            
            # 测试Chrome实例是否可用
            driver.get("data:,")  # 加载空页面测试
            
            return driver
        except Exception as e:
            pass  # 静默处理驱动创建失败
            raise e
    
    def get_driver(self):
        """获取一个可用的驱动实例"""
        with self.lock:
            # 尝试从池中获取可用的驱动
            while self.pool:
                driver = self.pool.popleft()
                try:
                    # 简单的健康检查
                    driver.window_handles
                    pass  # 静默获取驱动
                    return driver
                except:
                    # 驱动已损坏，尝试关闭
                    pass  # 静默处理损坏驱动
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
            pass  # 静默处理驱动池已满
            return None
    
    def increment_driver_usage(self, driver):
        """增加driver使用计数，检查是否需要重启"""
        with self.lock:
            if driver in self.driver_usage_count:
                self.driver_usage_count[driver] += 1
                # 检查是否超过生命周期
                if self.driver_usage_count[driver] >= self.driver_max_tasks:
                    print(f"Driver达到生命周期限制({self.driver_max_tasks}个任务)，准备重启")
                    return True  # 需要重启
            else:
                self.driver_usage_count[driver] = 1
            return False  # 不需要重启
    
    def restart_driver(self, old_driver, worker_id=None):
        """重启driver（生命周期管理）"""
        with self.lock:
            # 强制释放旧driver
            self._force_quit_driver_internal(old_driver)
            
            # 从计数中移除
            if old_driver in self.driver_usage_count:
                del self.driver_usage_count[old_driver]
            
            # 从预分配映射中移除
            if worker_id and worker_id in self.reserved_drivers:
                del self.reserved_drivers[worker_id]
            
            # 创建新的driver
            if self.total_created < self.max_drivers:
                new_driver = self.create_optimized_driver()
                if worker_id:
                    self.reserved_drivers[worker_id] = new_driver
                    self.driver_usage_count[new_driver] = 0
                print(f"Driver重启完成 (Worker: {worker_id})")
                return new_driver
            return None
    
    def return_driver(self, driver):
        """归还驱动到池中"""
        if driver is None:
            return
            
        with self.lock:
            try:
                # 简化清理操作，快速归还
                driver.get("data:,")  # 导航到空页面清理状态
                self.pool.append(driver)
                
                # 定期检查Chrome进程数量
                if len(self.pool) % 10 == 0:  # 每归还10个驱动检查一次
                    self._check_chrome_process_count()
                    
            except Exception as e:
                # 清理失败，不归还到池中
                self._force_quit_driver(driver)
    
    def release_driver(self, driver):
        """强制释放并销毁driver - 确保彻底清理"""
        if driver is None:
            return
            
        with self.lock:
            # 从池中移除此driver（如果存在）
            try:
                while driver in self.pool:
                    self.pool.remove(driver)
            except ValueError:
                pass  # driver不在池中
            
            # 强制销毁driver
            try:
                driver.quit()
                print(f"✅ 强制销毁driver成功")
            except Exception as e:
                print(f"❌ 无法关闭driver: {e}")
                # 即使quit失败，也尝试强制停止service
                try:
                    if hasattr(driver, 'service') and driver.service:
                        driver.service.stop()
                        print(f"🔧 强制停止driver service")
                except Exception as e2:
                    print(f"❌ 无法停止driver service: {e2}")
            
            # 更新统计
            self.total_created = max(0, self.total_created - 1)
    
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
            
            # 强制清理残留的Chrome进程
            self._force_cleanup_chrome_processes()
    
    def _force_quit_driver(self, driver):
        """强制关闭Chrome驱动"""
        try:
            # 首先尝试正常关闭
            driver.quit()
        except:
            pass
        
        try:
            # 如果有service，强制停止
            if hasattr(driver, 'service') and driver.service:
                driver.service.stop()
        except:
            pass
    
    def _force_quit_driver_internal(self, driver):
        """内部使用的强制关闭driver（无锁版本）"""
        try:
            driver.quit()
            print(f"✅ 强制关闭driver")
        except Exception as e:
            print(f"❌ 关闭driver失败: {e}")
            # 即使quit失败，也尝试强制停止service
            try:
                if hasattr(driver, 'service') and driver.service:
                    driver.service.stop()
                    print(f"🔧 强制停止driver service")
            except Exception as e2:
                print(f"❌ 停止driver service失败: {e2}")
    
    def _check_chrome_process_count(self):
        """检查Chrome进程数量，超过阈值时清理"""
        try:
            import psutil
            chrome_count = 0
            for proc in psutil.process_iter(['name', 'cmdline']):
                try:
                    proc_name = proc.info['name'].lower()
                    if 'chrome' in proc_name or 'chromium' in proc_name:
                        cmdline = ' '.join(proc.info.get('cmdline', []))
                        if '--headless' in cmdline or '--no-sandbox' in cmdline:
                            chrome_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            if chrome_count > 50:  # 超过50个Chrome进程时警告并清理
                print(f"⚠️  Chrome进程过多: {chrome_count}个，开始自动清理...")
                self._force_cleanup_chrome_processes()
                
        except Exception:
            pass  # 静默处理检查错误
    
    def _force_cleanup_chrome_processes(self):
        """强制清理残留的Chrome进程"""
        try:
            import psutil
            chrome_procs = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    proc_name = proc.info['name'].lower()
                    if 'chrome' in proc_name or 'chromium' in proc_name:
                        # 只清理爬虫相关的Chrome进程，避免误杀用户浏览器
                        cmdline = ' '.join(proc.info.get('cmdline', []))
                        if '--headless' in cmdline or '--no-sandbox' in cmdline:
                            chrome_procs.append(proc)
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            if len(chrome_procs) > 30:  # 超过30个headless Chrome进程时清理
                print(f"🧹 发现 {len(chrome_procs)} 个headless Chrome进程，开始清理...")
                killed_count = 0
                for proc in chrome_procs:
                    try:
                        proc.terminate()
                        killed_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                # 等待进程正常退出
                time.sleep(2)
                
                # 强制杀死仍然存在的进程
                for proc in chrome_procs:
                    try:
                        if proc.is_running():
                            proc.kill()
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                
                print(f"✅ 清理了 {killed_count} 个Chrome进程")
                
        except Exception as e:
            print(f"Chrome进程清理失败: {e}")


class TurboTaskScheduler:
    """Turbo任务调度器 - 高并发任务分发"""
    def __init__(self, max_threads=36):
        self.max_threads = max_threads
        # 高效队列配置 - 根据最佳实践调整队列大小
        queue_size = min(300, max_threads * 8)  # 限制队列最大300个任务
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
                time.sleep(0.01)
                continue
            
            # 获取Chrome驱动 - 带重试机制
            if self.current_driver is None:
                # 重试机制：最多等待3次，每次0.5秒
                for retry_attempt in range(3):
                    self.current_driver = self.driver_pool.get_driver()
                    if self.current_driver is not None:
                        pass  # 静默处理重试成功
                        break
                    if retry_attempt < 2:  # 不是最后一次重试
                        pass  # 静默处理驱动池重试
                        time.sleep(0.5)  # 等待驱动释放
                
                if self.current_driver is None:
                    # 重试后仍无法获取驱动，记录失败
                    pass  # 静默处理驱动获取失败
                    failed_result = {
                        'success': False,
                        'error': f'Chrome驱动池已满，重试3次后仍无法获取驱动',
                        'address': str(task.get('address', 'unknown')),
                        'worker_id': self.worker_id,
                        'index': task.get('index', -1),
                        'retry_info': 'driver_pool_exhausted_after_retry'
                    }
                    self.task_scheduler.put_result(failed_result)
                    self.stats['errors'] += 1
                    time.sleep(1.0)  # 较长等待避免CPU空转，等待驱动释放
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
                
                # 检查Driver生命周期（每次任务完成后）
                if self.current_driver and result['success']:
                    needs_restart = self.driver_pool.increment_driver_usage(self.current_driver)
                    if needs_restart:
                        # Driver达到生命周期，重启
                        old_driver = self.current_driver
                        self.current_driver = self.driver_pool.restart_driver(old_driver, self.worker_id)
                        if self.current_driver is None:
                            print(f"Worker {self.worker_id}: Driver重启失败，将在下次任务时重新获取")
                
                # 定期报告进度和内存清理
                if self.processed_count % 20 == 0:
                    print(f"Worker {self.worker_id}: 已处理 {self.processed_count} 个任务")
                    # 定期清理Chrome缓存
                    if self.current_driver and self.processed_count % 50 == 0:
                        try:
                            self.current_driver.execute_script("window.gc();")  # 强制垃圾回收
                            self.current_driver.delete_all_cookies()  # 清理cookies
                        except:
                            pass
                
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
                
                # Chrome可能有问题，强制释放并重新获取
                if self.current_driver:
                    self.driver_pool.release_driver(self.current_driver)
                    self.current_driver = None
        
        # 强制释放驱动 - 确保彻底清理
        if self.current_driver:
            self.driver_pool.release_driver(self.current_driver)
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
        
        # 构建结果 - 区分技术成功和数据结果
        if (isinstance(result, dict) and 
            result.get('status') == 'success'):  # 只要技术成功就算成功
            return {
                'success': True,
                'data': result.get('data'),  # data可能为None，这是正常的
                'poi_count': result.get('poi_count', 0),
                'result_type': result.get('result_type', 'data_found' if result.get('data') is not None else 'no_data'),
                'address': used_address,
                'worker_id': self.worker_id,
                'index': idx,
                'retry_info': retry_info,
                'processing_time': processing_time
            }
        else:
            # 真正的失败（技术失败）
            error_msg = result.get('error_message', '处理异常') if isinstance(result, dict) else '处理异常'
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

def monitor_system_resources():
    """全面监控系统资源使用情况"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        process_memory_mb = memory_info.rss / 1024 / 1024
        
        # 系统内存使用
        system_memory = psutil.virtual_memory()
        memory_used_gb = system_memory.used / 1024 / 1024 / 1024
        memory_percent = system_memory.percent
        
        # CPU使用率
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # 系统负载
        try:
            load_avg = psutil.getloadavg()[0]  # 1分钟平均负载
        except AttributeError:
            # Windows系统没有getloadavg
            load_avg = cpu_percent / 100.0
        
        # Chrome进程数量
        chrome_processes = 0
        try:
            for proc in psutil.process_iter(['name']):
                if 'chrome' in proc.info['name'].lower():
                    chrome_processes += 1
        except:
            chrome_processes = 0
        
        return {
            'process_memory_mb': process_memory_mb,
            'system_memory_gb': memory_used_gb,
            'system_memory_percent': memory_percent,
            'cpu_percent': cpu_percent,
            'load_average': load_avg,
            'chrome_processes': chrome_processes,
            'timestamp': time.time()
        }
    except Exception as e:
        print(f"资源监控失败: {e}")
        return None

def monitor_memory_usage():
    """兼容性函数，保持向后兼容"""
    resources = monitor_system_resources()
    if resources:
        return {
            'process_memory_mb': resources['process_memory_mb'],
            'system_memory_gb': resources['system_memory_gb'],
            'system_memory_percent': resources['system_memory_percent']
        }
    return None


class DataSaveWorker:
    """专用的数据保存进程工作器 - 隔离CPU密集型任务"""
    
    @staticmethod
    def save_worker_process(save_queue, stop_event, output_file_path, worker_id=0):
        """数据保存进程的主函数"""
        import pandas as pd
        import time
        from pathlib import Path
        
        print(f"🔧 数据保存进程 {worker_id} 启动")
        batch_count = 0
        total_saved = 0
        
        try:
            while not stop_event.is_set():
                try:
                    # 从队列获取数据，设置超时避免无限等待
                    data_batch = save_queue.get(timeout=1.0)
                    
                    if data_batch == "STOP":
                        print(f"📤 数据保存进程 {worker_id} 收到停止信号")
                        break
                    
                    # 处理数据批次
                    if data_batch and len(data_batch) > 0:
                        batch_count += 1
                        start_time = time.time()
                        
                        # CPU密集型操作：DataFrame合并
                        if isinstance(data_batch[0], pd.DataFrame):
                            combined_df = pd.concat(data_batch, ignore_index=True)
                        else:
                            # 如果是dict列表，转换为DataFrame
                            combined_df = pd.DataFrame(data_batch)
                        
                        # CPU密集型操作：去重
                        if not combined_df.empty:
                            # 确保有必要的列进行去重
                            dedup_columns = ['name', 'lat', 'lng']
                            existing_columns = [col for col in dedup_columns if col in combined_df.columns]
                            
                            if existing_columns:
                                before_count = len(combined_df)
                                combined_df = combined_df.drop_duplicates(subset=existing_columns, keep='first')
                                after_count = len(combined_df)
                                removed_count = before_count - after_count
                                
                                if removed_count > 0:
                                    print(f"🔄 批次 {batch_count}: 去重移除 {removed_count} 条重复数据")
                        
                        # I/O密集型操作：保存到文件
                        if not combined_df.empty:
                            output_path = Path(output_file_path)
                            file_exists = output_path.exists()
                            
                            combined_df.to_csv(
                                output_path,
                                mode='a',
                                header=not file_exists,
                                index=False,
                                encoding='utf-8-sig'
                            )
                            
                            total_saved += len(combined_df)
                            processing_time = time.time() - start_time
                            
                            print(f"💾 批次 {batch_count}: 保存 {len(combined_df)} 条数据 "
                                  f"(耗时: {processing_time:.2f}s, 累计: {total_saved})")
                        
                except queue.Empty:
                    # 队列为空，继续等待
                    continue
                except Exception as e:
                    print(f"❌ 数据保存进程 {worker_id} 处理错误: {e}")
                    continue
                    
        except KeyboardInterrupt:
            print(f"⚠️  数据保存进程 {worker_id} 被中断")
        except Exception as e:
            print(f"❌ 数据保存进程 {worker_id} 异常退出: {e}")
        finally:
            print(f"🏁 数据保存进程 {worker_id} 结束，共处理 {batch_count} 批次，保存 {total_saved} 条数据")


class AsyncDataSaveManager:
    """异步数据保存管理器"""
    
    def __init__(self, output_file_path, max_queue_size=100):
        self.output_file_path = output_file_path
        self.save_queue = MPQueue(maxsize=max_queue_size)
        self.stop_event = MPEvent()
        self.save_process = None
        self.is_running = False
        
    def start(self):
        """启动数据保存进程"""
        if self.is_running:
            return
            
        self.stop_event.clear()
        self.save_process = Process(
            target=DataSaveWorker.save_worker_process,
            args=(self.save_queue, self.stop_event, self.output_file_path, 1),
            daemon=False  # 确保进程能正常结束
        )
        self.save_process.start()
        self.is_running = True
        print(f"🚀 异步数据保存进程启动 (PID: {self.save_process.pid})")
    
    def save_batch_async(self, data_batch, timeout=5.0):
        """异步提交数据批次到保存队列"""
        if not self.is_running or not data_batch:
            return False
            
        try:
            # 非阻塞提交到队列
            self.save_queue.put(data_batch, timeout=timeout)
            return True
        except queue.Full:
            print(f"⚠️  数据保存队列已满，跳过当前批次")
            return False
        except Exception as e:
            print(f"❌ 提交数据批次失败: {e}")
            return False
    
    def stop(self, timeout=10.0):
        """停止数据保存进程"""
        if not self.is_running:
            return
            
        print("🛑 正在停止数据保存进程...")
        
        try:
            # 发送停止信号
            self.save_queue.put("STOP", timeout=2.0)
            self.stop_event.set()
            
            # 等待进程结束
            if self.save_process and self.save_process.is_alive():
                self.save_process.join(timeout=timeout)
                
                if self.save_process.is_alive():
                    print("⚠️  强制终止数据保存进程")
                    self.save_process.terminate()
                    self.save_process.join(timeout=2.0)
                    
        except Exception as e:
            print(f"❌ 停止数据保存进程时出错: {e}")
        finally:
            self.is_running = False
            print("✅ 数据保存进程已停止")
    
    def get_queue_size(self):
        """获取当前队列大小"""
        try:
            return self.save_queue.qsize()
        except:
            return 0


class DynamicResourceScheduler:
    """动态资源感知调度器 - 根据系统负载智能调节"""
    
    def __init__(self, check_interval=10):
        self.check_interval = check_interval  # 资源检查间隔(秒)
        self.feeding_event = threading.Event()
        self.feeding_event.set()  # 初始状态：允许分发
        
        # 调度状态
        self.is_paused = False
        self.last_check_time = 0
        self.pause_count = 0
        self.resume_count = 0
        
        # 阈值配置
        self.memory_high_threshold = 85   # 内存高阈值
        self.memory_low_threshold = 60    # 内存低阈值
        self.cpu_high_threshold = 90      # CPU高阈值
        self.cpu_low_threshold = 70       # CPU低阈值
        self.load_high_threshold = 8.0    # 负载高阈值
        
        # 历史状态跟踪
        self.resource_history = []
        self.max_history = 10
        
    def check_and_adjust_schedule(self):
        """检查资源状态并调整调度策略"""
        current_time = time.time()
        
        # 避免频繁检查
        if current_time - self.last_check_time < self.check_interval:
            return self.is_paused
            
        self.last_check_time = current_time
        
        # 获取系统资源状态
        resources = monitor_system_resources()
        if not resources:
            return self.is_paused
            
        # 记录历史状态
        self.resource_history.append(resources)
        if len(self.resource_history) > self.max_history:
            self.resource_history.pop(0)
        
        # 决策逻辑
        should_pause = self._should_pause_scheduling(resources)
        should_resume = self._should_resume_scheduling(resources)
        
        if should_pause and not self.is_paused:
            self._pause_scheduling(resources)
        elif should_resume and self.is_paused:
            self._resume_scheduling(resources)
            
        return self.is_paused
    
    def _should_pause_scheduling(self, resources):
        """判断是否应该暂停调度"""
        memory_overload = resources['system_memory_percent'] > self.memory_high_threshold
        cpu_overload = resources['cpu_percent'] > self.cpu_high_threshold
        load_overload = resources['load_average'] > self.load_high_threshold
        
        # 多重条件判断
        critical_conditions = sum([memory_overload, cpu_overload, load_overload])
        
        # 如果有2个或以上关键指标超标，或内存严重超标
        return critical_conditions >= 2 or resources['system_memory_percent'] > 90
    
    def _should_resume_scheduling(self, resources):
        """判断是否应该恢复调度"""
        if not self.is_paused:
            return False
            
        memory_ok = resources['system_memory_percent'] < self.memory_low_threshold
        cpu_ok = resources['cpu_percent'] < self.cpu_low_threshold
        load_ok = resources['load_average'] < 6.0
        
        # 所有关键指标都正常才恢复
        return memory_ok and cpu_ok and load_ok
    
    def _pause_scheduling(self, resources):
        """暂停任务调度"""
        self.is_paused = True
        self.pause_count += 1
        self.feeding_event.clear()  # 阻止任务分发
        
        print(f"🛑 暂停任务调度 (第{self.pause_count}次)")
        print(f"   内存: {resources['system_memory_percent']:.1f}% "
              f"CPU: {resources['cpu_percent']:.1f}% "
              f"负载: {resources['load_average']:.2f}")
        
    def _resume_scheduling(self, resources):
        """恢复任务调度"""
        self.is_paused = False
        self.resume_count += 1
        self.feeding_event.set()  # 允许任务分发
        
        print(f"🟢 恢复任务调度 (第{self.resume_count}次)")
        print(f"   内存: {resources['system_memory_percent']:.1f}% "
              f"CPU: {resources['cpu_percent']:.1f}% "
              f"负载: {resources['load_average']:.2f}")
    
    def wait_for_scheduling(self, timeout=None):
        """等待调度许可（被暂停时会阻塞）"""
        return self.feeding_event.wait(timeout=timeout)
    
    def force_pause(self):
        """强制暂停调度"""
        self.is_paused = True
        self.feeding_event.clear()
        print("🛑 手动暂停任务调度")
    
    def force_resume(self):
        """强制恢复调度"""
        self.is_paused = False
        self.feeding_event.set()
        print("🟢 手动恢复任务调度")
        
    def get_status(self):
        """获取调度器状态"""
        return {
            'is_paused': self.is_paused,
            'pause_count': self.pause_count,
            'resume_count': self.resume_count,
            'resource_history_length': len(self.resource_history)
        }


class AsyncCrawlWrapper:
    """异步爬虫包装器 - 协程+线程池混合架构"""
    def __init__(self, crawler_instance, driver_pool, max_workers=24):
        self.crawler = crawler_instance
        self.driver_pool = driver_pool
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="ChromeWorker")
        # 限制同时并发爬虫任务为20-25个，避免driver竞争
        self.semaphore = asyncio.Semaphore(min(25, max_workers))  
        self.resource_scheduler = DynamicResourceScheduler(check_interval=8)  # 资源感知调度器
        
    async def crawl_single_address_async(self, address_data):
        """异步单地址爬取 - 支持动态资源调度"""
        async with self.semaphore:
            # 动态资源调度检查
            is_paused = self.resource_scheduler.check_and_adjust_schedule()
            if is_paused:
                # 如果被暂停，等待恢复（异步等待）
                await asyncio.get_event_loop().run_in_executor(
                    None, 
                    self.resource_scheduler.wait_for_scheduling,
                    30.0  # 最长等待30秒
                )
            
            loop = asyncio.get_event_loop()
            try:
                # 在线程池中执行Chrome操作
                result = await loop.run_in_executor(
                    self.executor,
                    self._crawl_with_driver_sync,
                    address_data
                )
                return result
            except Exception as e:
                return {
                    'success': False,
                    'error': f'异步调用失败: {str(e)}',
                    'address': str(address_data.get('address', 'unknown')),
                    'index': address_data.get('index', -1)
                }
    
    def _crawl_with_driver_sync(self, address_data):
        """同步Chrome爬取操作（在线程池中执行）"""
        driver = None
        try:
            # 获取Driver - 带重试机制
            for retry_attempt in range(3):
                driver = self.driver_pool.get_driver()
                if driver is not None:
                    pass  # 静默处理异步重试成功
                    break
                if retry_attempt < 2:  # 不是最后一次重试
                    pass  # 静默处理异步重试
                    time.sleep(0.5)  # 等待驱动释放
            
            if driver is None:
                return {
                    'success': False,
                    'error': 'Chrome驱动池已满，重试3次后仍无法获取驱动',
                    'address': str(address_data.get('address', 'unknown')),
                    'index': address_data.get('index', -1)
                }
            
            # 执行爬取 - 支持多地址重试
            primary_address = address_data.get('address') or address_data.get('primary')
            secondary_address = address_data.get('secondary')
            fallback_address = address_data.get('fallback')
            idx = address_data['index']
            
            used_address = primary_address
            retry_info = []
            
            # 首先使用primary地址
            result = self.crawler._crawl_poi_info_turbo(primary_address, driver)
            
            # 检查是否需要重试 - 与Worker逻辑保持一致
            should_retry = (isinstance(result, dict) and 
                           not result.get('is_building', False) and
                           result.get('poi_count', 0) == 0 and
                           result.get('status') != 'hotel_category_page_skipped')
            
            # 重试secondary地址
            if should_retry and secondary_address:
                retry_result = self.crawler._crawl_poi_info_turbo(secondary_address, driver, check_building_type=False)
                retry_info.append(f"重试secondary: {secondary_address}")
                
                if (isinstance(retry_result, dict) and 
                    retry_result.get('status') == 'success'):
                    result = retry_result
                    used_address = secondary_address
                    should_retry = False
            
            # 重试fallback地址
            if should_retry and fallback_address:
                retry_result = self.crawler._crawl_poi_info_turbo(fallback_address, driver, check_building_type=False)
                retry_info.append(f"重试fallback: {fallback_address}")
                
                if (isinstance(retry_result, dict) and 
                    retry_result.get('status') == 'success'):
                    result = retry_result
                    used_address = fallback_address
            
            # 检查生命周期
            needs_restart = self.driver_pool.increment_driver_usage(driver)
            if needs_restart:
                # 重启driver
                new_driver = self.driver_pool.restart_driver(driver)
                if new_driver:
                    driver = new_driver
                else:
                    driver = None
            
            # 构建结果 - 使用与Worker相同的逻辑
            if (isinstance(result, dict) and 
                result.get('status') == 'success'):
                return {
                    'success': True,
                    'data': result.get('data'),
                    'poi_count': result.get('poi_count', 0),
                    'result_type': result.get('result_type', 'data_found' if result.get('data') is not None else 'no_data'),
                    'address': str(used_address),
                    'index': idx,
                    'retry_info': retry_info
                }
            else:
                # 真正的失败
                error_msg = result.get('error_message', '处理异常') if isinstance(result, dict) else '处理异常'
                return {
                    'success': False,
                    'error': error_msg,
                    'address': str(used_address),
                    'index': idx,
                    'retry_info': retry_info
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'address': str(address_data.get('address', 'unknown')),
                'index': address_data.get('index', -1)
            }
        finally:
            # 强制释放Driver - 确保彻底清理
            if driver:
                self.driver_pool.release_driver(driver)
    
    async def crawl_batch_async(self, address_list):
        """异步批量爬取"""
        tasks = []
        for address_data in address_list:
            task = self.crawl_single_address_async(address_data)
            tasks.append(task)
        
        # 并发执行所有任务
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    def cleanup(self):
        """清理资源"""
        self.executor.shutdown(wait=True)


class ParallelPOICrawler:
    def __init__(self, max_workers=None, output_dir="data/output", batch_size=50, enable_resume=True):
        # 科学的高性能配置
        cpu_count = mp.cpu_count()
        if max_workers is None:
            if cpu_count >= 12:
                # 12核心以上：IO密集型优化配置
                self.max_workers = min(36, int(cpu_count * 3.0))  # 核心数 × 2.5~3
            elif cpu_count >= 8:
                # 8-11核心：使用CPU数量*3
                self.max_workers = min(32, cpu_count * 3)
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
        
        # 高效Chrome驱动池配置 - driver数量控制在线程数以下，避免内存过载
        chrome_pool_size = min(30, max(24, int(self.max_workers * 0.75)))  # driver数 < 线程数
        
        # 初始化组件
        self.driver_pool = ChromeDriverPool(max_drivers=chrome_pool_size)
        self.task_scheduler = TurboTaskScheduler(max_threads=self.max_workers)
        
        # 异步锁用于线程安全的进度保存
        self.progress_lock = None  # 将在异步环境中初始化为 asyncio.Lock()
        
    def _crawl_poi_info_turbo(self, address, driver, check_building_type=True):
        """Turbo模式的POI信息爬取"""
        url = f'https://www.google.com/maps/place/{address}'
        
        try:
            driver.get(url)
            
            # 快速检查酒店类别页面
            hotel_check_result = self._has_category_header(driver)
            if hotel_check_result:
                print(f"🏨 酒店页面: {address[:50]}...")
                return {
                    'data': None,
                    'is_building': False,
                    'poi_count': 0,
                    'status': 'success',  # 技术成功，但是酒店类别页面
                    'result_type': 'hotel_category_page',
                    'hotel_type': hotel_check_result.get('type', 'unknown'),
                    'address': address
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
                            'status': 'success',  # 技术成功，但是数据列不完整
                            'result_type': 'column_error',
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
                        'status': 'error',  # 真正的数据处理失败
                        'error_message': error_msg,
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
                                'status': 'success',  # 技术成功，但是无POI数据
                                'result_type': 'no_poi_data',
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
                    'status': 'success',  # 技术成功，但是无POI数据
                    'result_type': 'no_poi_data'
                }
                
        except Exception as e:
            import traceback
            error_detail = f"{type(e).__name__}: {str(e)}"
            pass  # 静默处理Chrome任务异常
            return {
                'data': None,
                'is_building': False,
                'poi_count': 0,
                'status': 'error',  # 真正的失败
                'error_message': error_detail,
                'error_type': type(e).__name__
            }
    
    def _has_category_header(self, driver):
        """检查是否是酒店类别页面，返回检测结果详情"""
        try:
            # 检查酒店类别标题
            category_headers = driver.find_elements("css selector", "h2.kPvgOb.fontHeadlineSmall")
            for header in category_headers:
                text = header.text.strip()
                # 支持中文、日文和英文酒店检测
                if text == "酒店":
                    return {"type": "中文酒店类别页面", "keyword": text}
                elif text == "ホテル":
                    return {"type": "日文酒店类别页面", "keyword": text}
                elif "hotel" in text.lower():
                    return {"type": "英文酒店类别页面", "keyword": text}
            
            # 检查其他类别页面指示器
            category_indicators = [
                ("div.aIiAFe h1", "主标题"),
                ("h1.jRccSf", "类别标题"),
                ("h1.ZoUhNb", "商家类别标题")
            ]
            
            for selector, desc in category_indicators:
                try:
                    elements = driver.find_elements("css selector", selector)
                    for element in elements:
                        text = element.text.strip()
                        text_lower = text.lower()
                        
                        if "酒店" in text:
                            return {"type": f"中文酒店页面({desc})", "keyword": text}
                        elif "ホテル" in text:
                            return {"type": f"日文酒店页面({desc})", "keyword": text}
                        elif any(keyword in text_lower for keyword in ["hotel", "lodging", "accommodation"]):
                            return {"type": f"英文酒店页面({desc})", "keyword": text}
                except:
                    continue
                    
            return None
        except Exception as e:
            return None
    
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
        """保存进度 - 同步版本"""
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
    
    async def _save_progress_async(self, district_name, completed_count, total_count, total_success, total_errors):
        """异步保存进度 - 线程安全版本"""
        if not self.enable_resume:
            return
            
        # 初始化异步锁（如果尚未初始化）
        if self.progress_lock is None:
            self.progress_lock = asyncio.Lock()
            
        progress_data = {
            'district_name': district_name,
            'completed_count': completed_count,
            'total_count': total_count,
            'total_success': total_success,
            'total_errors': total_errors,
            'output_file': str(self.output_file),
            'timestamp': time.time()
        }
        
        # 使用异步锁确保线程安全
        async with self.progress_lock:
            # 原子性写入：先写临时文件，再重命名
            temp_file = f"{self.progress_file}.tmp"
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(progress_data, f, ensure_ascii=False, indent=2)
                
                # 原子性重命名
                import os
                os.rename(temp_file, self.progress_file)
            except Exception as e:
                # 清理临时文件
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except:
                    pass
                raise e
    
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
        
        # 检查不同类型的结果
        # 技术成功但无POI数据
        no_poi_count = sum(1 for r in batch_results 
                          if r.get('success', False) and 
                             r.get('result_type') == 'no_poi_data')
        
        # 技术成功且有POI数据
        has_poi_count = sum(1 for r in batch_results 
                           if r.get('success', False) and 
                              r.get('data') is not None)
        
        # 酒店类别页面
        hotel_category_count = sum(1 for r in batch_results 
                                  if r.get('success', False) and 
                                     r.get('result_type') == 'hotel_category_page_skipped')
        
        # 检查特定格式地址的非建筑物情况 (如: 3-chōme-5-23+地名)
        import re
        chome_format_pattern = r'\d+-ch[ōo]me-\d+-\d+\+\w+'  # 匹配 "数字-chōme-数字-数字+地名" 格式
        
        chome_format_no_poi = sum(1 for r in batch_results 
                                if r.get('success', False) and
                                r.get('result_type') == 'no_poi_data' and
                                re.search(chome_format_pattern, str(r.get('address', '')), re.IGNORECASE))
        
        # 触发警告的条件：整个批次技术失败率100% 或 chōme格式地址80%以上无POI
        fail_count = total_count - success_count
        should_warn = (fail_count == total_count) or \
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
    
    async def crawl_from_csv_async(self, input_file):
        """异步协程模式爬取CSV文件"""
        try:
            df = pd.read_csv(input_file)
        except Exception as e:
            print(f"读取CSV文件失败: {e}")
            return 0, 1
        
        # 准备地址数据 - 优先使用FormattedAddress
        addresses = []
        for index, row in df.iterrows():
            address_obj = {
                'primary': None,
                'secondary': None,
                'fallback': None,
                'index': index
            }
            
            # 优先使用FormattedAddress
            if 'FormattedAddress' in df.columns and pd.notna(row['FormattedAddress']) and row['FormattedAddress'].strip():
                address_obj['primary'] = row['FormattedAddress'].strip()
            
            # 其次使用Address
            if 'Address' in df.columns and pd.notna(row['Address']):
                address_obj['secondary'] = row['Address']
            
            # 最后使用ConvertedAddress
            if 'ConvertedAddress' in df.columns and pd.notna(row['ConvertedAddress']) and row['ConvertedAddress'].strip():
                address_obj['fallback'] = row['ConvertedAddress'].strip()
            
            # 地址预处理：如果primary为空，依次使用secondary和fallback
            if not address_obj['primary'] and address_obj['secondary']:
                address_obj['primary'] = address_obj['secondary']
                address_obj['secondary'] = address_obj['fallback']
                address_obj['fallback'] = None
            elif not address_obj['primary'] and address_obj['fallback']:
                address_obj['primary'] = address_obj['fallback']
                address_obj['fallback'] = None
            
            # 设置主要address键供异步使用
            address_obj['address'] = address_obj['primary']
            
            addresses.append(address_obj)
        
        district_name = self._extract_district_name(input_file)
        total_addresses = len(addresses)
        
        # 设置进度文件
        self.progress_file = self.progress_dir / f"{district_name}_progress.json"
        
        # 断点续传逻辑
        start_idx = 0
        total_success = 0
        total_errors = 0
        
        progress_data = self._load_progress(district_name)
        if progress_data:
            print(f"发现断点续传文件: {self.progress_file}")
            start_idx = progress_data.get('completed_count', 0)
            total_success = progress_data.get('total_success', 0)
            total_errors = progress_data.get('total_errors', 0)
            
            # 恢复输出文件
            if progress_data.get('output_file'):
                self.output_file = Path(progress_data['output_file'])
                print(f"恢复输出文件: {self.output_file}")
            else:
                self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
                
            print(f"从第 {start_idx} 个地址继续处理 (已完成: {start_idx}, 成功: {total_success}, 失败: {total_errors})")
        else:
            print(f"开始新的{district_name}异步爬取任务")
            self.output_file = self.output_dir / f"{district_name}_poi_data_{int(time.time())}.csv"
            
        # 过滤掉已处理的地址
        addresses = addresses[start_idx:]
        
        # 异步模式使用优化的批次大小
        async_batch_size = 125  # 每批处理125个地址，减少磁盘IO频率
        
        print(f"🚀 异步协程+多进程模式启动")
        print(f"数据集: {district_name} (总计: {total_addresses} 个地址, 待处理: {len(addresses)})")
        print(f"📊 配置参数:")
        print(f"   线程池: {self.max_workers} 个线程 (推荐: {mp.cpu_count()}核 × 3)")
        print(f"   Chrome池: {self.driver_pool.max_drivers} 个实例 (内存预估: {self.driver_pool.max_drivers * 350}MB)")
        print(f"   并发限制: {min(25, self.max_workers)} 个同时爬虫任务")
        print(f"   批次大小: {async_batch_size} 个地址/批次 (异步优化)")
        print(f"   输出文件: {self.output_file}")
        if start_idx > 0:
            print(f"   断点续传: 从第 {start_idx} 个地址继续")
        
        # 配置合理性检查
        if self.max_workers > self.driver_pool.max_drivers:
            print(f"⚠️  警告: 线程数({self.max_workers}) > Driver数({self.driver_pool.max_drivers})，可能导致竞争等待")
        
        memory_estimate = self.driver_pool.max_drivers * 350 / 1024  # GB
        if memory_estimate > 16:
            print(f"⚠️  警告: Chrome内存预估 {memory_estimate:.1f}GB，可能超出系统容量")
        
        # 显示初始内存状态
        initial_memory = monitor_memory_usage()
        if initial_memory:
            print(f"初始内存: {initial_memory['system_memory_percent']:.1f}%")
        
        # 创建输出文件头部（只有在新任务时）
        if start_idx == 0:
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
        
        # 启动异步数据保存管理器
        save_manager = AsyncDataSaveManager(str(self.output_file), max_queue_size=50)
        save_manager.start()
        
        # 创建异步包装器
        async_wrapper = AsyncCrawlWrapper(
            crawler_instance=self,
            driver_pool=self.driver_pool,
            max_workers=self.max_workers
        )
        
        try:
            # 批量异步处理
            success_count = 0
            error_count = 0
            batch_data = []
            processed_count = 0
            last_progress_save = time.time()  # 进度保存时间戳
            
            # 累积历史统计
            accumulated_success = total_success
            accumulated_errors = total_errors
            
            for i in range(0, len(addresses), async_batch_size):
                batch_addresses = addresses[i:i + async_batch_size]
                print(f"\n处理批次 {i//async_batch_size + 1}/{(len(addresses)-1)//async_batch_size + 1}")
                
                # 异步批量处理
                batch_results = await async_wrapper.crawl_batch_async(batch_addresses)
                
                # 处理结果
                for result in batch_results:
                    if isinstance(result, Exception):
                        error_count += 1
                        print(f"任务异常: {result}")
                        continue
                    
                    if result.get('success'):
                        success_count += 1
                        # 只有真正有数据的才添加到batch_data
                        if result.get('data') is not None:
                            batch_data.append(result['data'])
                    else:
                        error_count += 1
                
                # 更新已处理数量
                processed_count += len(batch_addresses)
                
                # 定期保存进度 - 每2分钟或每500个地址
                current_time = time.time()
                if (current_time - last_progress_save > 120) or (processed_count % 500 == 0):
                    await self._save_progress_async(district_name, start_idx + processed_count, total_addresses, 
                                                   accumulated_success + success_count, accumulated_errors + error_count)
                    last_progress_save = current_time
                
                # 多进程异步保存数据
                if batch_data:
                    # 提交到多进程保存队列，不阻塞主线程
                    save_success = save_manager.save_batch_async(batch_data.copy())
                    if save_success:
                        print(f"📤 提交 {len(batch_data)} 条数据到保存队列")
                        batch_data = []  # 成功提交后清空缓存
                    else:
                        print(f"⚠️  数据保存队列繁忙，保持数据在内存中")
                        # 如果队列满了，暂时保持数据不清空
                        if save_manager.get_queue_size() < 10:  # 队列不太满时再清空
                            batch_data = []
                
                # 显示进度和资源状态
                progress = (i + len(batch_addresses)) / len(addresses) * 100
                resources = monitor_system_resources()
                queue_size = save_manager.get_queue_size()
                
                # 获取调度器状态
                scheduler_status = async_wrapper.resource_scheduler.get_status()
                
                if resources:
                    resource_str = (f" - 内存: {resources['system_memory_percent']:.1f}% "
                                  f"CPU: {resources['cpu_percent']:.1f}% "
                                  f"负载: {resources['load_average']:.1f}")
                    queue_str = f" - 保存队列: {queue_size}"
                    scheduler_str = ""
                    
                    if scheduler_status['is_paused']:
                        scheduler_str = f" - 🛑调度已暂停"
                    elif scheduler_status['pause_count'] > 0:
                        scheduler_str = f" - 暂停/恢复: {scheduler_status['pause_count']}/{scheduler_status['resume_count']}"
                        
                    # 添加成功类型统计
                    technical_success = success_count + error_count  # 总的已处理数
                    if technical_success > 0:
                        success_rate = (success_count / technical_success) * 100
                        print(f"进度: {progress:.1f}% - 成功: {success_count}, 失败: {error_count} (成功率: {success_rate:.1f}%){resource_str}{queue_str}{scheduler_str}")
                    else:
                        print(f"进度: {progress:.1f}% - 成功: {success_count}, 失败: {error_count}{resource_str}{queue_str}{scheduler_str}")
                else:
                    print(f"进度: {progress:.1f}% - 成功: {success_count}, 失败: {error_count}")
                
                # 资源过载检查和反馈
                if resources:
                    if resources['system_memory_percent'] > 85:
                        print(f"⚠️  内存使用率过高: {resources['system_memory_percent']:.1f}%")
                        gc.collect()  # 强制垃圾回收
                    
                    if resources['cpu_percent'] > 85:
                        print(f"⚠️  CPU使用率过高: {resources['cpu_percent']:.1f}%")
                    
                    if resources['load_average'] > 8.0:
                        print(f"⚠️  系统负载过高: {resources['load_average']:.2f}")
                    
                    # 检查Chrome进程数量
                    if resources['chrome_processes'] > 50:
                        print(f"⚠️  Chrome进程过多: {resources['chrome_processes']}")
                        
                    # 极端情况：强制暂停
                    if resources['system_memory_percent'] > 95:
                        print(f"🚨 内存危险！强制暂停5秒")
                        async_wrapper.resource_scheduler.force_pause()
                        await asyncio.sleep(5)
                        async_wrapper.resource_scheduler.force_resume()
            
            # 提交剩余数据
            if batch_data:
                print(f"📤 提交最后 {len(batch_data)} 条数据到保存队列")
                save_manager.save_batch_async(batch_data)
            
            print(f"\n✅ 异步处理完成")
            print(f"成功: {success_count}, 失败: {error_count}")
            print(f"等待数据保存进程完成...")
            
            # 最终保存进度
            await self._save_progress_async(district_name, start_idx + processed_count, total_addresses, 
                                           accumulated_success + success_count, accumulated_errors + error_count)
            
            # 等待保存队列清空
            timeout_count = 0
            while save_manager.get_queue_size() > 0 and timeout_count < 30:
                print(f"⏳ 等待保存队列清空，剩余: {save_manager.get_queue_size()}")
                time.sleep(2)
                timeout_count += 1
            
            return success_count, error_count
            
        finally:
            # 清理资源
            print("🧹 清理资源中...")
            
            # 输出调度器统计信息
            scheduler_status = async_wrapper.resource_scheduler.get_status()
            if scheduler_status['pause_count'] > 0 or scheduler_status['resume_count'] > 0:
                print(f"📊 资源调度统计: 暂停 {scheduler_status['pause_count']} 次, "
                      f"恢复 {scheduler_status['resume_count']} 次")
            else:
                print("📊 资源调度: 运行期间未触发暂停/恢复")
            
            async_wrapper.cleanup()
            save_manager.stop()  # 停止数据保存进程
    
    def crawl_from_csv_turbo(self, input_file):
        """[已弃用] 旧的同步模式，自动重定向到异步模式"""
        print("⚠️  crawl_from_csv_turbo已弃用，自动使用异步模式...")
        import asyncio
        return asyncio.run(self.crawl_from_csv_async(input_file))
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

    # 已移除旧的同步方法 crawl_all_districts，请使用 crawl_all_districts_async

    async def crawl_all_districts_async(self, input_dir="data/input"):
        """异步批量处理所有区文件"""
        input_path = Path(input_dir)
        csv_files = list(input_path.glob("*.csv"))
        
        if not csv_files:
            print(f"在 {input_dir} 目录中没有找到CSV文件")
            return
        
        print(f"发现 {len(csv_files)} 个区文件，开始异步批量处理...\n")
        
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
                success, errors = await self.crawl_from_csv_async(csv_file)
                all_success += success
                all_errors += errors
                processed_districts.append(f"{district_name}: 成功{success}, 失败{errors}")
                
            except Exception as e:
                print(f"处理 {district_name} 时发生错误: {e}")
                all_errors += 1
                processed_districts.append(f"{district_name}: 处理失败")
                continue
            
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

    # 已移除旧的同步方法 crawl_multiple_files，请使用 crawl_multiple_files_async

    async def crawl_multiple_files_async(self, file_paths):
        """异步处理多个指定文件"""
        print(f"准备异步处理 {len(file_paths)} 个文件...\n")
        
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
                success, errors = await self.crawl_from_csv_async(file_path)
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
    # 移除 --async 参数，异步模式现在是默认且唯一的模式
    
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
        # 移除 --async 选项说明，异步模式现在是默认的
        print("")
        print("📊 最佳实践配置（12核32GB系统）：")
        print("  - 线程数: 36个 (核心数 × 3，IO密集型优化)")
        print("  - Chrome实例: 27个 (线程数 × 0.75，避免内存过载)")  
        print("  - 同时并发: 25个爬虫任务 (避免driver竞争)")
        print("  - 内存限制: 每个Chrome 350MB (总计约10.5GB)")
        print("  - 批次大小: 125个地址 (减少磁盘IO频率)")
        print("")
        print("🎛️ 智能特性：")
        print("  - 动态资源感知调度 (内存/CPU过载自动暂停)")
        print("  - 多进程数据保存 (CPU密集型任务隔离)")
        print("  - Chrome生命周期管理 (100任务后自动重启)")
        print("  - 异步协程+线程池 (默认高效IO处理)")
        print("  - Driver预分配机制 (避免竞争等待)")
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
        print("🚀 使用异步协程模式")
        asyncio.run(crawler.crawl_all_districts_async())
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
    
    # 异步模式是默认且唯一的模式
    print("🚀 使用异步协程模式")
    
    if len(valid_files) == 1:
        # 单文件异步处理
        asyncio.run(crawler.crawl_from_csv_async(valid_files[0]))
    else:
        # 多文件异步批量处理
        asyncio.run(crawler.crawl_multiple_files_async(valid_files))


if __name__ == "__main__":
    main()