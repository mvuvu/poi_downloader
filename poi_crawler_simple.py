#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
import queue
import json
import pandas as pd
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
import os
import gc
import argparse
import glob
import signal
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# 导入现有的POI提取函数
from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url, has_hotel_category
from driver_action import click_on_more_button, scroll_poi_section


class ChromeWorker(threading.Thread):
    """持久化Chrome工作线程"""
    
    def __init__(self, worker_id, task_queue, result_queue, stop_event, verbose=False, retry_queue=None):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stop_event = stop_event
        self.verbose = verbose
        self.retry_queue = retry_queue  # 重试队列
        self.driver = None
        self.processed_count = 0
        self.success_count = 0
        self.error_count = 0
        
    def create_driver(self):
        """创建优化的Chrome驱动 - 基于turbo版本验证配置"""
        try:
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
            
            # 禁用语音识别和AI功能，避免TensorFlow加载
            options.add_argument('--disable-speech-api')
            options.add_argument('--disable-features=AudioServiceOutOfProcess,TranslateUI')
            options.add_argument('--disable-background-media-suspend')
            options.add_experimental_option('prefs', {
                'profile.default_content_setting_values.media_stream_mic': 2,
                'profile.default_content_setting_values.media_stream_camera': 2,
                'profile.default_content_setting_values.geolocation': 2,
                'profile.default_content_setting_values.notifications': 2
            })
        
            # 完全静默Service
            service = Service(
                ChromeDriverManager().install(),
                log_path='NUL',
                service_args=['--silent']
            )
            
            driver = webdriver.Chrome(service=service, options=options)
            
            # 测试空页面加载
            driver.get('about:blank')
            
            if self.verbose:
                print(f"✅ Worker {self.worker_id}: Chrome驱动创建成功")
            
            return driver
            
        except Exception as e:
            print(f"💥 Worker {self.worker_id}: Chrome驱动创建失败: {e}")
            raise
    
    def run(self):
        """工作线程主循环"""
        print(f"🚀 Worker {self.worker_id}: 启动")
        
        # 创建持久化driver
        try:
            self.driver = self.create_driver()
        except Exception as e:
            print(f"💥 Worker {self.worker_id}: 无法创建driver，退出: {e}")
            return
        
        try:
            while not self.stop_event.is_set():
                try:
                    task = None
                    task_source = None
                    
                    # 首先检查重试队列（优先级更高）
                    try:
                        task = self.retry_queue.get_nowait()
                        task_source = 'retry'
                    except queue.Empty:
                        # 重试队列为空，从主任务队列获取
                        task = self.task_queue.get(timeout=1.0)
                        task_source = 'main'
                    
                    # 处理任务
                    result = self.process_task(task)
                    
                    # 提交结果
                    self.result_queue.put(result)
                    
                    # 标记任务完成
                    if task_source == 'retry':
                        self.retry_queue.task_done()
                    else:
                        self.task_queue.task_done()
                    
                    # 更新统计
                    self.processed_count += 1
                    if result['success']:
                        self.success_count += 1
                    else:
                        self.error_count += 1
                    
                    # 🔧 日志压缩 - 只在每100条或verbose模式时打印
                    if self.verbose or self.processed_count % 100 == 0:
                        print(f"📊 Worker {self.worker_id}: 已处理 {self.processed_count} 个任务 "
                              f"(成功: {self.success_count}, 失败: {self.error_count})")
                    
                    # 定期清理浏览器缓存
                    if self.processed_count % 100 == 0:
                        try:
                            self.driver.delete_all_cookies()
                            self.driver.execute_script("window.gc();")
                        except:
                            pass
                    
                    # 每1000个任务重启worker
                    if self.processed_count % 1000 == 0 and self.processed_count > 0:
                        print(f"🔄 Worker {self.worker_id}: 达到1000个任务，重启Chrome驱动...")
                        try:
                            # 关闭当前driver
                            if self.driver:
                                self.driver.quit()
                            # 创建新的driver
                            self.driver = self.create_driver()
                            print(f"✅ Worker {self.worker_id}: Chrome驱动重启成功")
                        except Exception as e:
                            print(f"❌ Worker {self.worker_id}: Chrome驱动重启失败: {e}")
                            # 如果重启失败，尝试继续使用现有driver或退出
                            if not self.driver:
                                print(f"💥 Worker {self.worker_id}: 无法继续，退出工作线程")
                                break
                    
                except queue.Empty:
                    # 队列为空，继续等待
                    continue
                except Exception as e:
                    if self.verbose:
                        print(f"❌ Worker {self.worker_id}: 处理任务异常: {e}")
                    continue
                    
        finally:
            # 清理资源
            if self.driver:
                try:
                    self.driver.quit()
                    if self.verbose:
                        print(f"🧹 Worker {self.worker_id}: 驱动已清理")
                except:
                    pass
            
            print(f"🏁 Worker {self.worker_id}: 完成，共处理 {self.processed_count} 个任务 "
                  f"(成功: {self.success_count}, 失败: {self.error_count})")
    
    def process_task(self, task):
        """处理单个POI提取任务"""
        address = task['address']
        index = task['index']
        original_address = task.get('original_address')
        is_retry = task.get('is_retry', False)
        
        try:
            # 调用现有的POI提取逻辑，传递重试标识
            result = self.crawl_poi_info(address, is_retry=is_retry)
            
            if result.get('status') == 'success':
                return {
                    'success': True,
                    'data': result.get('data'),
                    'address': address,
                    'original_address': original_address,
                    'index': index,
                    'worker_id': self.worker_id,
                    'poi_count': result.get('poi_count', 0),
                    'result_type': result.get('result_type', 'unknown'),
                    'is_building': result.get('is_building', False),
                    'is_retry': is_retry
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error_message', 'POI提取失败'),
                    'address': address,
                    'original_address': original_address,
                    'index': index,
                    'worker_id': self.worker_id,
                    'poi_count': result.get('poi_count', 0),
                    'result_type': result.get('result_type', 'unknown'),
                    'is_building': result.get('is_building', False),
                    'is_retry': is_retry
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'address': address,
                'original_address': original_address,
                'index': index,
                'worker_id': self.worker_id,
                'poi_count': 0,
                'result_type': 'exception_error',
                'is_building': False,
                'is_retry': is_retry
            }
    
    def crawl_poi_info(self, address, is_retry=False):
        """POI信息爬取 - 基于现有代码简化版，支持快速重试模式"""
        url = f'https://www.google.com/maps/place/{address}'
        
        # 添加地址处理开始日志
        if self.verbose:
            print(f"🔍 处理地址: {address[:50]}{'...' if len(address) > 50 else ''}")
        
        try:
            self.driver.get(url)
            
            # 等待页面基本加载
            time.sleep(1)  # 给页面一点时间开始跳转
            
            # 早期检测：判断是否是有效的建筑物页面
            if not self.is_valid_building_page():
                print(f"⚠️  {address[:30]}{'...' if len(address) > 30 else ''}  | 状态: 无效地址页面")
                return {
                    'data': None,
                    'status': 'success',  # 标记为成功以触发重试
                    'result_type': 'invalid_address',  # 新的结果类型
                    'poi_count': 0,
                    'is_building': False
                }
            
            # 快速检查酒店类别页面
            if has_hotel_category(self.driver,address):
                if self.verbose:
                    print(f"🏨 检测到酒店页面，跳过处理: {address[:50]}")
                return {
                    'data': None,
                    'status': 'success',
                    'result_type': 'hotel_category_page',
                    'poi_count': 0,
                    'is_building': False
                }
            
            poi_count = 0
            #place_type = 'unknown'
            #is_building = False

            try:
                place_name = get_building_name(self.driver)
            except Exception as e:
                # 尝试备用方案获取地点名称
                place_name = self._get_fallback_location_name(self.driver, address) or 'Unknown Location'
                    
         
            try:
                more_button = self.driver.find_elements('class name', 'M77dve')
                if more_button:
                    click_on_more_button(self.driver)
                    scroll_poi_section(self.driver)
            except:
                pass
        


            df = get_all_poi_info(self.driver)

            if df is not None and not df.empty:
            
                poi_count = len(df)
                # 获取坐标
                
                final_url = wait_for_coords_url(self.driver)
                if final_url:
                    lat, lng = get_coords(final_url)
                else:
                    lat, lng = None, None

                
                df['blt_name'] = place_name
                df['lat'] = lat
                df['lng'] = lng
                
                # 单地址完成总结 - 始终显示成功处理的地址
                print(f"✅ {address[:30]}{'...' if len(address) > 30 else ''}  | POI: {poi_count} | 状态: 已保存")


                return {
                            'data': df,
                            'status': 'success',
                            'result_type': 'building_with_poi',
                            'poi_count': poi_count,
                            'is_building': True
                        }
 
            else:
                
                place_type = get_building_type(self.driver)
                is_building = place_type == '建筑物' or place_type == '建造物'
                if is_building:
                    print(f"🏢 {address[:30]}{'...' if len(address) > 30 else ''}  | 类型: {place_type} | POI: 0 | 非商业建筑")
                    
                    return {
                        'data': None,
                        'status': 'success',
                        'result_type': 'building_no_poi',
                        'poi_count': 0,
                        'is_building': True
                    }
                else:
                    print(f"❌ {address[:30]}{'...' if len(address) > 30 else ''}  | 状态: 非建筑物")
                    return {
                        'data': None,
                        'status': 'success',  # 改为success，这样才能触发重试
                        'result_type': 'not_building',
                        'poi_count': 0,
                        'is_building': False
                    }
                
        except TimeoutException:
            print(f"⏰ {address[:30]}{'...' if len(address) > 30 else ''}  | 错误: 页面加载超时")
            return {
                'data': None,
                'status': 'error',
                'error_message': '页面加载超时',
                'result_type': 'timeout_error',
                'poi_count': 0,
                'is_building': False
            }
        except Exception as e:
            print(f"💥 {address[:30]}{'...' if len(address) > 30 else ''}  | 错误: {str(e)[:50]}")
            return {
                'data': None,
                'status': 'error',
                'error_message': str(e),
                'result_type': 'processing_error',
                'poi_count': 0,
                'is_building': False
            }

    
    
    def is_valid_building_page(self):
        """仅用H1判断页面是否是有效的建筑物页面"""
        try:
            # 等待页面基本加载
            WebDriverWait(self.driver, 3).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # 尝试获取H1
            h1_elements = self.driver.find_elements(By.TAG_NAME, "h1")
            if h1_elements and h1_elements[0].text.strip():
                # 有有效的H1标题，是建筑物页面
                return True
            
            # 没有H1或H1为空，是无效地址页面
            return False
            
        except:
            # 出错时保守处理，当作无效页面
            return False
    
    def _get_fallback_location_name(self, driver, address):
        """获取备用位置名称"""
        try:
            # 尝试多种选择器获取位置名称
            selectors = [
                "h1.DUwDvf",
                "h1.x3AX1-LfntMc-header-title-title", 
                "h1.bwoZTb",
                "h2.qrShPb",
                "span.DUwDvf"
            ]
            
            for selector in selectors:
                try:
                    element = driver.find_element("css selector", selector)
                    if element and element.text.strip():
                        return element.text.strip()
                except:
                    continue
            
            # 如果都失败，返回地址的简化版本
            return address.split(',')[0] if ',' in address else address
            
        except:
            return "Unknown Location"


class ResultBuffer:
    """结果缓存池 - 定期落盘"""
    
    def __init__(self, output_file, batch_size=50, flush_interval=30, verbose=False, crawler_instance=None):
        self.output_file = Path(output_file)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.verbose = verbose
        self.crawler_instance = crawler_instance
        self.buffer = []
        self.lock = threading.Lock()
        self.last_flush_time = time.time()
        self.total_saved = 0
        
        # 创建输出文件头部
        self.create_header()
        
        # 启动定期刷新线程
        self.flush_thread = threading.Thread(target=self.auto_flush, daemon=True)
        self.flush_thread.start()
    
    def create_header(self):
        """创建CSV文件头部 - 支持断点续传"""
        if not self.output_file.exists():
            # 文件不存在，创建新文件
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            if self.verbose:
                print(f"📝 创建输出文件: {self.output_file}")
        else:
            # 文件存在，检查断点续传情况
            try:
                # 读取现有文件检查数据状态
                existing_df = pd.read_csv(self.output_file, encoding='utf-8-sig')
                if existing_df.empty:
                    # 文件存在但为空，重新创建头部
                    header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
                    header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
                    if self.verbose:
                        print(f"📝 重新创建输出文件头部: {self.output_file}")
                else:
                    if self.verbose:
                        print(f"📝 继续使用现有输出文件: {self.output_file} (已有{len(existing_df)}条数据)")
            except Exception as e:
                if self.verbose:
                    print(f"⚠️ 读取现有文件失败，重新创建: {e}")
                # 出错时重新创建文件
                header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
                header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
    
    def add_result(self, result):
        """添加结果到缓存池 - 🔧 POI为空时快速跳过"""
        # 快速跳过失败或无数据的结果
        if not result['success']:
            return
            
        # 🔧 POI信息为空时快速跳过，避免无意义写入
        poi_count = result.get('poi_count', 0)
        if poi_count == 0:
            return
            
        data = result.get('data')
        if data is None:
            return
        
        if isinstance(data, pd.DataFrame) and not data.empty:
            with self.lock:
                self.buffer.append(data)
                
                # 检查是否需要立即刷新
                if len(self.buffer) >= self.batch_size:
                    self._flush_to_disk()
    
    def auto_flush(self):
        """定期自动刷新到磁盘"""
        while True:
            time.sleep(self.flush_interval)
            current_time = time.time()
            
            with self.lock:
                if (self.buffer and 
                    current_time - self.last_flush_time >= self.flush_interval):
                    self._flush_to_disk()
    
    def _flush_to_disk(self):
        """刷新缓存到磁盘（内部方法，需要持有锁）"""
        if not self.buffer:
            return
        
        # 检查中断标志
        if self.crawler_instance and self.crawler_instance.interrupt_flag.is_set():
            if self.verbose:
                print("⚠️  检测到中断信号，跳过数据写入")
            return
        
        try:
            # 合并所有DataFrame
            combined_df = pd.concat(self.buffer, ignore_index=True)
            
            # 追加到文件
            combined_df.to_csv(self.output_file, mode='a', header=False, 
                             index=False, encoding='utf-8-sig')
            
            self.total_saved += len(combined_df)
            if self.verbose or len(combined_df) >= 20:  # 只在大批次或verbose模式时打印
                print(f"💾 批次保存: {len(combined_df)} 条数据 (累计: {self.total_saved})")
            
            # 清空缓存
            self.buffer = []
            self.last_flush_time = time.time()
            
        except Exception as e:
            print(f"❌ 数据保存失败: {e}")
    
    def final_flush(self):
        """最终刷新所有剩余数据"""
        with self.lock:
            if self.buffer and not (self.crawler_instance and self.crawler_instance.interrupt_flag.is_set()):
                self._flush_to_disk()
                print(f"✅ 最终保存完成，总计: {self.total_saved} 条数据")
            elif self.crawler_instance and self.crawler_instance.interrupt_flag.is_set():
                print(f"⚠️  由于中断，跳过最终数据写入，已保存: {self.total_saved} 条数据")


class SimplePOICrawler:
    """简化版POI爬虫 - 10个持久化Chrome工作线程"""
    
    def __init__(self, num_workers=10, batch_size=50, flush_interval=30, verbose=False, enable_resume=True, show_progress=True):
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.verbose = verbose
        self.enable_resume = enable_resume
        
        # 任务和结果队列
        self.task_queue = queue.Queue()
        self.retry_queue = queue.Queue()  # 专门的重试队列，优先处理
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()
        self.interrupt_flag = threading.Event()  # 中断标志
        
        # 工作线程
        self.workers = []
        
        # 结果缓存池
        self.result_buffer = None
        
        # 断点续传支持
        self.progress_dir = Path("data/progress")
        self.progress_dir.mkdir(parents=True, exist_ok=True)
        self.progress_file = None
        self.processed_indices = set()  # 已处理的索引
        self.current_file_name = None  # 当前处理的文件名
        self.current_output_file = None  # 当前输出文件路径
        
        # 重试优化
        self.retry_cache = set()  # 重试地址缓存，避免重复重试
        
        # 统计信息
        self.total_tasks = 0
        self.processed_tasks = 0
        self.success_count = 0
        self.error_count = 0
        
        # 进度条支持
        self.progress_bar = None
        self.show_progress = show_progress
        self.start_time = None
        self.progress_lock = threading.Lock()  # 进度条更新锁
        
        # 设置信号处理器
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """设置信号处理器用于安全中断"""
        def signal_handler(signum, frame):
            print("\n🚨 接收到中断信号 (Ctrl+C)，正在安全退出...")
            self.interrupt_flag.set()
            self.stop_event.set()
            
            # 关闭进度条
            if self.progress_bar:
                with self.progress_lock:
                    self.progress_bar.close()
                    self.progress_bar = None
            
            print("🔄 正在停止工作线程和清理资源...")
        
        signal.signal(signal.SIGINT, signal_handler)
        if hasattr(signal, 'SIGTERM'):  # Windows 上可能没有 SIGTERM
            signal.signal(signal.SIGTERM, signal_handler)
    
    def discover_input_files(self, pattern="data/input/*区_*.csv"):
        """发现输入文件 - 支持--all功能"""
        files = glob.glob(pattern)
        csv_files = [f for f in files if f.endswith('.csv')]
        csv_files.sort()  # 按文件名排序
        
        if self.verbose:
            print(f"🔍 发现 {len(csv_files)} 个CSV文件:")
            for f in csv_files:
                print(f"  - {f}")
        
        return csv_files
    
    def load_files_from_txt(self, txt_file):
        """从.txt文档加载文件列表"""
        try:
            with open(txt_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            files = []
            for line in lines:
                line = line.strip()
                # 跳过空行和注释行
                if line and not line.startswith('#'):
                    # 支持相对路径和绝对路径
                    if not os.path.isabs(line):
                        line = os.path.join('data/input', line)
                    
                    if os.path.exists(line) and line.endswith('.csv'):
                        files.append(line)
                    elif self.verbose:
                        print(f"⚠️  文件不存在或非CSV: {line}")
            
            if self.verbose:
                print(f"📋 从 {txt_file} 加载了 {len(files)} 个文件")
            
            return files
            
        except Exception as e:
            print(f"❌ 加载TXT文件失败: {e}")
            return []
    
    def load_addresses_from_csv(self, csv_file):
        """从CSV文件加载地址"""
        try:
            df = pd.read_csv(csv_file)
            addresses = []
            
            for index, row in df.iterrows():
                # 优先使用FormattedAddress，然后Address，最后ConvertedAddress
                address = None
                original_address = None
                
                if 'FormattedAddress' in df.columns and pd.notna(row['FormattedAddress']):
                    address = row['FormattedAddress'].strip()
                elif 'Address' in df.columns and pd.notna(row['Address']):
                    address = row['Address']
                elif 'ConvertedAddress' in df.columns and pd.notna(row['ConvertedAddress']):
                    address = row['ConvertedAddress'].strip()
                
                # 保存日文原始地址用于重试
                if 'Address' in df.columns and pd.notna(row['Address']):
                    original_address = row['Address']
                
                if address:
                    addresses.append({
                        'address': address,
                        'original_address': original_address,
                        'index': index
                    })
            
            print(f"📋 加载地址: {len(addresses)} 条")
            return addresses
            
        except Exception as e:
            print(f"❌ 加载CSV文件失败: {e}")
            return []
    
    def _extract_file_name(self, file_path):
        """从文件路径提取文件名作为进度标识"""
        return Path(file_path).stem
    
    def _get_last_processed_index(self):
        """获取最后一个处理的索引"""
        if not self.processed_indices:
            return -1
        return max(self.processed_indices)
    
    def _save_progress(self):
        """保存当前进度到JSON文件 - 优化版（只保存最后处理的索引）"""
        if not self.enable_resume or not self.progress_file or self.interrupt_flag.is_set():
            return
        
        try:
            # 先检查是否已有进度文件，保持原始时间戳
            existing_timestamp = None
            if self.progress_file.exists():
                try:
                    with open(self.progress_file, 'r', encoding='utf-8') as f:
                        existing_data = json.load(f)
                        existing_timestamp = existing_data.get('timestamp')
                except:
                    pass  # 如果读取失败，就使用新的时间戳
            
            progress_data = {
                'file_name': self.current_file_name,
                'output_file': str(self.current_output_file) if self.current_output_file else None,
                'last_processed_index': self._get_last_processed_index(),
                'total_tasks': self.total_tasks,
                'processed_tasks': self.processed_tasks,
                'success_count': self.success_count,
                'error_count': self.error_count,
                'timestamp': existing_timestamp if existing_timestamp is not None else time.time(),  # 保持原时间戳或创建新的
                'last_updated': time.time()  # 添加最后更新时间
            }
            
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
                
            if self.verbose:
                print(f"💾 进度已保存: {self.processed_tasks}/{self.total_tasks}, 最后索引: {self._get_last_processed_index()}")
                
        except Exception as e:
            print(f"⚠️  保存进度失败: {e}")
    
    def _load_progress(self, file_name):
        """加载进度文件"""
        if not self.enable_resume:
            return None
        
        progress_file = self.progress_dir / f"{file_name}_simple_progress.json"
        
        if not progress_file.exists():
            return None
        
        try:
            with open(progress_file, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
            
            # 检查是否是同一个文件的进度
            if progress_data.get('file_name') == file_name:
                # 输出文件路径用于调试和验证
                if self.verbose and 'output_file' in progress_data:
                    last_index = progress_data.get('last_processed_index', -1)
                    print(f"📁 从进度文件加载: 输出路径={progress_data['output_file']}, 最后索引={last_index}")
                return progress_data
                
        except (json.JSONDecodeError, KeyError, FileNotFoundError) as e:
            print(f"⚠️  读取进度文件失败: {e}")
        
        return None
    
    def _cleanup_progress(self):
        """清理进度文件"""
        if self.progress_file and self.progress_file.exists():
            try:
                self.progress_file.unlink()
                if self.verbose:
                    print(f"🧹 进度文件已清理: {self.progress_file.name}")
            except Exception as e:
                print(f"⚠️  清理进度文件失败: {e}")
    
    def start_workers(self):
        """启动工作线程"""
        print(f"🚀 启动 {self.num_workers} 个Chrome工作线程...")
        
        for i in range(self.num_workers):
            worker = ChromeWorker(i, self.task_queue, self.result_queue, self.stop_event, self.verbose, self.retry_queue)
            worker.start()
            self.workers.append(worker)
            time.sleep(1)  # 错开启动时间，避免并发创建driver
        
        print(f"✅ 所有工作线程已启动")
    
    def stop_workers(self):
        """停止工作线程"""
        if not self.interrupt_flag.is_set():
            print("🛑 停止所有工作线程...")
        
        # 设置停止事件
        self.stop_event.set()
        
        # 如果是中断，不等待队列完成，直接停止
        if not self.interrupt_flag.is_set():
            self.task_queue.join()
        
        # 等待工作线程结束（中断时更短的超时）
        timeout = 1 if self.interrupt_flag.is_set() else 5
        for worker in self.workers:
            worker.join(timeout=timeout)
        
        if not self.interrupt_flag.is_set():
            print("✅ 所有工作线程已停止")
        else:
            print("✅ 工作线程已快速停止")
    
    def process_results(self):
        """处理结果队列"""
        print("📊 启动结果处理线程...")
        
        while not self.stop_event.is_set() or not self.result_queue.empty():
            # 检查中断标志
            if self.interrupt_flag.is_set():
                print("⚠️  检测到中断信号，结果处理线程退出")
                break
                
            try:
                result = self.result_queue.get(timeout=1.0)
                
                # 添加到缓存池
                self.result_buffer.add_result(result)
                
                # 记录已处理的索引（用于断点续传）
                if 'index' in result and not result.get('is_retry', False):
                    self.processed_indices.add(result['index'])
                
                # 更新统计
                self.processed_tasks += 1
                if result['success']:
                    self.success_count += 1
                else:
                    self.error_count += 1
                
                # 更新进度条（线程安全）
                if self.progress_bar:
                    with self.progress_lock:
                        self.progress_bar.update(1)
                        # 每5个任务更新一次详细信息，避免过于频繁的更新
                        if self.processed_tasks % 5 == 0:
                            self._update_progress_bar()
                
                # 定期保存进度（每处理10个任务保存一次）
                if self.processed_tasks % 10 == 0 and not self.interrupt_flag.is_set():
                    self._save_progress()
                
                # 检查是否需要使用日文地址重试
                # 只对无效地址进行重试
                if (result['success'] and 
                    result.get('result_type') == 'invalid_address' and  # 只重试无效地址
                    result.get('original_address') and 
                    result['address'] != result['original_address'] and
                    not result.get('is_retry', False) and  # 避免重复重试
                    result.get('original_address') not in self.retry_cache):  # 检查缓存
                    
                    original_address = result.get('original_address')
                    
                    # 记录到重试缓存
                    self.retry_cache.add(original_address)
                    
                    # 使用日文地址重试
                    print(f"🔄 无效地址，使用日文地址重试: {original_address[:30]}...")
                    
                    retry_task = {
                        'address': original_address,
                        'index': result['index'],
                        'original_address': original_address,
                        'is_retry': True
                    }
                    # 放入优先级重试队列，立即处理
                    self.retry_queue.put(retry_task)
                    # 增加总任务数以包含重试任务
                    self.total_tasks += 1
                
                # 调试：记录所有result_type的分布（只在verbose模式）
                if self.verbose and self.processed_tasks % 50 == 0:
                    print(f"📊 Result类型: {result.get('result_type', 'unknown')} | 重试: {result.get('is_retry', False)}")
                
                # 🔧 日志压缩 - 定期报告进度
                if self.verbose or self.processed_tasks % 200 == 0:
                    progress = (self.processed_tasks / self.total_tasks * 100) if self.total_tasks > 0 else 0
                    print(f"📈 总进度: {self.processed_tasks}/{self.total_tasks} ({progress:.1f}%) "
                          f"- 成功: {self.success_count}, 失败: {self.error_count}")
                
                self.result_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"❌ 处理结果异常: {e}")
                continue
    
    def _setup_file_processing(self, input_file, output_file=None):
        """设置文件处理的断点续传参数 - 统一接口"""
        self.current_file_name = self._extract_file_name(input_file)
        self.progress_file = self.progress_dir / f"{self.current_file_name}_simple_progress.json"
        
        # 检查是否有未完成的进度
        progress_data = self._load_progress(self.current_file_name)
        
        # 🔧 断点续传：优先使用保存的输出文件路径
        if progress_data and 'output_file' in progress_data:
            self.current_output_file = progress_data['output_file']
            print(f"🔄 发现未完成的任务，从断点继续...")
            print(f"📊 之前进度: {progress_data['processed_tasks']}/{progress_data['total_tasks']}")
            print(f"📁 续传输出文件: {self.current_output_file}")
            
            # 如果用户指定了不同的输出文件，给出警告
            if output_file and output_file != self.current_output_file:
                print(f"⚠️  用户指定的输出文件与断点续传文件不一致:")
                print(f"   - 断点续传: {self.current_output_file}")
                print(f"   - 用户指定: {output_file}")
                print(f"   - 将使用断点续传文件: {self.current_output_file}")
        else:
            # 没有进度数据，使用指定的输出文件
            if output_file:
                self.current_output_file = output_file
        
        # 加载地址
        addresses = self.load_addresses_from_csv(input_file)
        if not addresses:
            print("❌ 没有有效地址可处理")
            return None
        
        # 处理断点续传
        if progress_data:
            # 恢复统计信息
            last_processed_index = progress_data.get('last_processed_index', -1)
            self.processed_tasks = progress_data.get('processed_tasks', 0)
            self.success_count = progress_data.get('success_count', 0)
            self.error_count = progress_data.get('error_count', 0)
            
            # 重新构建 processed_indices（从 0 到 last_processed_index）
            self.processed_indices = set(range(0, last_processed_index + 1)) if last_processed_index >= 0 else set()
            
            # 过滤出未处理的地址（索引大于 last_processed_index）
            remaining_addresses = [addr for addr in addresses if addr['index'] > last_processed_index]
            print(f"📋 剩余未处理地址: {len(remaining_addresses)} 条 (从索引 {last_processed_index + 1} 开始)")
            
            if not remaining_addresses:
                print("✅ 所有地址已处理完成！")
                self._cleanup_progress()
                return None
                
            addresses = remaining_addresses
        else:
            # 重置统计信息
            self.processed_indices = set()
            self.processed_tasks = 0
            self.success_count = 0
            self.error_count = 0
        
        self.total_tasks = len(addresses) + self.processed_tasks  # 包含已处理的任务数
        
        # 初始化进度条
        if self.show_progress and self.total_tasks > 0:
            self.start_time = time.time()
            remaining_tasks = len(addresses)
            
            with self.progress_lock:
                # 关闭旧的进度条
                if self.progress_bar:
                    self.progress_bar.close()
                
                # 创建新的进度条
                self.progress_bar = tqdm(
                    total=self.total_tasks,
                    initial=self.processed_tasks,
                    desc=f"🔍 {self.current_file_name[:12]}",
                    unit="条",
                    ncols=90,
                    position=0,
                    leave=True,
                    bar_format='{desc}: {percentage:3.0f}%|{bar:20}| {n_fmt}/{total_fmt} {postfix}'
                )
                
                # 更新进度条信息
                self._update_progress_bar()
        
        return addresses
    
    def _update_progress_bar(self):
        """更新进度条显示信息（内部方法，调用时需已获得锁）"""
        if not self.progress_bar:
            return
            
        elapsed_time = time.time() - self.start_time if self.start_time else 0
        speed = self.processed_tasks / elapsed_time if elapsed_time > 0 else 0
        
        # 计算成功率
        success_rate = (self.success_count / self.processed_tasks * 100) if self.processed_tasks > 0 else 0
        
        # 简化的postfix信息
        if speed > 0:
            postfix = f"{success_rate:.0f}%成功 {speed:.1f}/s"
        else:
            postfix = f"{success_rate:.0f}%成功 启动中"
        
        self.progress_bar.set_postfix_str(postfix)
    
    def _finalize_file_processing(self):
        """完成文件处理后的清理工作 - 统一接口"""
        # 关闭进度条（线程安全）
        with self.progress_lock:
            if self.progress_bar:
                self.progress_bar.close()
                self.progress_bar = None
        
        # 保存最终进度并清理（只在未中断时）
        if not self.interrupt_flag.is_set():
            self._save_progress()
            self._cleanup_progress()
        else:
            print("⚠️  由于中断，跳过最终进度保存和清理")
    
    def process_single_file(self, input_file, output_file, workers_started=False):
        """处理单个文件的统一接口 - 支持断点续传"""
        # 设置文件处理参数
        addresses = self._setup_file_processing(input_file, output_file)
        if addresses is None:
            return {'success': False, 'reason': '无地址或已完成'}
        
        # 初始化结果缓存池
        self.result_buffer = ResultBuffer(output_file, self.batch_size, self.flush_interval, self.verbose, self)
        
        # 启动工作线程（如果还没启动）
        if not workers_started:
            self.start_workers()
            
            # 启动结果处理线程
            result_thread = threading.Thread(target=self.process_results, daemon=True)
            result_thread.start()
        
        try:
            # 添加任务到队列
            print(f"📤 添加 {len(addresses)} 个任务到队列...")
            for addr_data in addresses:
                self.task_queue.put(addr_data)
            
            # 等待当前文件的任务完成
            self.task_queue.join()
            
            # 等待结果处理完成
            while not self.result_queue.empty():
                time.sleep(0.1)
            
            # 最终刷新缓存
            if self.result_buffer:
                self.result_buffer.final_flush()
            
            # 完成文件处理
            self._finalize_file_processing()
            
            return {
                'success': True, 
                'processed': self.processed_tasks,
                'success_count': self.success_count,
                'error_count': self.error_count
            }
            
        except Exception as e:
            # 即使出错也保存进度
            try:
                self._save_progress()
            except:
                pass
            return {'success': False, 'reason': str(e)}
    
    def crawl_from_csv(self, input_file, output_file):
        """从CSV文件爬取POI数据 - 支持断点续传"""
        print(f"⏰ 等待所有任务完成...")
        start_time = time.time()
        
        try:
            # 使用统一的处理接口
            result = self.process_single_file(input_file, output_file, workers_started=False)
            
            if not result['success']:
                print(f"❌ 处理失败: {result.get('reason', '未知错误')}")
                return
            
            elapsed_time = time.time() - start_time
            
            print(f"🎉 所有任务完成！")
            print(f"⏱️  耗时: {elapsed_time/60:.1f} 分钟")
            print(f"📊 总计: {result['processed']} 个任务")
            print(f"✅ 成功: {result['success_count']}")
            print(f"❌ 失败: {result['error_count']}")
            if result['processed'] > 0:
                print(f"📈 成功率: {(result['success_count']/result['processed']*100):.1f}%")
            
        except KeyboardInterrupt:
            # Ctrl+C 已经由信号处理器处理，这里只需要静默退出
            pass
        
        finally:
            # 停止工作线程
            self.stop_workers()
            
            if not self.interrupt_flag.is_set():
                print(f"📁 结果已保存到: {output_file}")
            else:
                print(f"⚠️  由于中断，部分结果可能未保存: {output_file}")
    
    def crawl_multiple_files(self, file_list, output_dir="data/output"):
        """批量处理多个CSV文件"""
        if not file_list:
            print("❌ 没有文件需要处理")
            return
        
        print(f"🚀 开始批量处理 {len(file_list)} 个文件")
        print("="*60)
        
        all_success = 0
        all_errors = 0
        processed_files = []
        start_time = time.time()
        
        for i, file_path in enumerate(file_list):
            file_name = os.path.basename(file_path)
            print(f"\n📂 处理第 {i+1}/{len(file_list)} 个文件: {file_name}")
            print("-" * 50)
            
            # 🔧 智能输出文件名生成 - 支持断点续传
            input_path = Path(file_path)
            
            # 检查是否有断点续传的进度文件
            file_name = self._extract_file_name(file_path)
            progress_data = self._load_progress(file_name)
            
            if progress_data and 'output_file' in progress_data:
                # 断点续传：使用之前保存的输出文件路径
                output_file = progress_data['output_file']
                print(f"🔄 断点续传，使用之前的输出文件: {output_file}")
            else:
                # 新文件：生成唯一的输出文件名
                timestamp = int(time.time())
                import random
                unique_id = f"{timestamp}_{random.randint(1000, 9999)}"
                output_file = f"{output_dir}/{input_path.stem}_simple_{unique_id}.csv"
                print(f"📝 新文件，创建输出文件: {output_file}")
            
            # 确保输出目录存在
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            
            try:
                # 使用统一的处理接口
                workers_already_started = (i > 0)  # 从第二个文件开始，工作线程已经启动
                result = self.process_single_file(file_path, output_file, workers_already_started)
                
                if not result['success']:
                    processed_files.append(f"{file_name}: {result.get('reason', '处理失败')}")
                    continue
                
                # 统计结果
                all_success += result['success_count']
                all_errors += result['error_count']
                processed_files.append(f"{file_name}: 成功{result['success_count']}, 失败{result['error_count']}")
                
                print(f"✅ {file_name} 完成 - 成功: {result['success_count']}, 失败: {result['error_count']}")
                print(f"📁 输出文件: {output_file}")
                
            except Exception as e:
                print(f"❌ 处理文件 {file_name} 时出错: {e}")
                processed_files.append(f"{file_name}: 处理失败")
                continue
        
        # 停止工作线程
        self.stop_workers()
        
        # 总结报告
        total_time = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"🎉 批量处理完成！")
        print(f"{'='*60}")
        print(f"⏱️  总耗时: {total_time/60:.1f} 分钟")
        print(f"📊 处理文件: {len(processed_files)} 个")
        print(f"✅ 总成功: {all_success}")
        print(f"❌ 总失败: {all_errors}")
        success_rate = (all_success / (all_success + all_errors) * 100) if (all_success + all_errors) > 0 else 0
        print(f"📈 总成功率: {success_rate:.1f}%")
        
        print(f"\n📋 文件处理详情:")
        for file_summary in processed_files:
            print(f"  {file_summary}")
        
        print(f"\n📁 所有结果保存在: {output_dir}/")
        
        return all_success, all_errors


def main():
    parser = argparse.ArgumentParser(description='简化版POI爬虫 - 10个持久化Chrome工作线程')
    parser.add_argument('input_file', nargs='?', help='输入CSV文件路径')
    parser.add_argument('--all', action='store_true', help='批量处理所有区域文件 (data/input/*区_*.csv)')
    parser.add_argument('--file-list', type=str, help='从TXT文件读取要处理的文件列表')
    parser.add_argument('--pattern', type=str, help='使用通配符模式选择文件，如 "data/input/*区_complete*.csv"')
    parser.add_argument('--output', '-o', default=None, help='输出文件路径（单文件模式）或输出目录（批量模式）')
    parser.add_argument('--workers', '-w', type=int, default=10, help='工作线程数 (默认: 10)')
    parser.add_argument('--batch-size', '-b', type=int, default=50, help='批次大小 (默认: 50)')
    parser.add_argument('--flush-interval', '-f', type=int, default=30, help='刷新间隔秒数 (默认: 30)')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细日志输出模式')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续传功能')
    parser.add_argument('--no-progress', action='store_true', help='禁用进度条显示')
    
    args = parser.parse_args()
    
    # 参数验证
    if not args.all and not args.file_list and not args.pattern and not args.input_file:
        parser.error("必须提供输入文件，或使用 --all、--file-list、--pattern 选项之一")
    
    # 创建爬虫实例
    crawler = SimplePOICrawler(
        num_workers=args.workers,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        verbose=args.verbose,
        enable_resume=not args.no_resume,
        show_progress=not args.no_progress
    )
    
    # 确定要处理的文件列表
    file_list = []
    
    if args.all:
        # --all: 自动发现所有区域文件
        file_list = crawler.discover_input_files()
        if not file_list:
            print("❌ 在 data/input/ 目录下没有找到符合模式的CSV文件")
            return
        print(f"🔍 --all 模式: 发现 {len(file_list)} 个文件")
        
    elif args.file_list:
        # --file-list: 从TXT文件加载
        if not os.path.exists(args.file_list):
            print(f"❌ 文件列表不存在: {args.file_list}")
            return
        file_list = crawler.load_files_from_txt(args.file_list)
        if not file_list:
            print(f"❌ 从 {args.file_list} 没有加载到有效的CSV文件")
            return
        print(f"📋 --file-list 模式: 从 {args.file_list} 加载了 {len(file_list)} 个文件")
        
    elif args.pattern:
        # --pattern: 使用通配符模式
        file_list = crawler.discover_input_files(args.pattern)
        if not file_list:
            print(f"❌ 模式 '{args.pattern}' 没有匹配到任何CSV文件")
            return
        print(f"🔍 --pattern 模式: 模式 '{args.pattern}' 匹配到 {len(file_list)} 个文件")
        
    else:
        # 单文件模式
        if not os.path.exists(args.input_file):
            print(f"❌ 输入文件不存在: {args.input_file}")
            return
        file_list = [args.input_file]
        print(f"📄 单文件模式: {args.input_file}")
    
    # 显示要处理的文件
    if args.verbose and len(file_list) > 1:
        print(f"\n📋 将要处理的文件:")
        for i, f in enumerate(file_list, 1):
            print(f"  {i:2d}. {f}")
        print()
    
    # 执行处理
    if len(file_list) == 1:
        # 单文件处理模式
        input_file = file_list[0]
        
        # 🔧 智能输出文件名生成 - 支持断点续传
        if not args.output:
            input_path = Path(input_file)
            
            # 检查是否有断点续传的进度文件
            file_name = crawler._extract_file_name(input_file)
            progress_data = crawler._load_progress(file_name)
            
            if progress_data and 'output_file' in progress_data:
                # 断点续传：使用之前保存的输出文件路径
                args.output = progress_data['output_file']
                print(f"🔄 断点续传，使用之前的输出文件: {args.output}")
            else:
                # 新文件：生成唯一的输出文件名
                timestamp = int(time.time())
                import random
                unique_id = f"{timestamp}_{random.randint(1000, 9999)}"
                args.output = f"data/output/{input_path.stem}_simple_{unique_id}.csv"
                print(f"📝 新文件，创建输出文件: {args.output}")
        
        # 确保输出目录存在
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        
        print(f"🚀 简化版POI爬虫启动")
        print(f"📁 输入文件: {input_file}")
        print(f"📁 输出文件: {args.output}")
        print(f"👥 工作线程: {args.workers}")
        print(f"📦 批次大小: {args.batch_size}")
        print(f"⏰ 刷新间隔: {args.flush_interval}秒")
        print(f"🔊 详细日志: {'开启' if args.verbose else '关闭'}")
        print(f"🔄 断点续传: {'开启' if not args.no_resume else '关闭'}")
        print(f"📊 进度条: {'开启' if not args.no_progress else '关闭'}")
        print("="*60)
        
        crawler.crawl_from_csv(input_file, args.output)
        
    else:
        # 批量处理模式
        output_dir = args.output if args.output else "data/output"
        
        print(f"🚀 简化版POI爬虫启动（批量模式）")
        print(f"📂 处理文件: {len(file_list)} 个")
        print(f"📁 输出目录: {output_dir}")
        print(f"👥 工作线程: {args.workers}")
        print(f"📦 批次大小: {args.batch_size}")
        print(f"⏰ 刷新间隔: {args.flush_interval}秒")
        print(f"🔊 详细日志: {'开启' if args.verbose else '关闭'}")
        print(f"🔄 断点续传: {'开启' if not args.no_resume else '关闭'}")
        print(f"📊 进度条: {'开启' if not args.no_progress else '关闭'}")
        print("="*60)
        
        crawler.crawl_multiple_files(file_list, output_dir)


if __name__ == "__main__":
    main()