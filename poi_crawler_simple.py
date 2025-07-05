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
from concurrent.futures import ThreadPoolExecutor

# 导入现有的POI提取函数
from info_tool import get_building_type, get_building_name, get_all_poi_info, get_coords, wait_for_coords_url
from driver_action import click_on_more_button, scroll_poi_section


class ChromeWorker(threading.Thread):
    """持久化Chrome工作线程"""
    
    def __init__(self, worker_id, task_queue, result_queue, stop_event, verbose=False):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.stop_event = stop_event
        self.verbose = verbose
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
                    # 从队列获取任务，超时1秒
                    task = self.task_queue.get(timeout=1.0)
                    
                    # 处理任务
                    result = self.process_task(task)
                    
                    # 提交结果
                    self.result_queue.put(result)
                    
                    # 标记任务完成
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
        
        try:
            # 调用现有的POI提取逻辑
            result = self.crawl_poi_info(address)
            
            if result.get('status') == 'success':
                return {
                    'success': True,
                    'data': result.get('data'),
                    'address': address,
                    'index': index,
                    'worker_id': self.worker_id,
                    'poi_count': result.get('poi_count', 0),
                    'result_type': result.get('result_type', 'unknown')
                }
            else:
                return {
                    'success': False,
                    'error': result.get('error_message', 'POI提取失败'),
                    'address': address,
                    'index': index,
                    'worker_id': self.worker_id
                }
                
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'address': address,
                'index': index,
                'worker_id': self.worker_id
            }
    
    def crawl_poi_info(self, address):
        """POI信息爬取 - 基于现有代码简化版"""
        url = f'https://www.google.com/maps/place/{address}'
        
        try:
            self.driver.get(url)
            
            # 快速检查酒店类别页面
            if self.has_hotel_category(address):
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
                    
            # 尝试展开POI列表
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
                try:
                    current_url = self.driver.current_url
                    lat, lng = get_coords(current_url)
                except Exception as e:
                    lat, lng = None, None
                
                df['blt_name'] = place_name
                df['lat'] = lat
                df['lng'] = lng
                
                # 单地址完成总结
                if self.verbose:
                    print(f"{address[:30]}...  | POI: {poi_count} | 状态: 已保存")


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
                    if self.verbose:
                        print(f"{address[:30]}...  | 类型: {place_type} | POI: 0 | 非商业建筑")
                    
                    return {
                        'data': None,
                        'status': 'success',
                        'result_type': 'building_no_poi',
                        'poi_count': 0,
                        'is_building': True
                    }
                else:
                    return {
                        'data': None,
                        'status': 'success',
                        'result_type': 'not_building',
                        'poi_count': 0,
                        'is_building': False
                    }
                
        except TimeoutException:
            return {
                'data': None,
                'status': 'error',
                'error_message': '页面加载超时',
                'result_type': 'timeout_error',
                'poi_count': 0,
                'is_building': False
            }
        except Exception as e:
            return {
                'data': None,
                'status': 'error',
                'error_message': str(e),
                'result_type': 'processing_error',
                'poi_count': 0,
                'is_building': False
            }
    
    def has_hotel_category(self, address):
        """检查是否是酒店类别页面"""
        try:
            # 检查酒店类别标题
            selectors = [
                "h2.kPvgOb.fontHeadlineSmall",
                "div.aIiAFe h1",
                "h1.jRccSf",
                "h1.ZoUhNb"
            ]
            
            for selector in selectors:
                try:
                    elements = self.driver.find_elements("css selector", selector)
                    for element in elements:
                        text = element.text.strip().lower()
                        if any(keyword in text for keyword in ["酒店", "ホテル", "hotel", "lodging", "accommodation"]):
                            if self.verbose:
                                print(f"🏨 检测到酒店页面: {text} | {address[:30]}...")
                            return True
                except:
                    continue
                    
            return False
        except:
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
    
    def __init__(self, output_file, batch_size=50, flush_interval=30, verbose=False):
        self.output_file = Path(output_file)
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.verbose = verbose
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
        """创建CSV文件头部"""
        if not self.output_file.exists():
            header_df = pd.DataFrame(columns=['name', 'rating', 'class', 'add', 'comment_count', 'blt_name', 'lat', 'lng'])
            header_df.to_csv(self.output_file, index=False, encoding='utf-8-sig')
            if self.verbose:
                print(f"📝 创建输出文件: {self.output_file}")
    
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
            if self.buffer:
                self._flush_to_disk()
                print(f"✅ 最终保存完成，总计: {self.total_saved} 条数据")


class SimplePOICrawler:
    """简化版POI爬虫 - 10个持久化Chrome工作线程"""
    
    def __init__(self, num_workers=10, batch_size=50, flush_interval=30, verbose=False):
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.verbose = verbose
        
        # 任务和结果队列
        self.task_queue = queue.Queue()
        self.result_queue = queue.Queue()
        self.stop_event = threading.Event()
        
        # 工作线程
        self.workers = []
        
        # 结果缓存池
        self.result_buffer = None
        
        # 统计信息
        self.total_tasks = 0
        self.processed_tasks = 0
        self.success_count = 0
        self.error_count = 0
    
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
                if 'FormattedAddress' in df.columns and pd.notna(row['FormattedAddress']):
                    address = row['FormattedAddress'].strip()
                elif 'Address' in df.columns and pd.notna(row['Address']):
                    address = row['Address']
                elif 'ConvertedAddress' in df.columns and pd.notna(row['ConvertedAddress']):
                    address = row['ConvertedAddress'].strip()
                
                if address:
                    addresses.append({
                        'address': address,
                        'index': index
                    })
            
            print(f"📋 加载地址: {len(addresses)} 条")
            return addresses
            
        except Exception as e:
            print(f"❌ 加载CSV文件失败: {e}")
            return []
    
    def start_workers(self):
        """启动工作线程"""
        print(f"🚀 启动 {self.num_workers} 个Chrome工作线程...")
        
        for i in range(self.num_workers):
            worker = ChromeWorker(i, self.task_queue, self.result_queue, self.stop_event, self.verbose)
            worker.start()
            self.workers.append(worker)
            time.sleep(1)  # 错开启动时间，避免并发创建driver
        
        print(f"✅ 所有工作线程已启动")
    
    def stop_workers(self):
        """停止工作线程"""
        print("🛑 停止所有工作线程...")
        
        # 设置停止事件
        self.stop_event.set()
        
        # 等待队列完成
        self.task_queue.join()
        
        # 等待工作线程结束
        for worker in self.workers:
            worker.join(timeout=5)
        
        print("✅ 所有工作线程已停止")
    
    def process_results(self):
        """处理结果队列"""
        print("📊 启动结果处理线程...")
        
        while not self.stop_event.is_set() or not self.result_queue.empty():
            try:
                result = self.result_queue.get(timeout=1.0)
                
                # 添加到缓存池
                self.result_buffer.add_result(result)
                
                # 更新统计
                self.processed_tasks += 1
                if result['success']:
                    self.success_count += 1
                else:
                    self.error_count += 1
                    # 检查是否需要使用日文地址重试
                    if (not result.get('is_building', True) and 
                        result.get('poi_count', 0) == 0 and 
                        result.get('original_address') and 
                        result['address'] != result['original_address']):
                        
                        # 使用日文地址重试
                        if self.verbose:
                            print(f"🔄 非建筑物且POI为0，使用日文地址重试: {result['original_address'][:30]}...")
                        
                        retry_task = {
                            'address': result['original_address'],
                            'index': result['index'],
                            'original_address': result['original_address'],
                            'is_retry': True
                        }
                        self.task_queue.put(retry_task)
                
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
    
    def crawl_from_csv(self, input_file, output_file):
        """从CSV文件爬取POI数据"""
        # 加载地址
        addresses = self.load_addresses_from_csv(input_file)
        if not addresses:
            print("❌ 没有有效地址可处理")
            return
        
        self.total_tasks = len(addresses)
        
        # 初始化结果缓存池
        self.result_buffer = ResultBuffer(output_file, self.batch_size, self.flush_interval, self.verbose)
        
        # 启动工作线程
        self.start_workers()
        
        # 启动结果处理线程
        result_thread = threading.Thread(target=self.process_results, daemon=True)
        result_thread.start()
        
        try:
            # 添加任务到队列
            print(f"📤 添加 {len(addresses)} 个任务到队列...")
            for addr_data in addresses:
                self.task_queue.put(addr_data)
            
            print(f"⏰ 等待所有任务完成...")
            start_time = time.time()
            
            # 等待所有任务完成
            self.task_queue.join()
            
            # 等待结果处理完成
            self.result_queue.join()
            
            elapsed_time = time.time() - start_time
            
            print(f"🎉 所有任务完成！")
            print(f"⏱️  耗时: {elapsed_time/60:.1f} 分钟")
            print(f"📊 总计: {self.processed_tasks} 个任务")
            print(f"✅ 成功: {self.success_count}")
            print(f"❌ 失败: {self.error_count}")
            print(f"📈 成功率: {(self.success_count/self.processed_tasks*100):.1f}%")
            
        except KeyboardInterrupt:
            print("\n🛑 收到中断信号，正在安全退出...")
        
        finally:
            # 停止工作线程
            self.stop_workers()
            
            # 最终刷新缓存
            if self.result_buffer:
                self.result_buffer.final_flush()
            
            print(f"📁 结果已保存到: {output_file}")
    
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
            
            # 为每个文件生成唯一的输出文件名
            input_path = Path(file_path)
            timestamp = int(time.time())
            import random
            unique_id = f"{timestamp}_{random.randint(1000, 9999)}"
            output_file = f"{output_dir}/{input_path.stem}_simple_{unique_id}.csv"
            
            # 确保输出目录存在
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            
            try:
                # 加载地址
                addresses = self.load_addresses_from_csv(file_path)
                if not addresses:
                    print(f"⚠️  {file_name}: 没有有效地址，跳过")
                    processed_files.append(f"{file_name}: 跳过（无地址）")
                    continue
                
                # 重置统计信息
                self.total_tasks = len(addresses)
                self.processed_tasks = 0
                self.success_count = 0
                self.error_count = 0
                
                # 初始化结果缓存池
                self.result_buffer = ResultBuffer(output_file, self.batch_size, self.flush_interval, self.verbose)
                
                # 启动工作线程（只在第一个文件时启动）
                if i == 0:
                    self.start_workers()
                    
                    # 启动结果处理线程
                    result_thread = threading.Thread(target=self.process_results, daemon=True)
                    result_thread.start()
                
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
                
                # 统计结果
                all_success += self.success_count
                all_errors += self.error_count
                processed_files.append(f"{file_name}: 成功{self.success_count}, 失败{self.error_count}")
                
                print(f"✅ {file_name} 完成 - 成功: {self.success_count}, 失败: {self.error_count}")
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
    
    args = parser.parse_args()
    
    # 参数验证
    if not args.all and not args.file_list and not args.pattern and not args.input_file:
        parser.error("必须提供输入文件，或使用 --all、--file-list、--pattern 选项之一")
    
    # 创建爬虫实例
    crawler = SimplePOICrawler(
        num_workers=args.workers,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        verbose=args.verbose
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
        
        # 📦 输出路径加唯一命名（防重复覆盖）
        if not args.output:
            input_path = Path(input_file)
            timestamp = int(time.time())
            # 使用时间戳和随机数确保唯一性
            import random
            unique_id = f"{timestamp}_{random.randint(1000, 9999)}"
            args.output = f"data/output/{input_path.stem}_simple_{unique_id}.csv"
        
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
        print("="*60)
        
        crawler.crawl_multiple_files(file_list, output_dir)


if __name__ == "__main__":
    main()