#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终修复版POI爬虫 - 解决所有已知问题
"""

import pandas as pd
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import json
from pathlib import Path
import queue
import sys

# 使用增强版的工具函数
try:
    from enhanced_poi_extractor import (
        get_building_type_robust, is_building,
        safe_get_building_name, safe_get_coords, safe_get_all_poi_info
    )
    from enhanced_driver_actions import click_on_more_button, scroll_poi_section
    print("✅ 增强版工具函数导入成功")
except ImportError as e:
    print(f"❌ 工具函数导入失败: {e}")
    sys.exit(1)

# 简化日志
logging.basicConfig(level=logging.ERROR)

class RobustWebDriverPool:
    """高性能WebDriver池 - 优化版"""
    
    def __init__(self, pool_size=3, headless=True):
        self.pool_size = pool_size
        self.headless = headless
        self.available_drivers = queue.Queue()
        self.all_drivers = []
        self._initialize_pool()
    
    def _create_driver(self):
        options = webdriver.ChromeOptions()
        
        # 基础优化参数
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-logging')
        options.add_argument('--disable-extensions')
        
        # 性能优化参数
        options.add_argument('--disable-images')
        options.add_argument('--disable-plugins')
        options.add_argument('--disable-plugins-discovery')
        options.add_argument('--disable-preconnect')
        options.add_argument('--disable-prefetch')
        options.add_argument('--disable-background-networking')
        options.add_argument('--disable-background-timer-throttling')
        options.add_argument('--disable-backgrounding-occluded-windows')
        options.add_argument('--disable-renderer-backgrounding')
        options.add_argument('--disable-features=TranslateUI')
        options.add_argument('--disable-default-apps')
        options.add_argument('--no-default-browser-check')
        options.add_argument('--disable-hang-monitor')
        options.add_argument('--disable-prompt-on-repost')
        
        # 内存优化
        options.add_argument('--memory-pressure-off')
        options.add_argument('--max_old_space_size=4096')
        
        # 网络优化
        options.add_argument('--aggressive-cache-discard')
        options.add_argument('--disable-background-downloads')
        
        # 强制无头模式以提高性能
        if self.headless:
            options.add_argument('--headless=new')  # 使用新版无头模式
            options.add_argument('--disable-web-security')
            options.add_argument('--allow-running-insecure-content')
            options.add_argument('--window-size=1920,1080')
        
        # 禁用不必要的功能
        prefs = {
            'profile.default_content_setting_values': {
                'images': 2,  # 禁用图片
                'plugins': 2,  # 禁用插件
                'popups': 2,  # 禁用弹窗
                'geolocation': 2,  # 禁用地理位置
                'notifications': 2,  # 禁用通知
                'media_stream': 2,  # 禁用媒体流
            },
            'profile.managed_default_content_settings': {
                'images': 2
            }
        }
        options.add_experimental_option('prefs', prefs)
        
        # 设置页面加载策略
        options.add_argument('--page-load-strategy=eager')  # 不等待所有资源加载完成
        
        try:
            # 尝试使用系统Chrome
            driver = webdriver.Chrome(options=options)
        except:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
        
        # 设置页面加载超时
        driver.set_page_load_timeout(15)
        driver.implicitly_wait(5)
        
        return driver
    
    def _initialize_pool(self):
        print(f"🚀 正在初始化WebDriver池 (大小: {self.pool_size})...")
        for i in range(self.pool_size):
            try:
                driver = self._create_driver()
                self.available_drivers.put(driver)
                self.all_drivers.append(driver)
                print(f"  ✅ WebDriver {i+1} 创建成功")
            except Exception as e:
                print(f"  ❌ WebDriver {i+1} 创建失败: {e}")
    
    def get_driver(self, timeout=30):
        try:
            return self.available_drivers.get(timeout=timeout)
        except queue.Empty:
            return None
    
    def release_driver(self, driver):
        if driver in self.all_drivers:
            self.available_drivers.put(driver)
    
    def close_all(self):
        print("🔄 正在关闭所有WebDriver...")
        for driver in self.all_drivers:
            try:
                driver.quit()
            except:
                pass

class FinalPOICrawler:
    """最终版POI爬虫"""
    
    def __init__(self, config):
        self.config = config
        self.driver_pool = RobustWebDriverPool(config['driver_pool_size'], config['headless'])
        self.data_buffer = []
        self.processed_count = 0
        self.success_count = 0
        self.failed_addresses = []
        self._data_lock = threading.Lock()
        self.start_time = time.time()
    
    def _wait_for_page_load(self, driver, timeout=10):
        """优化的页面加载等待"""
        try:
            # 等待页面基本加载完成
            WebDriverWait(driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            
            # 等待Google Maps特定元素，但时间更短
            try:
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                pass
            
            # 减少等待时间
            time.sleep(1)
            return True
        except TimeoutException:
            return False
    
    def _crawl_single_poi(self, address, driver):
        max_retries = self.config['retry_times']
        
        for attempt in range(max_retries):
            try:
                print(f"🔍 正在爬取: {address} (尝试 {attempt+1}/{max_retries})")
                
                url = f'https://www.google.com/maps/place/{address}'
                driver.get(url)
                
                if not self._wait_for_page_load(driver, self.config['timeout']):
                    print("  ⚠️ 页面加载超时")
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    return None
                
                # 使用修复版函数获取地点类型
                place_type = get_building_type_robust(driver)
                
                # 使用新的判断逻辑
                if not is_building(place_type):
                    print(f"  ⏭️ 非建筑物类型: '{place_type}'，跳过")
                    return None
                
                print(f"  ✅ 确认为建筑物: '{place_type}'")
                
                # 获取建筑物名称
                place_name = safe_get_building_name(driver)
                print(f"  🏢 建筑名: {place_name}")
                
                # 检查更多按钮
                try:
                    more_button = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'M77dve'))
                    )
                    print("  📋 找到更多按钮，正在处理...")
                    click_on_more_button(driver)
                    scroll_poi_section(driver)
                except TimeoutException:
                    print("  📋 没有更多按钮")
                
                # 获取POI信息
                df = safe_get_all_poi_info(driver)
                if df.empty:
                    print("  ❌ 未找到POI信息")
                    return None
                
                # 获取坐标
                lat, lng = safe_get_coords(driver.current_url)
                print(f"  🌍 坐标: ({lat}, {lng})")
                
                # 添加额外信息
                df['blt_name'] = place_name
                df['place_type'] = place_type
                df['lat'] = lat
                df['lng'] = lng
                df['crawl_time'] = pd.Timestamp.now()
                df['source_address'] = address
                
                print(f"  ✅ 成功获取 {len(df)} 个POI")
                return df
                
            except Exception as e:
                print(f"  ❌ 爬取失败: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return None
        
        return None
    
    def _process_address(self, address):
        driver = self.driver_pool.get_driver()
        if driver is None:
            print(f"❌ 无法获取WebDriver: {address}")
            return None, address
        
        try:
            result = self._crawl_single_poi(address, driver)
            return result, address
        finally:
            self.driver_pool.release_driver(driver)
    
    def _save_batch_data(self, force=False):
        with self._data_lock:
            if len(self.data_buffer) >= self.config['batch_size'] or force:
                if self.data_buffer:
                    df = pd.concat(self.data_buffer, ignore_index=True)
                    
                    file_exists = Path(self.config['output_file']).exists()
                    df.to_csv(
                        self.config['output_file'], 
                        mode='a', 
                        header=not file_exists, 
                        index=False,
                        encoding='utf-8'
                    )
                    
                    print(f"💾 保存了 {len(self.data_buffer)} 条数据到 {self.config['output_file']}")
                    self.data_buffer.clear()
    
    def _save_checkpoint(self, processed_addresses):
        checkpoint_data = {
            'processed_addresses': processed_addresses,
            'processed_count': self.processed_count,
            'success_count': self.success_count,
            'failed_addresses': self.failed_addresses,
            'timestamp': pd.Timestamp.now().isoformat()
        }
        
        with open('checkpoint.json', 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
    
    def _load_checkpoint(self):
        try:
            with open('checkpoint.json', 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
                self.processed_count = checkpoint_data.get('processed_count', 0)
                self.success_count = checkpoint_data.get('success_count', 0)
                self.failed_addresses = checkpoint_data.get('failed_addresses', [])
                return checkpoint_data.get('processed_addresses', [])
        except FileNotFoundError:
            return []
    
    def _print_progress(self):
        elapsed = time.time() - self.start_time
        if self.processed_count > 0:
            avg_time = elapsed / self.processed_count
            remaining = len(self.remaining_addresses) - self.processed_count
            eta = remaining * avg_time / 60
            
            success_rate = self.success_count / self.processed_count * 100 if self.processed_count > 0 else 0
            
            print(f"\n📊 进度报告:")
            print(f"  处理: {self.processed_count}/{len(self.remaining_addresses)}")
            print(f"  成功: {self.success_count} ({success_rate:.1f}%)")
            print(f"  失败: {len(self.failed_addresses)}")
            print(f"  平均: {avg_time:.1f}s/个")
            print(f"  预计剩余: {eta:.1f}分钟")
            print("-" * 50)
    
    def process_addresses(self, addresses):
        processed_addresses = self._load_checkpoint()
        self.remaining_addresses = [addr for addr in addresses if addr not in processed_addresses]
        
        print(f"📊 初始统计:")
        print(f"  总地址数: {len(addresses)}")
        print(f"  已处理: {len(processed_addresses)}")
        print(f"  剩余: {len(self.remaining_addresses)}")
        print(f"  历史成功: {self.success_count}")
        print(f"  历史失败: {len(self.failed_addresses)}")
        
        if not self.remaining_addresses:
            print("🎉 所有地址已处理完成")
            return
        
        print(f"\n🚀 开始处理，使用 {self.config['max_workers']} 个并发线程...")
        
        with ThreadPoolExecutor(max_workers=self.config['max_workers']) as executor:
            future_to_address = {
                executor.submit(self._process_address, addr): addr 
                for addr in self.remaining_addresses
            }
            
            for future in as_completed(future_to_address):
                original_address = future_to_address[future]
                try:
                    result, address = future.result()
                    
                    if result is not None:
                        with self._data_lock:
                            self.data_buffer.append(result)
                        self.success_count += 1
                    else:
                        self.failed_addresses.append(address)
                    
                    self.processed_count += 1
                    processed_addresses.append(address)
                    
                    # 显示进度
                    if self.processed_count % 5 == 0:
                        self._print_progress()
                    
                    self._save_batch_data()
                    
                    if self.processed_count % self.config['checkpoint_interval'] == 0:
                        self._save_checkpoint(processed_addresses)
                    
                except Exception as e:
                    print(f"❌ 处理地址 {original_address} 时发生错误: {e}")
                    self.failed_addresses.append(original_address)
        
        self._save_batch_data(force=True)
        self._save_checkpoint(processed_addresses)
        
        elapsed = time.time() - self.start_time
        success_rate = self.success_count / (self.success_count + len(self.failed_addresses)) * 100
        
        print(f"\n🎯 最终报告:")
        print(f"  总耗时: {elapsed/60:.1f} 分钟")
        print(f"  成功: {self.success_count}")
        print(f"  失败: {len(self.failed_addresses)}")
        print(f"  成功率: {success_rate:.1f}%")
        print(f"  平均速度: {elapsed/len(self.remaining_addresses):.1f}秒/个")
    
    def close(self):
        self.driver_pool.close_all()

def main():
    # 高性能配置 - 优化版
    config = {
        'max_workers': 4,  # 增加并发数
        'driver_pool_size': 4,  # 增加WebDriver池
        'batch_size': 15,  # 适中的批量大小
        'timeout': 12,  # 减少超时时间
        'retry_times': 2,  # 减少重试次数
        'headless': True,  # 强制无头模式
        'checkpoint_interval': 30,  # 更频繁保存检查点
        'input_file': 'data/input/千代田区_complete_1751433587.csv',
        'output_file': '千代田区_poi_高性能版.csv'
    }
    
    print("🎯 最终版POI爬虫")
    print("=" * 60)
    
    try:
        df_input = pd.read_csv(config['input_file'])
        print(f"📄 成功读取: {config['input_file']}")
        print(f"  数据行数: {len(df_input)}")
        
        addresses = df_input['Address'].dropna().tolist()
        print(f"📍 准备处理 {len(addresses)} 个地址")
        
        # 处理所有地址 - 生产模式
        print(f"🚀 生产模式：处理全部 {len(addresses)} 个地址")
        
        crawler = FinalPOICrawler(config)
        crawler.process_addresses(addresses)
        
    except Exception as e:
        print(f"❌ 运行失败: {e}")
    finally:
        try:
            crawler.close()
        except:
            pass

if __name__ == "__main__":
    main()