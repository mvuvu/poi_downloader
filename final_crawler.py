#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
最终修复版POI爬虫 - 解决所有已知问题
"""

import os
import warnings
import sys

# 屏蔽所有警告信息
warnings.filterwarnings('ignore')
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
os.environ['PYTHONWARNINGS'] = 'ignore'

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

# 使用增强版的工具函数
try:
    from enhanced_poi_extractor import (
        get_building_type_robust, is_building,
        safe_get_building_name, safe_get_coords, safe_get_all_poi_info
    )
    from enhanced_driver_actions import click_on_more_button, scroll_poi_section, get_poi_count_enhanced
    from simple_file_selector import get_simple_file_config
    print("✅ 增强版工具函数导入成功")
except ImportError as e:
    print(f"❌ 工具函数导入失败: {e}")
    sys.exit(1)

# 完全禁用日志
logging.basicConfig(level=logging.CRITICAL)
logging.getLogger('selenium').setLevel(logging.CRITICAL)
logging.getLogger('urllib3').setLevel(logging.CRITICAL)

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
        
        # 屏蔽警告和错误信息
        options.add_argument('--log-level=3')
        options.add_argument('--silent')
        options.add_argument('--disable-dev-tools')
        options.add_argument('--disable-features=VizDisplayCompositor')
        options.add_argument('--disable-ipc-flooding-protection')
        
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
            # 尝试使用系统Chrome (屏蔽Service日志)
            service = Service()
            service.log_path = os.devnull
            driver = webdriver.Chrome(service=service, options=options)
        except:
            # 屏蔽webdriver manager的日志
            os.environ['WDM_LOG_LEVEL'] = '0'
            os.environ['WDM_PRINT_FIRST_LINE'] = 'False'
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            service.log_path = os.devnull
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
        
        # 处理结果记录
        result_info = {
            'address': address,
            'status': 'failed',
            'reason': '',
            'place_type': '',
            'place_name': '',
            'has_expand_button': False,
            'poi_count': 0,
            'attempt_count': 0
        }
        
        for attempt in range(max_retries):
            result_info['attempt_count'] = attempt + 1
            try:
                url = f'https://www.google.com/maps/place/{address}'
                driver.get(url)
                
                if not self._wait_for_page_load(driver, self.config['timeout']):
                    result_info['reason'] = '页面加载超时'
                    if attempt < max_retries - 1:
                        time.sleep(2)
                        continue
                    break
                
                # 1. 首先判断是否为建筑物
                place_type = get_building_type_robust(driver)
                result_info['place_type'] = place_type or '未知'
                
                if not is_building(place_type):
                    result_info['status'] = 'skipped'
                    result_info['reason'] = f'非建筑物({place_type})'
                    break
                
                # 获取建筑物名称
                place_name = safe_get_building_name(driver)
                result_info['place_name'] = place_name
                
                # 2. 检查是否有展开按钮
                try:
                    more_button = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CLASS_NAME, 'M77dve'))
                    )
                    result_info['has_expand_button'] = True
                    click_on_more_button(driver)
                    
                    # 获取展开前的POI数量用于验证
                    initial_poi_count = get_poi_count_enhanced(driver)
                    
                    # 执行强化滚动
                    scroll_poi_section(driver)
                    
                    # 快速验证滚动效果
                    time.sleep(1)  # 减少等待时间
                    df_first = safe_get_all_poi_info(driver)
                    
                    # 如果初始显示很多POI但实际提取很少，快速再次尝试
                    if initial_poi_count > 50 and len(df_first) < initial_poi_count * 0.4:  # 提高阈值
                        # 快速再次滚动
                        scroll_poi_section(driver)
                        time.sleep(1.5)  # 减少等待
                        
                except TimeoutException:
                    result_info['has_expand_button'] = False
                
                # 3. 提取POI数据
                df = safe_get_all_poi_info(driver)
                if df.empty:
                    result_info['reason'] = '未找到POI'
                    break
                
                # 获取坐标
                lat, lng = safe_get_coords(driver.current_url)
                
                # 添加额外信息
                df['blt_name'] = place_name
                df['place_type'] = place_type
                df['lat'] = lat
                df['lng'] = lng
                df['crawl_time'] = pd.Timestamp.now()
                df['source_address'] = address
                
                result_info['poi_count'] = len(df)
                result_info['status'] = 'success'
                
                # 输出处理结果
                self._print_address_result(result_info)
                return df
                
            except Exception as e:
                result_info['reason'] = str(e)[:50]
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    break
        
        # 输出处理结果
        self._print_address_result(result_info)
        return None
    
    def _print_address_result(self, result_info):
        """输出单个地址的处理结果"""
        address = result_info['address']
        status = result_info['status']
        
        if status == 'success':
            expand_status = "展开后提取" if result_info['has_expand_button'] else "直接提取"
            # 简化地址显示（只显示前30个字符）
            short_address = address[:30] + "..." if len(address) > 30 else address
            print(f"✅ [{short_address}] 建筑物({result_info['place_type']}) - {expand_status} - POI数量: {result_info['poi_count']}")
        elif status == 'skipped':
            short_address = address[:30] + "..." if len(address) > 30 else address
            print(f"⏭️ [{short_address}] 跳过 - {result_info['reason']}")
        else:
            short_address = address[:30] + "..." if len(address) > 30 else address
            attempt_info = f" (尝试{result_info['attempt_count']}次)" if result_info['attempt_count'] > 1 else ""
            print(f"❌ [{short_address}] 失败{attempt_info} - {result_info['reason']}")
    
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

def show_startup_menu():
    """显示启动菜单"""
    print("🎯 POI爬虫启动器")
    print("=" * 40)
    print("📋 可用模式:")
    print("1. 🚀 自动爬取 (自动选择最大CSV文件)")
    print("2. 🧪 测试模式 (前5个地址)")
    print("3. 🖥️ 显示Chrome窗口模式")
    print("4. 📄 帮助信息")
    print("5. 🔧 高级参数模式")
    
    print("\n💡 快速使用:")
    print("  python final_crawler.py        # 显示此菜单")
    print("  python final_crawler.py --test # 直接测试模式")
    print("  python final_crawler.py --help # 查看所有参数")

def show_help_info():
    """显示详细帮助信息"""
    print("\n📄 详细使用说明:")
    print("=" * 50)
    print("🔥 自动文件选择:")
    print("  程序会自动扫描 data/input/ 目录")
    print("  选择行数最多的有效CSV文件")
    print("  生成带时间戳的输出文件")
    print()
    print("📋 命令行参数:")
    print("  --test          测试模式(前5个地址)")
    print("  --no-headless   显示Chrome窗口")
    print("  --workers N     设置并发线程数")
    print("  --input FILE    指定输入文件")
    print("  --output FILE   指定输出文件")
    print()
    print("📂 文件要求:")
    print("  - CSV格式")
    print("  - 必须包含 'Address' 列")
    print("  - 放在 data/input/ 目录下")
    print()
    print("🎯 示例命令:")
    print("  python final_crawler.py                    # 显示菜单")
    print("  python final_crawler.py --test             # 测试前5个地址")
    print("  python final_crawler.py --workers 2        # 使用2个线程")
    print("  python final_crawler.py --no-headless      # 显示Chrome")

def main():
    """主函数 - 统一启动入口"""
    # 添加命令行参数支持
    import argparse
    parser = argparse.ArgumentParser(description='POI爬虫工具', add_help=False)
    parser.add_argument('mode', nargs='?', help='启动模式 (1-5)')
    parser.add_argument('--input', '-i', help='输入CSV文件路径')
    parser.add_argument('--output', '-o', help='输出CSV文件路径') 
    parser.add_argument('--workers', '-w', type=int, default=4, help='并发线程数 (默认: 4)')
    parser.add_argument('--headless', action='store_true', help='无头模式运行')
    parser.add_argument('--no-headless', action='store_true', help='显示Chrome窗口')
    parser.add_argument('--test', action='store_true', help='测试模式 (处理前5个地址)')
    parser.add_argument('--help', '-h', action='store_true', help='显示帮助信息')
    args = parser.parse_args()
    
    # 处理帮助信息
    if args.help:
        parser.print_help()
        show_help_info()
        return
    
    # 处理直接命令行参数
    if args.test or args.input or args.no_headless:
        run_crawler_direct(args)
        return
    
    # 处理菜单模式
    if args.mode:
        run_menu_mode(args.mode)
        return
    
    # 显示启动菜单
    show_startup_menu()
    try:
        choice = input("\n请选择模式 (1-5): ").strip()
        if choice:
            run_menu_mode(choice)
    except (KeyboardInterrupt, EOFError):
        print("\n❌ 已取消")

def run_menu_mode(mode):
    """根据菜单选择运行"""
    if mode == '1':
        # 自动爬取模式
        print("\n🚀 启动自动爬取模式...")
        run_crawler_with_config(test_mode=False, headless=True)
        
    elif mode == '2':
        # 测试模式
        print("\n🧪 启动测试模式...")
        run_crawler_with_config(test_mode=True, headless=True)
        
    elif mode == '3':
        # 显示Chrome窗口模式
        print("\n🖥️ 启动显示Chrome窗口模式...")
        run_crawler_with_config(test_mode=False, headless=False)
        
    elif mode == '4':
        # 帮助信息
        show_help_info()
        
    elif mode == '5':
        # 高级参数模式
        print("\n🔧 高级参数模式:")
        print("使用以下命令行参数:")
        print("  python final_crawler.py --input your_file.csv --workers 2")
        print("  python final_crawler.py --test --no-headless")
        
    else:
        print("❌ 无效选择，请使用 1-5")

def run_crawler_direct(args):
    """直接运行爬虫（命令行参数模式）"""
    print("🎯 增强版POI爬虫")
    print("=" * 60)
    
    # 简单文件选择逻辑
    if args.input and args.output:
        input_file = args.input
        output_file = args.output
        print(f"📄 使用指定文件:")
        print(f"  📥 输入: {input_file}")
        print(f"  📤 输出: {output_file}")
    else:
        print("📂 自动选择文件...")
        file_config = get_simple_file_config(suffix="test" if args.test else "poi_enhanced")
        
        if not file_config['has_input']:
            print("❌ 未找到有效的输入文件")
            print("💡 请将CSV文件放入 data/input/ 目录")
            return
        
        input_file = file_config['input_file']
        output_file = file_config['output_file']
    
    # 确定是否使用无头模式
    headless = True  # 默认无头模式
    if args.no_headless:
        headless = False
    elif args.headless:
        headless = True
    
    # 运行爬虫
    run_crawler_core(input_file, output_file, args.workers, headless, args.test)

def run_crawler_with_config(test_mode=False, headless=True, workers=4):
    """使用预设配置运行爬虫"""
    print("📂 自动选择文件...")
    file_config = get_simple_file_config(suffix="test" if test_mode else "poi_enhanced")
    
    if not file_config['has_input']:
        print("❌ 未找到有效的输入文件")
        print("💡 请将CSV文件放入 data/input/ 目录")
        return
    
    # 测试模式清理检查点
    if test_mode:
        checkpoint_file = Path('checkpoint.json')
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            print("🧹 已清理检查点文件")
    
    run_crawler_core(
        file_config['input_file'], 
        file_config['output_file'], 
        workers, 
        headless, 
        test_mode
    )

def run_crawler_core(input_file, output_file, workers, headless, test_mode):
    """爬虫核心运行逻辑"""
    config = {
        'max_workers': workers,
        'driver_pool_size': workers,
        'batch_size': 15,
        'timeout': 12,
        'retry_times': 2,
        'headless': headless,
        'checkpoint_interval': 30,
        'input_file': input_file,
        'output_file': output_file
    }
    
    print(f"\n⚙️ 运行配置:")
    print(f"  📥 输入文件: {config['input_file']}")
    print(f"  📤 输出文件: {config['output_file']}")
    print(f"  🔧 并发线程: {config['max_workers']}")
    print(f"  {'🔥' if config['headless'] else '🖥️'} 运行模式: {'无头模式 (后台)' if config['headless'] else '显示Chrome窗口'}")
    
    try:
        # 验证输入文件
        df_input = pd.read_csv(config['input_file'])
        print(f"\n📊 输入文件统计:")
        print(f"  数据行数: {len(df_input):,}")
        print(f"  列名: {list(df_input.columns)}")
        
        # 获取地址列表
        if 'Address' in df_input.columns:
            addresses = df_input['Address'].dropna().tolist()
        else:
            print("⚠️ 未找到'Address'列，尝试使用第一列")
            addresses = df_input.iloc[:, 0].dropna().tolist()
        
        # 测试模式处理
        if test_mode:
            test_count = min(5, len(addresses))
            addresses = addresses[:test_count]
            print(f"🧪 测试模式: 只处理前 {test_count} 个地址")
        
        print(f"  处理地址数: {len(addresses):,} 个")
        
        if len(addresses) == 0:
            print("❌ 没有有效地址数据")
            return
        
        # 预估时间
        estimated_time = len(addresses) * 2.5 / config['max_workers'] / 60
        print(f"  ⏱️ 预计耗时: {estimated_time:.1f} 分钟")
        
        # 确认执行 (测试模式跳过确认)
        if len(addresses) > 100 and not test_mode:
            print(f"\n⚠️ 将要处理 {len(addresses):,} 个地址，这可能需要较长时间")
            try:
                confirm = input("确认继续？(y/n): ").lower()
                if confirm != 'y':
                    print("❌ 已取消执行")
                    return
            except EOFError:
                print("🤖 非交互环境，自动继续执行")
        
        print(f"\n🚀 开始爬取...")
        start_time = time.time()
        
        # 创建爬虫并运行
        crawler = FinalPOICrawler(config)
        try:
            crawler.process_addresses(addresses)
            
            elapsed_time = time.time() - start_time
            print(f"\n🎉 爬取完成！")
            print(f"⏱️ 总耗时: {elapsed_time/60:.1f} 分钟")
            print(f"📈 平均速度: {elapsed_time/len(addresses):.1f} 秒/地址")
            print(f"📁 结果文件: {config['output_file']}")
            
        except KeyboardInterrupt:
            print("\n⏹️ 用户中断爬取")
        except Exception as e:
            print(f"\n❌ 爬取过程中发生错误: {e}")
        finally:
            crawler.close()
            
    except FileNotFoundError:
        print(f"❌ 输入文件不存在: {config['input_file']}")
    except Exception as e:
        print(f"❌ 运行失败: {e}")

if __name__ == "__main__":
    main()