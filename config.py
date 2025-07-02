#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POI爬虫配置管理模块
"""

import json
import os
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class CrawlerConfig:
    """爬虫配置数据类"""
    # 核心爬取参数
    max_workers: int = 2
    driver_pool_size: int = 2
    batch_size: int = 20
    timeout: int = 15
    retry_times: int = 3
    headless: bool = False
    checkpoint_interval: int = 50
    
    # 输入输出
    input_file: str = ""
    output_file: str = ""
    
    # 高级选项
    enable_images: bool = False
    enable_javascript: bool = True
    user_agent: str = ""
    proxy: str = ""
    
    # 调试选项
    debug_mode: bool = False
    save_screenshots: bool = False
    verbose_logging: bool = False

class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        
    def load_config(self, config_name: str = "default") -> CrawlerConfig:
        """加载配置文件"""
        config_file = self.config_dir / f"{config_name}.json"
        
        if not config_file.exists():
            # 如果配置文件不存在，创建默认配置
            default_config = self.get_default_config()
            self.save_config(default_config, config_name)
            return default_config
        
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            # 转换为CrawlerConfig对象
            return CrawlerConfig(**config_data.get('crawler', {}))
            
        except Exception as e:
            print(f"❌ 配置文件加载失败: {e}")
            return self.get_default_config()
    
    def save_config(self, config: CrawlerConfig, config_name: str = "default"):
        """保存配置文件"""
        config_file = self.config_dir / f"{config_name}.json"
        
        config_data = {
            "crawler": {
                "max_workers": config.max_workers,
                "driver_pool_size": config.driver_pool_size,
                "batch_size": config.batch_size,
                "timeout": config.timeout,
                "retry_times": config.retry_times,
                "headless": config.headless,
                "checkpoint_interval": config.checkpoint_interval,
                "input_file": config.input_file,
                "output_file": config.output_file,
                "enable_images": config.enable_images,
                "enable_javascript": config.enable_javascript,
                "user_agent": config.user_agent,
                "proxy": config.proxy,
                "debug_mode": config.debug_mode,
                "save_screenshots": config.save_screenshots,
                "verbose_logging": config.verbose_logging
            },
            "output": {
                "format": "csv",
                "encoding": "utf-8",
                "include_timestamp": True
            },
            "logging": {
                "level": "INFO",
                "file": "logs/crawler.log",
                "max_size_mb": 10
            }
        }
        
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, ensure_ascii=False, indent=2)
            print(f"✅ 配置已保存: {config_file}")
        except Exception as e:
            print(f"❌ 配置保存失败: {e}")
    
    def get_default_config(self) -> CrawlerConfig:
        """获取默认配置"""
        return CrawlerConfig(
            max_workers=2,
            driver_pool_size=2,
            batch_size=20,
            timeout=15,
            retry_times=3,
            headless=False,
            checkpoint_interval=50,
            input_file="data/input/addresses.csv",
            output_file="data/output/poi_results.csv"
        )
    
    def get_fast_config(self) -> CrawlerConfig:
        """获取快速模式配置"""
        config = self.get_default_config()
        config.max_workers = 4
        config.driver_pool_size = 4
        config.batch_size = 10
        config.timeout = 10
        config.headless = True
        config.enable_images = False
        return config
    
    def get_stable_config(self) -> CrawlerConfig:
        """获取稳定模式配置"""
        config = self.get_default_config()
        config.max_workers = 1
        config.driver_pool_size = 1
        config.batch_size = 50
        config.timeout = 30
        config.retry_times = 5
        config.headless = False
        return config
    
    def get_debug_config(self) -> CrawlerConfig:
        """获取调试模式配置"""
        config = self.get_default_config()
        config.max_workers = 1
        config.driver_pool_size = 1
        config.debug_mode = True
        config.save_screenshots = True
        config.verbose_logging = True
        config.headless = False
        return config
    
    def list_configs(self) -> list:
        """列出所有配置文件"""
        if not self.config_dir.exists():
            return []
        
        configs = []
        for config_file in self.config_dir.glob("*.json"):
            configs.append(config_file.stem)
        
        return sorted(configs)
    
    def create_preset_configs(self):
        """创建预设配置文件"""
        presets = {
            "default": self.get_default_config(),
            "fast": self.get_fast_config(),
            "stable": self.get_stable_config(),
            "debug": self.get_debug_config()
        }
        
        for name, config in presets.items():
            self.save_config(config, name)
        
        print("✅ 预设配置文件已创建:")
        for name in presets.keys():
            print(f"  - {name}.json")

def main():
    """配置管理命令行工具"""
    import sys
    
    if len(sys.argv) < 2:
        print("使用方法:")
        print("  python config.py create-presets  # 创建预设配置")
        print("  python config.py list           # 列出配置文件")
        print("  python config.py show <name>    # 显示配置内容")
        return
    
    manager = ConfigManager()
    command = sys.argv[1]
    
    if command == "create-presets":
        manager.create_preset_configs()
    
    elif command == "list":
        configs = manager.list_configs()
        print("可用配置文件:")
        for config in configs:
            print(f"  - {config}")
    
    elif command == "show" and len(sys.argv) > 2:
        config_name = sys.argv[2]
        config = manager.load_config(config_name)
        print(f"配置 '{config_name}':")
        print(f"  并发数: {config.max_workers}")
        print(f"  WebDriver池: {config.driver_pool_size}")
        print(f"  批处理大小: {config.batch_size}")
        print(f"  超时时间: {config.timeout}秒")
        print(f"  重试次数: {config.retry_times}")
        print(f"  无头模式: {config.headless}")
    
    else:
        print("❌ 未知命令")

if __name__ == "__main__":
    main()