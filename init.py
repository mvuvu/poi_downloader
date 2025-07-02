#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
POI爬虫项目初始化脚本
运行此脚本完成项目环境初始化
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def print_step(step, description):
    """打印步骤信息"""
    print(f"\n{'='*60}")
    print(f"🚀 步骤 {step}: {description}")
    print('='*60)

def check_python_version():
    """检查Python版本"""
    version = sys.version_info
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print("❌ 需要Python 3.8或更高版本")
        print(f"   当前版本: {version.major}.{version.minor}.{version.micro}")
        return False
    
    print(f"✅ Python版本检查通过: {version.major}.{version.minor}.{version.micro}")
    return True

def install_requirements():
    """安装依赖包"""
    requirements_file = Path('requirements.txt')
    if not requirements_file.exists():
        print("❌ requirements.txt 文件不存在")
        return False
    
    try:
        print("📦 正在安装依赖包...")
        result = subprocess.run([sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ 依赖包安装完成")
            return True
        else:
            print(f"❌ 依赖包安装失败: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ 安装过程出错: {e}")
        return False

def create_directories():
    """创建必要的目录"""
    directories = [
        'data/input',
        'data/output', 
        'logs',
        'config',
        'temp'
    ]
    
    for directory in directories:
        path = Path(directory)
        path.mkdir(parents=True, exist_ok=True)
        print(f"📁 创建目录: {directory}")
    
    print("✅ 目录结构创建完成")

def create_config_files():
    """创建配置文件"""
    config = {
        "crawler": {
            "max_workers": 2,
            "driver_pool_size": 2,
            "batch_size": 20,
            "timeout": 15,
            "retry_times": 3,
            "headless": False,
            "checkpoint_interval": 50
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
    
    config_file = Path('config/default.json')
    with open(config_file, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    
    print("✅ 默认配置文件已创建: config/default.json")

def create_example_files():
    """创建示例文件"""
    # 创建示例输入文件
    example_data = """District,Latitude,Longitude,Address
千代田区,35.6862245,139.7347045,東京都千代田区鍛冶町1丁目7-1
千代田区,35.6903667,139.7712745,東京都千代田区二番町10-46
千代田区,35.6951129,139.7623342,東京都千代田区神田小川町3丁目6-2"""
    
    example_file = Path('data/input/example_addresses.csv')
    with open(example_file, 'w', encoding='utf-8') as f:
        f.write(example_data)
    
    print("✅ 示例输入文件已创建: data/input/example_addresses.csv")

def check_chrome_driver():
    """检查Chrome和WebDriver"""
    try:
        from selenium import webdriver
        from webdriver_manager.chrome import ChromeDriverManager
        
        print("🌐 正在检查Chrome WebDriver...")
        
        # 尝试创建WebDriver实例
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        
        driver = webdriver.Chrome(options=options)
        driver.quit()
        
        print("✅ Chrome WebDriver 检查通过")
        return True
        
    except Exception as e:
        print(f"❌ Chrome WebDriver 检查失败: {e}")
        print("💡 请确保已安装Chrome浏览器")
        return False

def create_gitignore():
    """创建.gitignore文件"""
    gitignore_content = """# Python
__pycache__/
*.py[cod]
*$py.class
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# PyInstaller
*.manifest
*.spec

# Unit test / coverage reports
htmlcov/
.tox/
.coverage
.coverage.*
.cache
.pytest_cache/

# Jupyter Notebook
.ipynb_checkpoints

# pyenv
.python-version

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# 项目特定
logs/
temp/
data/output/*.csv
checkpoint.json
*.log

# 系统文件
.DS_Store
Thumbs.db

# WebDriver
*.exe
chromedriver*
geckodriver*
"""
    
    gitignore_file = Path('.gitignore')
    if not gitignore_file.exists():
        with open(gitignore_file, 'w', encoding='utf-8') as f:
            f.write(gitignore_content)
        print("✅ .gitignore 文件已创建")
    else:
        print("ℹ️ .gitignore 文件已存在，跳过创建")

def main():
    """主初始化流程"""
    print("🎯 POI爬虫项目初始化")
    print("=" * 60)
    
    # 步骤1: 检查Python版本
    print_step(1, "检查Python环境")
    if not check_python_version():
        print("❌ 初始化失败")
        return
    
    # 步骤2: 安装依赖
    print_step(2, "安装依赖包")
    if not install_requirements():
        print("⚠️ 依赖安装失败，请手动安装")
    
    # 步骤3: 创建目录结构
    print_step(3, "创建项目目录")
    create_directories()
    
    # 步骤4: 创建配置文件
    print_step(4, "创建配置文件")
    create_config_files()
    
    # 步骤5: 创建示例文件
    print_step(5, "创建示例文件")
    create_example_files()
    
    # 步骤6: 检查WebDriver
    print_step(6, "检查WebDriver环境")
    webdriver_ok = check_chrome_driver()
    
    # 步骤7: 创建.gitignore
    print_step(7, "创建.gitignore")
    create_gitignore()
    
    # 完成报告
    print("\n" + "🎉" * 20)
    print("🎉 项目初始化完成！")
    print("🎉" * 20)
    
    print(f"\n📊 初始化结果:")
    print(f"  ✅ Python环境: 正常")
    print(f"  ✅ 项目目录: 已创建")
    print(f"  ✅ 配置文件: 已创建")
    print(f"  ✅ 示例文件: 已创建")
    print(f"  {'✅' if webdriver_ok else '⚠️'} WebDriver: {'正常' if webdriver_ok else '需要检查'}")
    
    print(f"\n📋 下一步操作:")
    print(f"  1. 将输入数据放入 data/input/ 目录")
    print(f"  2. 根据需要修改 config/default.json 配置")
    print(f"  3. 运行 python final_crawler.py 开始爬取")
    
    if not webdriver_ok:
        print(f"\n⚠️ WebDriver警告:")
        print(f"  请确保已安装Chrome浏览器")
        print(f"  如果问题持续，请手动安装chromedriver")

if __name__ == "__main__":
    main()