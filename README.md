# POI Crawler

从Google Maps高效爬取POI数据的轻量化工具，支持断点续传、智能重试和实时进度显示。

## 特性

### 🚀 核心功能
- **多线程并行爬取** - 10个持久化Chrome工作线程，高效处理大量数据
- **断点续传** - 支持中断后继续爬取，避免重复工作
- **智能重试** - 无效地址自动使用日文地址重试
- **实时进度条** - 显示处理速度、成功率和预计完成时间
- **酒店页面过滤** - 自动识别并跳过酒店分类页面

### 📊 数据质量保障
- **自动去重** - 批次级数据去重机制
- **错误监控** - 完善的错误日志和警告系统
- **坐标提取** - 从Google Maps URL自动提取经纬度坐标
- **地址标准化** - 支持日文地址转换为英文格式

### 🛠 操作便利性
- **多种文件选择方式** - 支持单文件、多文件、通配符、文件列表
- **灵活参数配置** - 可调节工作线程数、批次大小等参数
- **详细日志输出** - 可选的详细日志模式，便于调试
- **状态监控** - 实时查看爬取进度和统计信息

## 系统要求

- Python 3.8+
- Chrome浏览器
- 稳定的网络连接
- 建议8GB+内存

## 安装

```bash
# 克隆项目
git clone <repository-url>
cd poi_crawler

# 安装依赖
pip install -r requirements.txt
```

## 快速开始

```bash
# 1. 准备输入数据（CSV文件，包含坐标和地址信息）
# 2. 放置文件到 data/input/ 目录

# 3. 开始爬取
python poi_crawler_simple.py --all
```

## 使用说明

### 基本命令

```bash
# 处理所有输入文件
python poi_crawler_simple.py --all

# 处理单个文件
python poi_crawler_simple.py data/input/千代田区_complete.csv

# 处理多个指定文件
python poi_crawler_simple.py file1.csv file2.csv file3.csv

# 使用通配符选择文件
python poi_crawler_simple.py --pattern "data/input/*区_complete*.csv"

# 从文件列表读取
python poi_crawler_simple.py --file-list files_to_process.txt

# 查看爬取状态
python poi_crawler_simple.py --status
```

### 高级选项

```bash
# 自定义工作线程数（默认：10）
python poi_crawler_simple.py --all --workers 8

# 自定义批次大小（默认：50）
python poi_crawler_simple.py --all --batch-size 25

# 启用详细日志
python poi_crawler_simple.py --all --verbose

# 禁用断点续传
python poi_crawler_simple.py --all --no-resume

# 禁用进度条（适合脚本环境）
python poi_crawler_simple.py --all --no-progress

# 组合使用
python poi_crawler_simple.py --pattern "*.csv" --workers 8 --verbose --no-progress
```

### 地址转换

```bash
# 转换单个文件（日文地址→英文地址）
python address_converter.py data/output/area_file.csv

# 批量转换所有文件
python address_converter.py --all

# 强制重新转换
python address_converter.py --regenerate
```

## 数据格式

### 输入CSV格式要求

| 列名 | 说明 | 示例 |
|------|------|------|
| District | 区域名称 | 千代田区 |
| Latitude | 纬度坐标 | 35.6895 |
| Longitude | 经度坐标 | 139.6917 |
| Address | 日文地址 | 東京都千代田区神田駿河台3丁目1-1 |

### POI输出数据字段

| 字段 | 说明 | 示例 |
|------|------|------|
| name | POI名称 | スターバックス |
| rating | 评分 | 4.2 |
| class | 分类 | コーヒーショップ |
| add | 地址 | 東京都千代田区... |
| comment_count | 评论数 | 150 |
| blt_name | 建筑物名称 | 神田ビル |
| lat | 纬度 | 35.6895 |
| lng | 经度 | 139.6917 |

### 地址转换示例

- **输入**：東京都千代田区神田駿河台3丁目1-1
- **输出**：〒101-0062,+Tokyo,+Chiyoda+City,+Kandasurugadai,+3-chōme−1-1

## 项目结构

```
poi_crawler/
├── poi_crawler_simple.py          # 主程序 - 轻量化POI爬虫
├── address_converter.py           # 地址转换工具
├── info_tool.py                  # POI信息提取模块
├── driver_action.py              # 浏览器自动化操作
├── requirements.txt              # Python依赖列表
├── CLAUDE.md                    # 开发指南
├── README.md                    # 项目说明
└── data/                        # 数据目录
    ├── input/                  # 爬取输入文件目录
    ├── output/                 # POI输出结果目录
    ├── progress/               # 进度跟踪文件目录
    ├── warnings/               # 警告日志目录
    └── archive/                # 映射数据存档
        ├── tokyo_complete_mapping.json  # 地址映射数据
        └── x-ken-all.csv              # 邮编数据
```

## 高级功能

### 断点续传机制

- 程序会自动在 `data/progress/` 目录保存进度
- 重启后自动从上次中断位置继续
- 删除进度文件可重新开始爬取

```bash
# 删除特定文件的进度（重新开始）
rm data/progress/千代田区_complete_simple_progress.json

# 清空所有进度文件
rm data/progress/*.json
```

### 进度条功能

进度条显示以下信息：
- 当前进度百分比
- 处理速度（条/秒）
- 成功/失败统计
- 预计剩余时间

```
🔍 世田谷区_optimi:  45%|████████    | 4500/10000 95%成功 22.1/s
```

### 智能重试机制

当检测到以下情况时，系统会自动使用日文地址重试：
- 页面无有效H1标题（无效地址页面）
- 当前使用的是英文地址
- 存在原始日文地址

重试优化特性：
- **早期检测**：在页面加载后立即检测是否为有效建筑物页面
- **优先处理**：重试任务使用专门的高优先级队列
- **避免重复**：缓存机制防止同一地址重复重试
- **快速跳过**：无效页面1-3秒内完成检测，避免耗时操作

### 性能优化建议

- **工作线程数**：建议设置为CPU核心数，默认10个线程适合大多数情况
- **批次大小**：默认50条记录/批次，可根据内存情况调整
- **内存监控**：大量数据处理时注意内存使用情况
- **任务分布**：简单任务先完成，后期遇到复杂任务时性能会下降
- **长时间运行**：建议分批处理大文件，每2-3小时重启避免性能衰退

## 故障排除

### 常见问题

1. **Chrome驱动问题**
   ```bash
   # 手动更新Chrome驱动
   pip install --upgrade webdriver-manager
   ```

2. **内存不足**
   ```bash
   # 减少工作线程数
   python poi_crawler_simple.py --all --workers 5
   
   # 减少批次大小
   python poi_crawler_simple.py --all --batch-size 25
   ```

3. **网络超时**
   - 检查网络连接稳定性
   - 考虑使用VPN（如访问受限）

4. **进度条在脚本中显示异常**
   ```bash
   # 禁用进度条
   python poi_crawler_simple.py --all --no-progress
   ```

5. **数据格式错误**
   - 确保输入CSV包含必需列
   - 检查坐标格式是否为小数

### 调试模式

```bash
# 启用详细日志进行调试
python poi_crawler_simple.py data/input/测试文件.csv --verbose --workers 1

# 禁用断点续传进行全新测试
python poi_crawler_simple.py data/input/测试文件.csv --no-resume

# 组合调试选项
python poi_crawler_simple.py data/input/测试文件.csv --verbose --no-resume --workers 1
```

### 日志查看

程序运行日志会显示错误信息，注意查看控制台输出。主要日志类型：
- 🔍 处理信息
- ✅ 成功消息
- ❌ 错误警告
- 📊 统计数据
- 🔄 重试信息

## 注意事项

- 地址转换会直接覆盖原文件，请提前备份重要数据
- 爬取大量数据时请确保网络稳定
- 遵守Google Maps服务条款，合理控制请求频率
- 建议在非高峰时段运行大批量任务

## 开发指南

参考 `CLAUDE.md` 文件了解代码架构和开发规范。

## 技术架构

- **单进程多线程**：10个持久化Chrome工作线程
- **双队列系统**：主任务队列 + 高优先级重试队列
- **结果缓存**：批量写入机制，提高I/O效率
- **进度管理**：JSON格式的断点续传支持
- **Chrome优化**：无头模式运行，资源使用优化
- **智能检测**：基于H1标题的页面类型快速识别

---

**版本**: v6.1 (2025-07)  
**特性**: 轻量化架构 | 断点续传 | 实时进度条 | 优化重试机制 | 无效地址早期检测