# 🗘️ POI Crawler

️ **本项目仅用于研究和学习目的，不包含任何违反目标网站服务条款的内容。请勿将其用于商业用途。**

高效爬取 POI 数据的轻量化工具，支持多线程、断点续传、智能重试等功能。

---

## ✅ 系统要求

* Python 3.8+
* Chrome 浏览器
* 稳定的网络连接
* 建议内存 8GB+

---

## 🚀 安装方式

```bash
# 克隆项目
git clone <repository-url>
cd poi_crawler

# 安装依赖
pip install -r requirements.txt
```

---

## ⚡ 快速开始

```bash
# 1. 准备输入数据文件
# 2. 放入 data/input/ 目录

# 3. 启动爬虫
python poi_crawler_simple.py --all
```

---

## 📘 使用说明

### 基本命令

```bash
# 处理所有输入文件
python poi_crawler_simple.py --all

# 处理单个文件
python poi_crawler_simple.py data/input/千代田区_complete.csv

# 通配符匹配
python poi_crawler_simple.py --pattern "data/input/*区_complete*.csv"

# 从文件列表读取
python poi_crawler_simple.py --file-list files_to_process.txt
```

### 高级选项

```bash
# 自定义工作线程数
python poi_crawler_simple.py --all --workers 8

# 自定义批次大小
python poi_crawler_simple.py --all --batch-size 25

# 详细日志
python poi_crawler_simple.py --all --verbose

# 禁用断点续传
python poi_crawler_simple.py --all --no-resume

# 禁用进度条
python poi_crawler_simple.py --all --no-progress

# 调试模式
python poi_crawler_simple.py data/input/test_sample.csv --workers 1 --verbose
```

---

## 🗭 地址处理机制

支持地址字段：

1. `FormattedAddress` 标准化地址
2. `Address` 日文地址
3. `ConvertedAddress` 英文地址

优先级：FormattedAddress → Address → ConvertedAddress

---

## 📄 数据格式

### 输入 CSV

| 列名               | 说明   | 示例                             | 必填 |
| ---------------- | ---- | ------------------------------ | -- |
| District         | 区域名称 | 千代田区                           | 否  |
| Latitude         | 纬度   | 35.6895                        | 否  |
| Longitude        | 经度   | 139.6917                       | 否  |
| Address          | 日文地址 | 東京都千代田区神田駿沿台3丁目1-1             | 是  |
| ConvertedAddress | 英文地址 | 3-chome-1-1+Kanda+Surugadai... | 否  |
| FormattedAddress | 标准地址 | 3-chōme-1-1+Kanda+Surugadai... | 否  |

### 输出 POI 数据

| 字段             | 说明     | 示例         |
| -------------- | ------ | ---------- |
| name           | POI 名称 | スターバックス    |
| rating         | 评分     | 4.2        |
| class          | 分类     | コーヒーショップ   |
| add            | 地址     | 東京都千代田区... |
| comment\_count | 评论数    | 150        |
| blt\_name      | 建筑名称   | 神田ビル       |
| lat            | 纬度     | 35.6895    |
| lng            | 经度     | 139.6917   |

---

## 📂 项目结构

```
poi_crawler/
├── poi_crawler_simple.py        # 主程序入口
├── info_tool.py                 # POI 信息提取模块
├── driver_action.py             # 浏览器操作模块
├── requirements.txt             # 依赖列表
├── CLAUDE.md                    # 开发指南
├── README.md                    # 项目说明
└── data/
    ├── input/                   # 输入文件
    ├── output/                  # 结果输出
    ├── progress/                # 进度保存
    ├── warnings/                # 警告信息
    └── archive/                 # 历史存档
```

---

## 🔄 智能重试

检测无效地址或网页失败时，自动切换为日文地址重试

---

## 🛠️ 故障排查

**Chrome 驱动问题**

```bash
pip install --upgrade webdriver-manager
```

**内存不足**

```bash
python poi_crawler_simple.py --all --workers 5 --batch-size 25
```

**网络不稳**

* 检查网络或使用 VPN

**数据格式错误**

* 检查列名是否符合要求

---

## 🔮 调试模式

```bash
python poi_crawler_simple.py data/input/test_sample.csv --verbose --workers 1 --no-resume
```

---

## 📊 日志类型

* 🔍 处理信息
* ✅ 成功结果
* ❌ 错误警告
* 📊 总结统计
* 🔄 重试日志

---

## 📋 License

**MIT License**
本项目只授权用于学术研究和非商业用途。

---

## 📌 声明 / Disclaimer

本项目用于研究网页结构、自动化爬虫技术及城市特征提取，未使用任何官方 API，也不启动辅证或代理机制。

请自行阅读并确认目标网站的服务条款是否允许爬取。

开发者不对因使用本项目而引起的合法纠纷、数据被禁或账号限制负责。

> 如你是地图数据研究人员、公共机构或数据开发者，请优先使用：
>
> * [Google Places API](https://developers.google.com/maps/documentation/places)
> * [OpenStreetMap](https://www.openstreetmap.org)
> * [国土地理院 GSI](https://www.gsi.go.jp/)

---

## 📚 推荐应用场景

* 🏩 城市空间研究 (Urban Research)
* 🧐 Web 自动化技术演示 (Automation Practice)
* 🧮 空间数据工程培训 (GeoData Engineering)

---

## 🥺 测试样例

模拟测试文件：

```
data/input/test_sample.csv
```

包含 8 条方便地址，可用于验证系统是否应答正确。

```bash
python poi_crawler_simple.py data/input/test_sample.csv --verbose
```

---

📀 **版本**: v6.1
🗓 **日期**: 2025-07
🧪 **特性**: 轻量化框架，多线程，智能重试，无效地址早期检测，支持断点续传
