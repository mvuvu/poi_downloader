# POI Crawler

从Google Maps并行爬取POI数据，支持断点续传和智能地址转换。

## 特性

- 多进程并行爬取
- 断点续传
- 智能地址转换（日文→英文）
- 静默运行

## 安装

```bash
pip install -r requirements.txt
```

## 使用

### POI数据爬取

```bash
# 处理所有区域
python parallel_poi_crawler.py --all

# 处理单个文件
python parallel_poi_crawler.py data/input/千代田区_complete.csv

# 查看进度
python parallel_poi_crawler.py --status
```

### 地址转换

```bash
# 转换单个文件
python address_converter.py data/oring_add/千代田区.csv

# 批量转换所有文件（直接覆盖原文件）
python address_converter.py --all

# 强制重新转换所有文件
python address_converter.py --regenerate
```

## 数据格式

### 输入CSV格式
需包含：District, Latitude, Longitude, Address

### POI输出格式
包含：name, rating, class, add, comment_count, blt_name, lat, lng

### 地址转换
- 输入：日文地址（如：東京都千代田区神田駿河台3丁目1-1）
- 输出：英文格式（如：〒101-0062,+Tokyo,+Chiyoda+City,+Kandasurugadai,+3-chōme−1-1）
- 自动添加ConvertedAddress列到原文件

## 项目结构

```
poi_crawler/
├── parallel_poi_crawler.py  # 主程序
├── address_converter.py     # 地址转换工具
├── info_tool.py            # 数据提取
├── driver_action.py        # 浏览器操作
├── requirements.txt        # 依赖
└── data/
    ├── input/             # 爬取输入文件
    ├── oring_add/         # 地址转换输入文件
    ├── output/            # 输出文件
    ├── progress/          # 进度文件
    └── archive/           # 映射数据
        └── tokyo_complete_mapping.json
```

## 地址转换说明

地址转换工具基于预设的映射数据，能够将日文地址转换为标准化的英文格式：

- 支持有丁目和无丁目的地址格式
- 自动查找对应的邮编
- 统一使用英文区名和地名
- 直接覆盖原文件，无备份