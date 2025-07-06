# POI爬虫性能分析工具集

## 🎯 目的
识别POI爬虫长时间运行后CPU和内存利用率下降的原因

## 🛠️ 工具列表

### 1. queue_monitor.py - 任务队列监控
手动记录任务处理进度，分析队列状态

### 2. analyze_timing.py - 任务时间分析
分析每个任务的处理时间，找出慢任务

### 3. worker_monitor.py - Worker线程监控
实时监控Worker线程的CPU使用率和状态

### 4. retry_analyzer.py - 重试任务分析
统计重试任务的比例和特征

### 5. performance_test.py - 性能基准测试
运行标准测试用例，对比性能

## 📋 使用步骤

1. **准备测试数据**
   ```bash
   # 创建小批量测试文件（10条数据）
   head -n 11 data/input/你的文件.csv > data/input/test_small.csv
   
   # 创建中等批量测试文件（50条数据）
   head -n 51 data/input/你的文件.csv > data/input/test_medium.csv
   ```

2. **运行爬虫并记录输出**
   ```bash
   python poi_crawler_simple.py --all > crawler_output.log 2>&1
   ```

3. **同时运行监控工具**
   ```bash
   # 在另一个终端运行Worker监控
   python performance_analysis/worker_monitor.py
   ```

4. **分析结果**
   ```bash
   # 分析重试模式
   python performance_analysis/retry_analyzer.py crawler_output.log
   
   # 运行性能测试
   python performance_analysis/performance_test.py
   ```

## 🔍 重点关注

1. **CPU使用率下降时间点** - 记录何时开始出现低CPU
2. **重试任务比例变化** - 观察重试是否逐渐增多
3. **单任务处理时间** - 是否有任务特别慢
4. **Worker线程状态** - 是否大部分时间在等待

## 📊 预期发现

- 任务队列后期以重试任务为主
- Worker线程大部分时间在等待网页加载
- 某些特定地址导致处理时间过长
- Chrome进程资源占用逐渐增加
