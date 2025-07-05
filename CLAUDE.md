# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

POI Crawler is a high-performance web scraping system that extracts Point of Interest (POI) data from Google Maps using Selenium. The system comes in two versions and consists of multiple components:

### Versions
1. **Standard Version** (`parallel_poi_crawler.py`) - Multi-process architecture, stable and reliable
2. **Turbo Version** (`parallel_poi_crawler_turbo.py`) - High-performance multi-threaded architecture with optimized resource management

### Core Components
1. **POI Data Crawler** - Scrapes POI information (names, ratings, classifications, addresses, etc.) from Google Maps
2. **Address Converter** - Converts Japanese addresses to standardized English format with postal codes

## Architecture

### File Structure

- `parallel_poi_crawler.py` - Standard version: Multi-process crawling with stable performance
- `parallel_poi_crawler_turbo.py` - Turbo version: High-performance multi-threaded architecture
- `info_tool.py` - Data extraction functions for POI information from web pages  
- `driver_action.py` - Browser automation functions (scrolling, clicking, navigation)
- `address_converter.py` - Address translation system using mapping data

### Data Flow

1. Input CSV files contain coordinates and Japanese addresses
2. Crawler opens Google Maps at each coordinate location
3. Selenium extracts POI data using XPath selectors and CSS classes
4. Results saved to output CSV with progress tracking
5. Address converter optionally translates addresses to English format

### Key Design Patterns

#### Standard Version
- **Process Pool Pattern**: Uses ProcessPoolExecutor for parallel crawling across multiple browser instances
- **Resume/Recovery**: Progress tracking in JSON files enables resuming interrupted crawls
- **Driver Pool**: Chrome driver instances cached and reused within processes
- **Batch Processing**: Data written in configurable batch sizes for memory efficiency

#### Turbo Version
- **High-Concurrency Threading**: Scientifically configured thread pools (CPU cores × 4 for 12+ core systems)
- **Chrome Driver Pool Management**: Intelligent reuse of Chrome instances (up to 20 instances)
- **FIFO Task Queue**: Fair task processing without starvation
- **Thread-Local Statistics**: Lock-free performance monitoring
- **Intelligent Retry System**: Three-layer retry mechanism (primary → secondary → fallback)

## Common Commands

### POI Crawling
```bash
# Install dependencies
pip install -r requirements.txt

# Standard Version - Process all input files
python parallel_poi_crawler.py --all

# Turbo Version - High performance processing
python parallel_poi_crawler_turbo.py --all

# Process single file
python parallel_poi_crawler.py data/input/千代田区_complete.csv
python parallel_poi_crawler_turbo.py data/input/千代田区_complete.csv

# Process multiple files
python parallel_poi_crawler.py file1.csv file2.csv file3.csv
python parallel_poi_crawler_turbo.py file1.csv file2.csv file3.csv

# Use wildcard pattern
python parallel_poi_crawler.py --pattern "data/input/*区_complete*.csv"

# Use file list
python parallel_poi_crawler.py --file-list files_to_process.txt

# Check crawling status
python parallel_poi_crawler.py --status
```

### Address Conversion
```bash
# Convert single file (overwrites original)
python address_converter.py data/oring_add/area_file.csv

# Convert all files in oring_add directory
python address_converter.py --all

# Force regenerate all converted addresses
python address_converter.py --regenerate
```

## Data Formats

### Input CSV Requirements
- **District**: Area name
- **Latitude**: Decimal latitude coordinate  
- **Longitude**: Decimal longitude coordinate
- **Address**: Japanese address text

### POI Output Schema
- **name**: POI business name
- **rating**: Google Maps rating (1-5 scale)
- **class**: POI category/type
- **add**: Address text
- **comment_count**: Number of reviews
- **blt_name**: Building name if applicable
- **lat/lng**: Extracted coordinates

## File Structure Context

- `data/input/` - Source coordinate/address files for crawling
- `data/output/` - Generated POI data CSV files
- `data/progress/` - JSON progress tracking files for resume capability
- `data/warnings/` - Error and warning log files
- `data/no_poi_warnings/` - No POI detection warnings
- `data/non_building_warnings/` - Non-building detection warnings
- `data/archive/tokyo_complete_mapping.json` - Address mapping data for English conversion

## Important Implementation Notes

### Standard Version
- Multi-process architecture with CPU count - 1 workers by default
- Chrome runs in headless mode with stable configuration
- ProcessPoolExecutor manages browser instances
- Batch size: 50 records per batch

### Turbo Version  
- High-performance multi-threaded architecture
- Scientific concurrency: 12+ core systems can use 48 threads
- Chrome driver pool: 20 instances with intelligent management
- Enhanced retry mechanism with three-layer fallback
- Thread-local statistics for lock-free performance
- FIFO task queue prevents task starvation
- Batch size: 25 records per batch for balanced performance

### Common Notes
- XPath selectors are specific to Google Maps DOM structure and may need updates
- Address conversion assumes Tokyo area mapping data
- Progress files enable resuming but should be deleted for fresh crawls
- Address conversion directly overwrites input files (no backup created)

## Recent Updates

### Multi-file Selection (2025-07)
- Added support for processing multiple files in a single command
- Implemented wildcard pattern matching for file selection
- Added file list support for batch processing from text files
- All file selection methods support existing options (--workers, --batch-size, etc.)
- Files are automatically deduplicated if selected multiple times

### Address Format Fixes (2025-07)
- Fixed English spelling errors in converted addresses: 'jyu'→'ju', 'shya'→'sha', 'jya'→'ja'
- Updated Kanda area addresses to use separated format: '+Kanda+Surugadai' instead of '+Kandasurugadai'

### Turbo Version Optimization (2025-07)
- Added high-performance Turbo version with multi-threaded architecture
- Implemented scientific concurrency configuration based on CPU cores
- Added Chrome driver pool management with intelligent reuse
- Introduced FIFO task queue to prevent task starvation
- Implemented thread-local statistics for lock-free performance monitoring
- Enhanced retry mechanism with three-layer fallback system
- Comprehensive warning and error logging system

## Version Selection Guide

### When to Use Standard Version (parallel_poi_crawler.py)
- **Production environments** requiring stability and reliability
- **Systems with limited resources** (< 8 CPU cores, < 8GB RAM)
- **Long-running batch jobs** where stability is more important than speed
- **First-time usage** or testing the system

### When to Use Turbo Version (parallel_poi_crawler_turbo.py)  
- **High-performance systems** with 8+ CPU cores and 16+ GB RAM
- **Large-scale data processing** with thousands of addresses
- **Development environments** where maximum speed is desired
- **Systems where you can monitor resource usage** and adjust parameters

### Performance Comparison
| Metric | Standard Version | Turbo Version |
|--------|------------------|---------------|
| Architecture | Multi-process | Multi-threaded |
| Default Workers | CPU cores - 1 | CPU cores × 4 |
| Chrome Instances | 4-8 | 20 |
| Memory Usage | Low | Higher |
| CPU Usage | Moderate | High |
| Stability | Very High | High |
| Speed | Good | Excellent |