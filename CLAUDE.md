# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a high-performance POI (Point of Interest) crawler system for Google Maps, designed to extract building information and associated businesses/POIs from Japanese addresses. The system uses multi-process parallel processing for optimal performance.

## Core Architecture

### Module Structure
- **`parallel_poi_crawler.py`** - Main parallel crawler with real-time data output
- **`info_tool.py`** - Core data extraction functions that parse Google Maps pages using Selenium and BeautifulSoup
- **`driver_action.py`** - Selenium interaction helpers for clicking buttons, scrolling, and expanding content

### Data Flow
```
CSV Input → District Name Extraction → Parallel Address Processing → 
Google Maps Navigation → Page Interaction → POI Data Extraction → 
Real-time CSV Append (District Named File)
```

### Input/Output Structure
- **Input**: CSV files in `data/input/` with columns: `District,Latitude,Longitude,Address`
- **Output**: Single CSV file per district: `data/output/[区名]_poi_data_[timestamp].csv`
- **Fields**: `name,rating,class,add,comment_count,blt_name,lat,lng`

## Running the Crawler

### Installation
```bash
pip install -r requirements.txt
```

### Parallel Crawler (Recommended)
```bash
python parallel_poi_crawler.py data/input/your_addresses.csv
```

### Configuration
- Default: 4 parallel workers, batch size of 20 addresses
- Modify `ParallelPOICrawler(max_workers=4, batch_size=20)` for different settings
- Uses headless Chrome with optimized options for performance

### Output Format
- Silent crawling with per-address summary: `Address | 建筑物: Yes/No | 滑动: Yes/No | POI: count`
- Real-time data append to district-named CSV file
- Each POI has individual `comment_count` extracted from rating format like `4.2(144)`

## Key Technical Details

### Browser Configuration
The crawler uses Chrome with specific options:
- Headless mode for performance
- Disabled images/JavaScript/extensions to reduce load time
- Multiple isolated browser instances for parallel processing

### Error Handling
- Individual address failures don't stop batch processing
- Automatic driver cleanup on errors
- Results saved per batch to prevent data loss

### XPath Dependencies
Critical XPaths used in `info_tool.py` and `driver_action.py`:
- Building type detection: `//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]/div/div[1]/div[2]/div/div[2]/span/span/span`
- POI section: `//*[@id="QA0Szd"]/div/div/div[1]/div[2]/div/div[1]/div/div/div[2]`
- More button: Class `M77dve`

These XPaths are fragile and may break with Google Maps UI changes.

## Dependencies

Required Python packages (create requirements.txt):
- selenium
- webdriver-manager
- beautifulsoup4
- pandas
- tqdm

## Development Notes

### Recent Improvements
- Real-time data saving prevents data loss on interruption
- District-named files for better organization
- Complete silence during crawling (no debug output)
- Individual comment count extraction per POI
- Removed legacy utilities and notebook files

### Code Quality Issues
- Mixed languages in comments (Chinese/English)
- Hardcoded XPaths that may break with UI changes
- Building type detection may need adjustment for current Google Maps UI

### Performance Considerations
- Parallel processing provides 4-8x speed improvement over sequential
- Real-time append prevents memory issues with large datasets
- Complete Chrome silence eliminates noise
- Chrome driver management is critical for stability

### Testing
Currently no automated tests exist. Building type detection may need debugging if all addresses show "建筑物: 否".