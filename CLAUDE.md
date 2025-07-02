# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a POI (Point of Interest) crawler system for Google Maps, designed to extract building information and associated businesses/POIs from Japanese addresses. The system supports both sequential and parallel processing modes.

## Core Architecture

### Module Structure
- **`info_tool.py`** - Core data extraction functions that parse Google Maps pages using Selenium and BeautifulSoup
- **`driver_action.py`** - Selenium interaction helpers for clicking buttons, scrolling, and expanding content
- **`utilities.py`** - File I/O utilities for CSV operations and data management
- **`parallel_poi_crawler.py`** - Multi-process parallel crawler implementation (recommended)
- **`start.ipynb`** - Original sequential crawler (legacy, single-threaded)

### Data Flow
```
CSV Input → Address Processing → Google Maps Navigation → 
Page Interaction → POI Data Extraction → CSV Output
```

### Input/Output Structure
- **Input**: CSV files in `data/input/` with columns: `District,Latitude,Longitude,Address`
- **Output**: CSV files in `data/output/` with POI details: `name,rating,class,add,blt_name,lat,lng,comment_count`

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
- Silent crawling with per-address summary: `Address | 建筑物: Yes/No | 滑动: Yes/No | POI: count | 评论: count`
- Final CSV includes `comment_count` field

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

### Code Quality Issues
- Mixed languages in comments (Chinese/English)
- Hardcoded XPaths that may break with UI changes
- Legacy code in notebooks needs cleanup

### Performance Considerations
- Parallel processing provides 4-8x speed improvement over sequential
- Batch processing prevents memory issues with large datasets
- Chrome driver management is critical for stability

### Testing
Currently no automated tests exist. Manual testing with small address samples recommended before large runs.