# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

POI Crawler is a multi-process web scraping system that extracts Point of Interest (POI) data from Google Maps using Selenium. The system consists of two main components:

1. **POI Data Crawler** - Scrapes POI information (names, ratings, classifications, addresses, etc.) from Google Maps
2. **Address Converter** - Converts Japanese addresses to standardized English format with postal codes

## Architecture

### Core Components

- `parallel_poi_crawler.py` - Main orchestrator implementing multi-process crawling with resume capability
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

- **Process Pool Pattern**: Uses ProcessPoolExecutor for parallel crawling across multiple browser instances
- **Resume/Recovery**: Progress tracking in JSON files enables resuming interrupted crawls
- **Driver Pool**: Chrome driver instances cached and reused within processes
- **Batch Processing**: Data written in configurable batch sizes for memory efficiency

## Common Commands

### POI Crawling
```bash
# Install dependencies
pip install -r requirements.txt

# Process all input files
python parallel_poi_crawler.py --all

# Process single file
python parallel_poi_crawler.py data/input/千代田区_complete.csv

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
- `data/oring_add/` - Files for address conversion processing  
- `data/output/` - Generated POI data CSV files
- `data/progress/` - JSON progress tracking files for resume capability
- `data/archive/tokyo_complete_mapping.json` - Address mapping data for English conversion

## Important Implementation Notes

- Chrome runs in headless mode with extensive anti-detection measures
- XPath selectors are specific to Google Maps DOM structure and may need updates
- Address conversion assumes Tokyo area mapping data
- Progress files enable resuming but should be deleted for fresh crawls
- Maximum workers default to CPU count - 1 for optimal performance
- Address conversion directly overwrites input files (no backup created)