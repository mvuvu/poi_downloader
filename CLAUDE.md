# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

POI Crawler is a multi-threaded web scraping system that extracts Point of Interest data from Google Maps using Selenium. The system features persistent Chrome workers, intelligent retry mechanisms, and checkpoint-based resume capability.

## Architecture

### Core Components & Data Flow

```
poi_crawler_simple.py (Main Controller)
    ├── Creates 10 ChromeWorker threads
    ├── Manages dual queue system (main + retry)
    └── Coordinates with:
        ├── info_tool.py (Data Extraction)
        │   └── XPath-based scrapers for Google Maps DOM
        └── driver_action.py (Browser Automation)
            └── Page interactions (scrolling, clicking)
```

### Key Design Patterns

1. **Persistent Worker Pool**: Each ChromeWorker maintains its own Chrome instance throughout execution, avoiding initialization overhead
2. **Dual Queue System**: Main task queue + high-priority retry queue for failed addresses
3. **Result Buffering**: ResultBuffer class batches writes to CSV (default: 50 records)
4. **Checkpoint System**: JSON-based progress tracking enables resume from interruption

### Critical Implementation Details

#### Chrome Driver Configuration
- Runs headless with disabled images/JavaScript for performance
- Suppresses all Chrome logging via service log path to NUL
- Workers restart Chrome every 1000 tasks to prevent memory leaks

#### Address Processing Priority
```python
# SimplePOICrawler._process_addresses()
1. FormattedAddress (if exists and non-empty)
2. Address (original Japanese)
3. ConvertedAddress (English translation)
```

#### Page Validation Logic
- Invalid pages detected by absence of H1 title within 3 seconds
- Hotel category pages filtered by specific h2 element check
- Only invalid address pages trigger retry with Japanese address

#### XPath Dependencies
The system relies on specific Google Maps DOM structure. Key selectors in `info_tool.py`:
- Building name: `//h2[@class='bwoZTb fontHeadlineLarge']/span`
- POI container: `//div[@role='feed']`
- Coordinates: Extracted from URL pattern `/@(-?\d+\.\d+),(-?\d+\.\d+),`

## Common Commands

### Running the Crawler
```bash
# Process all input files
python poi_crawler_simple.py --all

# Process with custom thread count (default: 10)
python poi_crawler_simple.py --all --workers 8

# Process without progress bar (for cron/scripts)
python poi_crawler_simple.py --all --no-progress

# Fresh run (ignore checkpoints)
python poi_crawler_simple.py --all --no-resume

# Debug mode with single thread
python poi_crawler_simple.py input.csv --workers 1 --verbose
```

### Data Management
```bash
# Monitor progress files
ls -la data/progress/*_simple_progress.json

# Clear progress for fresh run
rm data/progress/*.json

# Check output files
ls -lh data/output/*_simple_*.csv | tail -10
```

## Development Workflow

### Testing Changes
No formal test suite exists. Manual testing approach:
1. Use small test file with known addresses
2. Run with `--verbose --workers 1` for debugging
3. Check output CSV for data quality
4. Verify checkpoint/resume works correctly

### Modifying Extraction Logic
1. Update XPath selectors in `info_tool.py` when Google Maps DOM changes
2. Test selectors in Chrome DevTools first
3. Add fallback selectors for robustness
4. Update `has_invalid_address_page()` if page detection breaks

### Performance Tuning
- **Workers**: CPU cores = good default, max ~20 for stability
- **Batch Size**: 50 works well, increase for better I/O efficiency
- **Flush Interval**: 30 seconds prevents data loss on crashes

### Common Issues & Solutions

1. **Progress bar shows incorrect total with retries**
   - Fixed in recent update: retry tasks now update progress bar total dynamically
   
2. **Chrome crashes/hangs**
   - Reduce worker count
   - Check available memory
   - Ensure Chrome version compatible with selenium

3. **High failure rate**
   - Check if Google Maps DOM changed (update XPaths)
   - Verify network stability
   - Add delays if rate-limited

4. **Resume not working**
   - Check progress file isn't corrupted
   - Ensure output file still exists at saved path
   - Delete progress file for fresh start

## Code Patterns to Follow

### Logging Format
```python
print(f"🔍 {address[:30]}... | 検索中")  # Searching
print(f"✅ {address[:30]}... | POI: {count}")  # Success
print(f"❌ {address[:30]}... | エラー: {error}")  # Error
print(f"🔄 無効地址，使用日文地址重試")  # Retry
```

### Thread Safety
- Use `with self.stats_lock:` for shared statistics
- Queue operations are thread-safe by default
- Progress bar updates need `with self.progress_lock:`

### Error Handling
```python
try:
    # Operation
except TimeoutException:
    # Page load timeout is common, not critical
except Exception as e:
    # Log but don't crash thread
    print(f"❌ Unexpected error: {e}")
```

## Important Notes

- **No linting/formatting tools configured** - follow existing code style
- **No automated tests** - manual verification required
- **Data deduplication** happens at batch level, not globally
- **Address converter mentioned but not found** in current codebase
- **Progress files** are the source of truth for resume functionality