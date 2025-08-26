# Market Data Pipeline

This is my data pipeline for processing academic papers from OpenAlex API and storing them in a PostgreSQL database.

## Overview

The pipeline consists of several components that work together to:
1. **Fetch academic papers** from OpenAlex API
2. **Process and transform** the data for database storage
3. **Store papers** in a PostgreSQL database with deduplication
4. **Manage database schema** and connections

## Components

### 1. **Consolidated Pipeline (`pipeline.py`)** 🆕 **⭐ RECOMMENDED**
- **Purpose**: **NEW!** Single script that consolidates all functionality
- **Key Features**:
  - **Complete Workflow**: API → Database → Quality Tests in one command
  - **Configurable Parameters**: Days, batch size, force table recreation
  - **Automatic Execution**: No need to run multiple scripts manually
  - **Progress Tracking**: Real-time updates with emojis for easy reading
  - **Error Handling**: Robust error handling with graceful degradation
  - **Flexible Options**: Skip quality tests, force table recreation, etc.
- **Usage**: `python pipeline.py [--days N] [--batch-size N] [--force] [--skip-quality-tests]`

### 2. Database Connection (`db_connection.py`)
- **Purpose**: Manages PostgreSQL database connections using Neon database
- **Features**: 
  - Environment variable management for secure password handling
  - Connection testing and validation
  - Error handling and logging
- **Usage**: Imported by other scripts for database connectivity

### 3. Papers Table Management (`create_papers_table.py`)
- **Purpose**: Creates and manages the `papers` table schema
- **Features**:
  - 40+ fields covering all aspects of academic papers
  - Proper indexing for efficient querying
  - Timestamp tracking for data freshness
  - Comprehensive metadata storage
- **Usage**: Automatically called by the processing script

### 4. Papers Processing (`process_papers_json.py`) ⭐ **Legacy Script**
- **Purpose**: Processes JSON data from OpenAlex API and inserts papers into the database
- **Key Features**:
  - **JSON Data Loading**: Loads papers data from JSON files (like those in `temp/` folder)
  - **Database Integration**: Uses existing `db_connection.py` for PostgreSQL connectivity
  - **Table Management**: Automatically creates papers table using `create_papers_table.py`
  - **Data Transformation**: Converts OpenAlex API format to database schema
  - **Deduplication**: Prevents duplicate papers using PostgreSQL's `ON CONFLICT` handling
  - **Batch Processing**: Processes large datasets in configurable batches
  - **Error Handling**: Gracefully handles missing/null data fields
  - **Progress Tracking**: Shows real-time processing progress
  - **Comprehensive Logging**: Detailed logging for debugging and monitoring

### 5. Data Quality Testing (`data_quality_tests.py`)
- **Purpose**: Runs comprehensive data quality tests on the papers table
- **Key Features**:
  - **Missing Field Detection**: Identifies papers with missing required fields
  - **Citation Validation**: Validates citation counts and ranges
  - **Score Range Validation**: Ensures topic scores are within expected ranges
  - **Duplicate Detection**: Finds duplicate IDs and DOIs
  - **Comprehensive Reporting**: Generates detailed quality reports
  - **Automated Testing**: Can be run independently or as part of the pipeline

## Usage

### Prerequisites
1. **Virtual Environment**: Activate the `.venv` folder
2. **Database Access**: Ensure your `.env` file contains `DB_PASSWORD`
3. **Dependencies**: All required packages are in `requirements.txt`

### 🆕 **NEW: Consolidated Pipeline (Recommended)**

The new `pipeline.py` script consolidates all functionality into a single command:

```bash
# Activate virtual environment
source .venv/bin/activate

# Basic usage (last 3 days, default batch size)
python pipeline.py

# Customize days and batch size
python pipeline.py --days 7 --batch-size 50

# Force table recreation
python pipeline.py --force

# Skip quality tests for faster execution
python pipeline.py --skip-quality-tests

# Help
python pipeline.py --help
```

**Benefits of the consolidated pipeline:**
- ✅ **One command** instead of multiple scripts
- ✅ **Automatic workflow** from API to database to quality tests
- ✅ **Configurable parameters** for different use cases
- ✅ **Better error handling** and progress tracking
- ✅ **Professional logging** with emojis for easy reading

### Pipeline Steps

The consolidated pipeline executes these steps automatically:

1. **Database Connection** - Establishes connection to PostgreSQL database
2. **API Fetching** - Queries OpenAlex API for recent AI papers
3. **Data Backup** - Saves papers to timestamped JSON file in `temp/` folder
4. **Table Creation** - Ensures papers table exists (creates if needed)
5. **Data Upload** - Processes and uploads papers to database in batches
6. **Quality Testing** - Runs data quality tests and generates report
7. **Cleanup** - Closes database connection and provides summary

### Output Files

- **JSON Backup**: Papers saved to `temp/ai_field_subfield_papers_YYYYMMDD_HHMMSS.json`
- **Quality Report**: Data quality test results saved to `reports/data_quality_report_YYYYMMDD_HHMMSS.txt`

### Error Handling

The pipeline includes comprehensive error handling:

- **Graceful Degradation**: Continues processing even if some papers fail
- **Detailed Logging**: All errors are logged with context
- **Transaction Safety**: Database operations use transactions for data integrity
- **Connection Management**: Automatic connection cleanup on completion or failure

### Performance Considerations

- **Batch Processing**: Papers are processed in configurable batches (default: 100)
- **Pagination**: API calls use pagination to handle large result sets
- **Deduplication**: Existing papers are skipped to avoid duplicates
- **Memory Management**: Large datasets are processed incrementally

### Example Output

The consolidated pipeline provides clear, emoji-based progress updates:

```
🚀 Starting Market Data Pipeline
==================================================
🔌 Connecting to database...
✅ Database connection established
🗓️ Searching for AI papers from the last 3 days...
📅 Date range: 2025-01-23 to 2025-01-26
🎯 Filtering by AI subfield using direct API filtering...
📊 AI papers available: 1,247
📥 Fetching all papers (this may take a moment)...
  📄 Page 1: fetched 200 papers (total: 200)
  📄 Page 2: fetched 200 papers (total: 400)
  📄 Page 3: fetched 200 papers (total: 600)
  📄 Page 4: fetched 200 papers (total: 800)
  📄 Page 5: fetched 200 papers (total: 1000)
  📄 Page 6: fetched 200 papers (total: 1200)
  📄 Page 7: fetched 47 papers (total: 1247)
✅ Successfully fetched 1,247 papers (100.0% of total)
💾 Saving papers to: temp/ai_field_subfield_papers_20250126_143022.json
✅ Successfully saved 1,247 papers to temp/ai_field_subfield_papers_20250126_143022.json
📚 Checking papers table...
✅ Papers table already exists
📤 Uploading 1,247 papers to database...
Processing 1,247 papers in batches of 100...
Processing batch of 100 papers...
Progress: 100/100 papers processed (inserted: 95, skipped: 5)
✅ Batch 1 completed and committed
...
🔍 Running data quality tests...
✅ All tests completed successfully!

📊 Pipeline Summary
------------------------------
Papers fetched: 1,247
Papers inserted: 1,242
Papers skipped: 5
Errors: 0
Success rate: 100.0%

✅ Pipeline completed successfully!
```

### Migration from Individual Scripts

If you were using the individual scripts before, here's how to migrate:

| Old Script | New Pipeline Method |
|------------|---------------------|
| `find_ai_papers.py` | `fetch_recent_papers()` |
| `create_papers_table.py` | `ensure_table_exists()` |
| `process_papers_json.py` | `upload_papers_to_database()` |
| `data_quality_tests.py` | `run_data_quality_tests()` |

### Legacy Usage (Individual Scripts)

If you prefer to use the individual scripts:

### Advanced Options

```bash
# Process with custom batch size
python process_papers_json.py temp/your_file.json --batch-size 50

# Force recreation of papers table
python process_papers_json.py temp/your_file.json --force

# Help
python process_papers_json.py --help
```

### Command Line Arguments
- `json_filepath`: Path to the JSON file containing papers data (required)
- `--batch-size`: Number of papers to process in each batch (default: 100)
- `--force`: Force recreation of the papers table even if it exists
- `--help`: Show help message

## Data Processing

### Input Format
The script expects JSON files with the following structure:

```json
{
  "metadata": {
    "timestamp": "2025-08-25T16:17:58.393346",
    "total_papers": 431,
    "source": "OpenAlex API - Direct Filtering"
  },
  "papers": [
    {
      "id": "https://openalex.org/W3164661918",
      "doi": "https://doi.org/10.46298/dmtcs.11602",
      "title": "Paper Title",
      "publication_year": 2025,
      "authorships": [...],
      "topics": [...],
      "primary_location": {...},
      ...
    }
  ]
}
```

### Output Schema
Papers are inserted into the `papers` table with the following key fields:

- **Basic Info**: ID, DOI, title, display name
- **Temporal**: Publication year, date, created/updated dates
- **Metadata**: Language, paper type, Open Access status
- **Metrics**: Citation counts, author counts, country/institution counts
- **Classification**: Topic hierarchy (domain → field → subfield → topic)
- **Journal Info**: Name, ISSN, indexing status, publisher

## Deduplication Strategy

The script uses a multi-layered approach:
1. **Primary Key**: Uses OpenAlex ID as the primary key
2. **Existence Check**: Checks if paper already exists before insertion
3. **Upsert Logic**: Uses `ON CONFLICT` to update existing papers with new data
4. **Fallback IDs**: Generates fallback IDs if no standard ID is available

## Performance Features

- **Batch Processing**: Configurable batch sizes (default: 100)
- **Database Commits**: After each batch for data safety
- **Memory Efficient**: Processes one paper at a time
- **Indexed Queries**: Leverages existing database indexes

## Error Handling

- **File Errors**: Handles missing or invalid JSON files
- **Database Errors**: Manages connection and query failures
- **Data Errors**: Handles malformed paper data gracefully
- **Null Safety**: Handles missing fields gracefully

## Current Status

✅ **Successfully Processed**: 431 papers from the sample JSON file  
✅ **Database**: Papers table created and populated  
✅ **Data Quality**: All papers processed without errors  
✅ **Performance**: Efficient batch processing working  
✅ **Deduplication**: Working correctly  

## Example Output

```
📚 Processing Papers JSON Data and Inserting into Database
======================================================================
INFO - Loading JSON data from: temp/ai_field_subfield_papers_20250825_161758.json
INFO - ✅ Successfully loaded 431 papers from JSON
INFO - Connecting to database...
INFO - ✅ Papers table already exists
INFO - Processing 431 papers in batches of 100
INFO - Processing batch of 100 papers...
INFO - Progress: 100/431 papers processed (inserted: 95, errors: 0)
INFO - ✅ Batch 1 completed and committed
...

📊 Processing Summary
----------------------------------------
Total papers processed: 431
Papers inserted: 431
Papers skipped (already exist): 0
Errors: 0
Success rate: 100.0%
```

## Troubleshooting

### Common Issues

1. **Database Connection Failed**
   - Check `.env` file for `DB_PASSWORD`
   - Verify database is accessible
   - Check network connectivity

2. **JSON File Not Found**
   - Verify file path is correct
   - Check file permissions
   - Ensure file exists

3. **Table Creation Failed**
   - Check database permissions
   - Verify connection string
   - Check for existing table conflicts

4. **API Rate Limiting**
   - Reduce batch size with `--batch-size`
   - Check OpenAlex API status

5. **Memory Issues**
   - Use smaller batch sizes
   - Monitor system resources during execution

6. **Quality Test Failures**
   - Review the generated quality report
   - Check database table structure

### Getting Help

- Check the logs for detailed error messages
- Review the data quality report for data issues
- Verify database permissions and table structure

### Debug Mode

Enable detailed logging by modifying the logging level in the script:

```python
logging.basicConfig(level=logging.DEBUG)
```

## Dependencies

- `psycopg2-binary`: PostgreSQL database adapter
- `python-dotenv`: Environment variable management
- Standard library: `json`, `logging`, `argparse`, `datetime`

## Files Structure

```
market-data-pipeline/
├── .venv/                           # Virtual environment
├── temp/                            # JSON data files
│   └── ai_field_subfield_papers_20250825_161758.json
├── reports/                         # Data quality test reports
├── pipeline.py                      # 🆕 Consolidated pipeline (RECOMMENDED)
├── example_usage.py                 # 🆕 Example usage demonstrations
├── db_connection.py                 # Database connectivity
├── create_papers_table.py           # Table schema management
├── process_papers_json.py           # Legacy processing script
├── find_ai_papers.py                # Legacy API fetching script
├── data_quality_tests.py            # Data quality testing
├── requirements.txt                  # Python dependencies

└── README.md                        # This file
```

## Examples and Learning

### Example Usage Script

The `example_usage.py` script demonstrates different ways to use the pipeline:

```bash
# Run all examples
python example_usage.py

# Examples include:
# - Basic pipeline usage
# - Custom pipeline steps
# - Error handling patterns
# - Batch size comparisons
```

### Programmatic Usage

You can also use the `MarketDataPipeline` class in your own scripts:

```python
from pipeline import MarketDataPipeline

# Create pipeline instance
pipeline = MarketDataPipeline(days=7, batch_size=50)

# Run complete pipeline
success = pipeline.run_pipeline()

# Or run individual steps
pipeline.connect_database()
pipeline.fetch_recent_papers()
pipeline.upload_papers_to_database()
```

## Next Steps

The system is ready for production use. You can:

1. **Use the Consolidated Pipeline**: `python pipeline.py` for complete workflow
2. **Process Additional Files**: Use the script with new JSON files
3. **Monitor Data**: Check database for data growth and quality
4. **Customize Processing**: Modify field mappings as needed
5. **Scale Up**: Process larger datasets with appropriate batch sizes
6. **Integration**: Use this as part of larger data pipeline workflows

## Future Enhancements

Potential improvements for future versions:

- **Parallel Processing**: Multi-threaded API calls and database operations
- **Incremental Updates**: Only fetch new papers since last run
- **Configuration File**: YAML/JSON configuration for complex setups
- **Monitoring**: Integration with monitoring systems
- **Scheduling**: Built-in cron-like scheduling capabilities

## Contributing

When modifying the script:

1. Update the data transformation logic in `transform_paper_data()`
2. Modify the database insertion in `insert_paper()`
3. Update field mappings as needed
4. Test thoroughly before production use
5. Update this documentation

---

This pipeline follows best practices for data processing, database operations, and error handling, making it robust and maintainable for ongoing use.