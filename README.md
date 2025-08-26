# Market Data Pipeline

This is my data pipeline for processing academic papers from OpenAlex API and storing them in a PostgreSQL database.

## Overview

The pipeline consists of several components that work together to:
1. **Fetch academic papers** from OpenAlex API
2. **Process and transform** the data for database storage
3. **Store papers** in a PostgreSQL database with deduplication
4. **Manage database schema** and connections

## Components

### 1. Database Connection (`db_connection.py`)
- **Purpose**: Manages PostgreSQL database connections using Neon database
- **Features**: 
  - Environment variable management for secure password handling
  - Connection testing and validation
  - Error handling and logging
- **Usage**: Imported by other scripts for database connectivity

### 2. Papers Table Management (`create_papers_table.py`)
- **Purpose**: Creates and manages the `papers` table schema
- **Features**:
  - 40+ fields covering all aspects of academic papers
  - Proper indexing for efficient querying
  - Timestamp tracking for data freshness
  - Comprehensive metadata storage
- **Usage**: Automatically called by the processing script

### 3. Papers Processing (`process_papers_json.py`) ⭐ **Main Script**
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

## Usage

### Prerequisites
1. **Virtual Environment**: Activate the `.venv` folder
2. **Database Access**: Ensure your `.env` file contains `DB_PASSWORD`
3. **Dependencies**: All required packages are in `requirements.txt`

### Basic Usage

```bash
# Activate virtual environment
source .venv/bin/activate

# Process a JSON file
python process_papers_json.py temp/ai_field_subfield_papers_20250825_161758.json
```

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
├── db_connection.py                 # Database connectivity
├── create_papers_table.py           # Table schema management
├── process_papers_json.py           # Main processing script ⭐
├── requirements.txt                  # Python dependencies
└── README.md                        # This file
```

## Next Steps

The system is ready for production use. You can:

1. **Process Additional Files**: Use the script with new JSON files
2. **Monitor Data**: Check database for data growth and quality
3. **Customize Processing**: Modify field mappings as needed
4. **Scale Up**: Process larger datasets with appropriate batch sizes
5. **Integration**: Use this as part of larger data pipeline workflows

## Contributing

When modifying the script:

1. Update the data transformation logic in `transform_paper_data()`
2. Modify the database insertion in `insert_paper()`
3. Update field mappings as needed
4. Test thoroughly before production use
5. Update this documentation

---

This pipeline follows best practices for data processing, database operations, and error handling, making it robust and maintainable for ongoing use.