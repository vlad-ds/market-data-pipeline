# Market Data Pipeline

Academic papers data pipeline that fetches AI research papers from OpenAlex API and stores them in PostgreSQL.

## 📁 Project Organization

> **Note for students**: The codebase was reorganized for better clarity ([see commit](https://github.com/vlad-ds/market-data-pipeline/commit/e2e0e7c)). All supporting modules are now in `modules/` folder, while main entry points (`pipeline.py` and `dashboard.py`) remain at the root level.

```
market-data-pipeline/
├── pipeline.py          # Main pipeline script (entry point)
├── dashboard.py         # Streamlit dashboard (entry point)
├── modules/             # Supporting Python modules
│   ├── db_connection.py
│   ├── create_papers_table.py
│   ├── find_ai_papers.py
│   ├── process_papers_json.py
│   └── data_quality_tests.py
├── scripts/             # Launch scripts
│   ├── run_dashboard.sh
│   └── run_dashboard.bat
├── temp/                # JSON data backups
├── reports/             # Quality test reports
└── requirements.txt     # Dependencies
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- PostgreSQL database (we use Neon)
- `.env` file with `DB_PASSWORD`

### Installation
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Database Setup
1. Get your Neon database connection string (looks like: `postgresql://user:password@host/database`)
2. Extract the password (the part between `:` and `@` after the username)
3. Create `.env` file in project root with:
```
DB_PASSWORD=your_extracted_password
```
Example: If connection string is `postgresql://john:abc123xyz@ep-cool.aws.neon.tech/dbname`  
Then password is `abc123xyz`

### Running the Pipeline

```bash
# Fetch last 3 days of papers (default)
python pipeline.py

# Customize parameters
python pipeline.py --days 7 --batch-size 50

# Skip quality tests for speed
python pipeline.py --skip-quality-tests

# See all options
python pipeline.py --help
```

### Viewing the Dashboard

```bash
# Using launch scripts
./scripts/run_dashboard.sh   # Linux/Mac
scripts\run_dashboard.bat     # Windows

# Or manually
streamlit run dashboard.py
```

## 📊 What It Does

The pipeline automatically:
1. **Fetches** AI papers from OpenAlex API
2. **Saves** JSON backup to `temp/` folder  
3. **Creates** database table if needed
4. **Processes** papers in batches with deduplication
5. **Tests** data quality and generates report
6. **Displays** results in interactive dashboard

## 🛠️ Key Features

- **Batch Processing**: Configurable batch sizes for large datasets
- **Deduplication**: Prevents duplicate papers using OpenAlex ID
- **Error Handling**: Continues processing even if individual papers fail
- **Quality Testing**: Validates data completeness and consistency
- **Interactive Dashboard**: Real-time visualization of papers data

## 📝 Configuration

### Command Line Options
- `--days N`: Number of days to look back (default: 3)
- `--batch-size N`: Papers per batch (default: 100)
- `--force`: Recreate papers table
- `--skip-quality-tests`: Skip quality validation

### Environment Variables
Create `.env` file:
```
DB_PASSWORD=your_database_password
```

## 🔍 Troubleshooting

| Issue | Solution |
|-------|----------|
| Database connection failed | Check `.env` file has correct `DB_PASSWORD` |
| Module import errors | Ensure you're in project root directory |
| API rate limiting | Reduce `--batch-size` parameter |
| Dashboard won't start | Check Streamlit is installed: `pip install streamlit` |

## 📈 Example Output

```
🚀 Starting Market Data Pipeline
==================================================
🔌 Connecting to database...
✅ Database connection established
🗓️ Searching for AI papers from the last 3 days...
📊 AI papers available: 1,247
📥 Fetching all papers...
✅ Successfully fetched 1,247 papers
💾 Papers backed up to: temp/ai_field_subfield_papers_20250126_143022.json
📤 Uploading to database...
✅ Inserted: 1,242 | Skipped: 5 | Errors: 0
🔍 Running data quality tests...
✅ All tests passed!
```

## 🧪 Data Quality Tests

The pipeline automatically validates:
- Missing required fields
- Citation count ranges
- Topic score validity (0-1 range)
- Duplicate detection (ID and DOI)

Reports are saved to `reports/` folder.

## 💡 For Students

This project demonstrates:
- API integration with rate limiting
- Database design and batch processing
- Data validation and quality testing
- Modular code organization
- Interactive data visualization

Feel free to explore the code in `modules/` to understand each component!

---

Built for educational purposes as part of a data engineering course.