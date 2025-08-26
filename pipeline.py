#!/usr/bin/env python3
"""
Market Data Pipeline - Consolidated Pipeline Class

This script consolidates all the existing logic into a single Pipeline class that:
1. Queries API to get recent papers
2. Creates the DB table if needed
3. Uploads the papers to the database
4. Runs data quality tests

Usage:
    python pipeline.py [--days N] [--batch-size N] [--force] [--skip-quality-tests]
"""

import json
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import psycopg2

# Import existing modules
from modules.db_connection import get_database_connection, close_connection
from modules.create_papers_table import create_papers_table, check_table_exists
from modules.data_quality_tests import DataQualityTester

# Import pyalex for API calls
import pyalex

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MarketDataPipeline:
    """Consolidated pipeline class for market data processing."""
    
    def __init__(self, days: int = 3, batch_size: int = 100, force_recreate: bool = False):
        """
        Initialize the pipeline.
        
        Args:
            days: Number of days to look back for papers
            batch_size: Batch size for database operations
            force_recreate: Force recreation of papers table
        """
        self.days = days
        self.batch_size = batch_size
        self.force_recreate = force_recreate
        self.connection = None
        self.papers = []
        self.stats = {
            'papers_fetched': 0,
            'papers_inserted': 0,
            'papers_skipped': 0,
            'errors': 0
        }
    
    def connect_database(self) -> bool:
        """Establish database connection."""
        logger.info("🔌 Connecting to database...")
        self.connection = get_database_connection()
        
        if not self.connection:
            logger.error("❌ Failed to connect to database")
            return False
        
        logger.info("✅ Database connection established")
        return True
    
    def get_ai_identifiers(self) -> str:
        """Get AI-related identifiers for filtering papers by topics."""
        logger.info("🔍 Setting up AI topic filtering...")
        
        # AI subfield ID - use short format for API filtering
        ai_subfield_id = "1702"  # Computer Science -> Artificial Intelligence
        
        logger.info(f"✅ Using AI subfield ID: {ai_subfield_id}")
        logger.info(f"📋 This will find papers where any topic has 'Artificial Intelligence' as the subfield")
        
        return ai_subfield_id
    
    def fetch_recent_papers(self) -> bool:
        """Fetch recent AI papers from the OpenAlex API."""
        logger.info(f"🗓️ Searching for AI papers from the last {self.days} days...")
        
        try:
            # Get AI identifiers
            ai_subfield_id = self.get_ai_identifiers()
            
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=self.days)
            
            # Format dates for OpenAlex API (YYYY-MM-DD)
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"📅 Date range: {start_date_str} to {end_date_str}")
            
            # Filter directly by AI subfield using the API
            logger.info("🎯 Filtering by AI subfield using direct API filtering...")
            works_query = pyalex.Works().filter(
                **{'topics.subfield.id': ai_subfield_id},
                from_publication_date=start_date_str,
                to_publication_date=end_date_str
            )
            
            # Check total count
            total_count = works_query.count()
            logger.info(f"📊 AI papers available: {total_count}")
            
            if total_count == 0:
                logger.warning("⚠️ No papers found for the specified criteria")
                return True
            
            # Get all results using pagination
            logger.info("📥 Fetching all papers (this may take a moment)...")
            all_papers = []
            per_page = 200  # Maximum allowed by OpenAlex
            
            try:
                # Create paginator and iterate through all pages
                paginator = works_query.paginate(per_page=per_page)
                
                for page_num, page_results in enumerate(paginator, 1):
                    all_papers.extend(page_results)
                    logger.info(f"  📄 Page {page_num}: fetched {len(page_results)} papers (total: {len(all_papers)})")
                    
                    # Safety check to prevent excessive API calls
                    if page_num >= 50:  # Reasonable upper limit for ~10,000 papers
                        logger.warning("⚠️ Reached maximum page limit, stopping")
                        break
                
                logger.info(f"✅ Successfully fetched {len(all_papers)} papers ({len(all_papers)/total_count*100:.1f}% of total)")
                
            except Exception as e:
                logger.warning(f"⚠️ Error during pagination: {e}")
                logger.info("🔄 Falling back to single page fetch...")
                # Fallback to simple get() if pagination fails
                all_papers = works_query.get()
                logger.info(f"📄 Fallback: fetched {len(all_papers)} papers")
            
            self.papers = all_papers
            self.stats['papers_fetched'] = len(all_papers)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error fetching papers: {e}")
            return False
    
    def save_papers_to_json(self) -> Optional[str]:
        """Save papers to a timestamped JSON file in temp/ folder."""
        if not self.papers:
            logger.warning("⚠️ No papers to save")
            return None
        
        # Create timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"ai_field_subfield_papers_{timestamp}.json"
        
        # Ensure temp directory exists
        temp_dir = Path('temp')
        temp_dir.mkdir(exist_ok=True)
        
        # Full file path
        file_path = temp_dir / filename
        
        logger.info(f"💾 Saving papers to: {file_path}")
        
        # Prepare data for JSON serialization
        output_data = {
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'total_papers': len(self.papers),
                'date_range_days': self.days,
                'filter_criteria': 'Papers where Artificial Intelligence is the subfield (topics.subfield.id=1702)',
                'ai_subfield_id': '1702',
                'ai_subfield_full_id': 'https://openalex.org/subfields/1702',
                'source': 'OpenAlex API - Direct Filtering'
            },
            'papers': self.papers
        }
        
        # Save to JSON file
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ Successfully saved {len(self.papers)} papers to {file_path}")
        
        return str(file_path)
    
    def ensure_table_exists(self) -> bool:
        """Ensure the papers table exists, create if necessary."""
        logger.info("📚 Checking papers table...")
        
        if not self.connection:
            logger.error("❌ No database connection")
            return False
        
        try:
            # Check if table already exists
            if check_table_exists(self.connection) and not self.force_recreate:
                logger.info("✅ Papers table already exists")
                return True
            
            # Create the table
            logger.info("Creating papers table...")
            if create_papers_table(self.connection):
                logger.info("✅ Papers table created successfully!")
                return True
            else:
                logger.error("❌ Failed to create papers table")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error ensuring table exists: {e}")
            return False
    
    def transform_paper_data(self, paper: Dict[str, Any]) -> Dict[str, Any]:
        """Transform paper data from OpenAlex format to database schema format."""
        # Extract basic information
        paper_id = self.extract_paper_id(paper)
        
        # Extract source/journal information with safe navigation
        primary_location = paper.get('primary_location') or {}
        source = primary_location.get('source') or {}
        
        # Extract topic information (flattened) with safe navigation
        topics = paper.get('topics') or []
        primary_topic = topics[0] if topics else {}
        subfield = primary_topic.get('subfield') or {}
        field = primary_topic.get('field') or {}
        domain = primary_topic.get('domain') or {}
        
        # Extract citation metrics with safe navigation
        citation_metrics = paper.get('citation_metrics') or {}
        
        # Extract authorships with safe navigation
        authorships = paper.get('authorships') or []
        referenced_works = paper.get('referenced_works') or []
        
        # Transform the data with safe navigation
        transformed = {
            'id': paper_id,
            'doi': paper.get('doi'),
            'title': paper.get('title') or paper.get('display_name'),
            'display_name': paper.get('display_name'),
            
            # Temporal data
            'publication_year': paper.get('publication_year'),
            'publication_date': paper.get('publication_date'),
            'created_date': paper.get('created_date'),
            'updated_date': paper.get('updated_date'),
            
            # Basic metadata
            'language': paper.get('language'),
            'paper_type': paper.get('type'),
            'type_crossref': paper.get('type_crossref'),
            
            # Open Access information
            'is_open_access': primary_location.get('is_oa'),
            'oa_status': primary_location.get('oa_status'),
            'oa_url': primary_location.get('pdf_url'),
            
            # Quantitative measures
            'cited_by_count': paper.get('cited_by_count', 0),
            'referenced_works_count': len(referenced_works),
            'authors_count': len(authorships),
            'countries_distinct_count': len(set(
                author.get('country_code') 
                for author in authorships
                if author and author.get('country_code')
            )),
            'institutions_distinct_count': len(set(
                inst.get('id')
                for author in authorships
                if author and author.get('institutions')
                for inst in author.get('institutions') or []
                if inst and inst.get('id')
            )),
            
            # Citation metrics
            'citation_normalized_percentile': citation_metrics.get('normalized_percentile'),
            'is_in_top_1_percent': citation_metrics.get('is_in_top_1_percent', False),
            'is_in_top_10_percent': citation_metrics.get('is_in_top_10_percent', False),
            
            # Source/Journal information
            'journal_name': source.get('display_name'),
            'journal_issn': source.get('issn_l'),
            'journal_is_oa': source.get('is_oa'),
            'journal_is_indexed_scopus': source.get('is_indexed_in_scopus'),
            'journal_is_core': source.get('is_core'),
            'journal_host_organization': source.get('host_organization_name'),
            
            # Topic classification (flattened)
            'primary_topic_name': primary_topic.get('display_name'),
            'primary_topic_score': primary_topic.get('score'),
            'primary_subfield_name': subfield.get('display_name'),
            'primary_field_name': field.get('display_name'),
            'primary_domain_name': domain.get('display_name'),
            
            # Additional metadata
            'is_retracted': paper.get('is_retracted', False),
            'is_paratext': paper.get('is_paratext', False),
            'has_fulltext': paper.get('has_fulltext', False),
        }
        
        return transformed
    
    def extract_paper_id(self, paper: Dict[str, Any]) -> str:
        """Extract the unique paper ID from the paper data."""
        # Try different ID fields in order of preference
        if paper.get('id'):
            return paper['id']
        elif paper.get('doi'):
            return paper['doi']
        elif paper.get('ids', {}).get('openalex'):
            return paper['ids']['openalex']
        else:
            # Generate a fallback ID if none exists
            return f"fallback_{hash(str(paper))}"
    
    def check_paper_exists(self, paper_id: str) -> bool:
        """Check if a paper already exists in the database."""
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT 1 FROM papers WHERE id = %s", (paper_id,))
            exists = cursor.fetchone() is not None
            cursor.close()
            return exists
        except Exception as e:
            logger.error(f"❌ Error checking if paper exists: {e}")
            return False
    
    def insert_paper(self, paper_data: Dict[str, Any]) -> bool:
        """Insert a single paper into the database."""
        # Prepare the SQL statement with all fields
        fields = [
            'id', 'doi', 'title', 'display_name', 'publication_year', 'publication_date',
            'created_date', 'updated_date', 'language', 'paper_type', 'type_crossref',
            'is_open_access', 'oa_status', 'oa_url', 'cited_by_count', 'referenced_works_count',
            'authors_count', 'countries_distinct_count', 'institutions_distinct_count',
            'citation_normalized_percentile', 'is_in_top_1_percent', 'is_in_top_10_percent',
            'journal_name', 'journal_issn', 'journal_is_oa', 'journal_is_indexed_scopus',
            'journal_is_core', 'journal_host_organization', 'primary_topic_name',
            'primary_topic_score', 'primary_subfield_name', 'primary_field_name',
            'primary_domain_name', 'is_retracted', 'is_paratext', 'has_fulltext'
        ]
        
        # Create placeholders for the SQL statement
        placeholders = ', '.join(['%s'] * len(fields))
        field_names = ', '.join(fields)
        
        sql = f"""
        INSERT INTO papers ({field_names})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET
            updated_at = CURRENT_TIMESTAMP,
            title = EXCLUDED.title,
            display_name = EXCLUDED.display_name,
            publication_year = EXCLUDED.publication_year,
            publication_date = EXCLUDED.publication_date,
            cited_by_count = EXCLUDED.cited_by_count,
            referenced_works_count = EXCLUDED.referenced_works_count,
            authors_count = EXCLUDED.authors_count,
            countries_distinct_count = EXCLUDED.countries_distinct_count,
            institutions_distinct_count = EXCLUDED.institutions_distinct_count,
            citation_normalized_percentile = EXCLUDED.citation_normalized_percentile,
            is_in_top_1_percent = EXCLUDED.is_in_top_1_percent,
            is_in_top_10_percent = EXCLUDED.is_in_top_10_percent
        """
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare values in the correct order
            values = [paper_data.get(field) for field in fields]
            
            cursor.execute(sql, values)
            cursor.close()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting paper {paper_data.get('id', 'unknown')}: {e}")
            return False
    
    def process_papers_batch(self, papers: List[Dict[str, Any]]) -> Dict[str, int]:
        """Process a batch of papers and insert them into the database."""
        stats = {
            'total': len(papers),
            'inserted': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }
        
        logger.info(f"Processing batch of {len(papers)} papers...")
        
        for i, paper in enumerate(papers, 1):
            try:
                # Transform the paper data
                transformed_paper = self.transform_paper_data(paper)
                
                # Check if paper already exists
                if self.check_paper_exists(transformed_paper['id']):
                    stats['skipped'] += 1
                    if i % 100 == 0:
                        logger.info(f"Progress: {i}/{len(papers)} papers processed (skipped: {stats['skipped']})")
                    continue
                
                # Insert the paper
                if self.insert_paper(transformed_paper):
                    stats['inserted'] += 1
                else:
                    stats['errors'] += 1
                
                # Log progress every 100 papers
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(papers)} papers processed (inserted: {stats['inserted']}, errors: {stats['errors']})")
                    
            except Exception as e:
                logger.error(f"❌ Error processing paper {i}: {e}")
                stats['errors'] += 1
        
        return stats
    
    def upload_papers_to_database(self) -> bool:
        """Upload all fetched papers to the database."""
        if not self.papers:
            logger.warning("⚠️ No papers to upload")
            return True
        
        if not self.connection:
            logger.error("❌ No database connection")
            return False
        
        logger.info(f"📤 Uploading {len(self.papers)} papers to database...")
        
        try:
            total_papers = len(self.papers)
            logger.info(f"Processing {total_papers} papers in batches of {self.batch_size}")
            
            overall_stats = {
                'total': total_papers,
                'inserted': 0,
                'updated': 0,
                'skipped': 0,
                'errors': 0
            }
            
            # Process in batches
            for i in range(0, total_papers, self.batch_size):
                batch = self.papers[i:i + self.batch_size]
                batch_stats = self.process_papers_batch(batch)
                
                # Update overall stats
                overall_stats['inserted'] += batch_stats['inserted']
                overall_stats['skipped'] += batch_stats['skipped']
                overall_stats['errors'] += batch_stats['errors']
                
                # Commit after each batch
                self.connection.commit()
                logger.info(f"✅ Batch {i//self.batch_size + 1} completed and committed")
            
            # Update pipeline stats
            self.stats['papers_inserted'] = overall_stats['inserted']
            self.stats['papers_skipped'] = overall_stats['skipped']
            self.stats['errors'] = overall_stats['errors']
            
            # Final summary
            logger.info("\n📊 Upload Summary")
            logger.info("-" * 40)
            logger.info(f"Total papers processed: {overall_stats['total']}")
            logger.info(f"Papers inserted: {overall_stats['inserted']}")
            logger.info(f"Papers skipped (already exist): {overall_stats['skipped']}")
            logger.info(f"Errors: {overall_stats['errors']}")
            logger.info(f"Success rate: {((overall_stats['inserted'] + overall_stats['skipped']) / overall_stats['total'] * 100):.1f}%")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error uploading papers: {e}")
            return False
    
    def run_data_quality_tests(self) -> bool:
        """Run data quality tests on the papers table."""
        if not self.connection:
            logger.error("❌ No database connection")
            return False
        
        logger.info("🔍 Running data quality tests...")
        
        try:
            # Initialize tester and run tests
            tester = DataQualityTester(self.connection)
            results = tester.run_all_tests()
            
            # Generate and display report
            report = tester.generate_report()
            print("\n" + report)
            
            # Save report to file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = f"reports/data_quality_report_{timestamp}.txt"
            
            # Ensure reports directory exists
            reports_dir = Path('reports')
            reports_dir.mkdir(exist_ok=True)
            
            with open(report_filename, 'w', encoding='utf-8') as report_file:
                report_file.write(report)
            
            logger.info(f"📄 Report saved to: {report_filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error running data quality tests: {e}")
            return False
    
    def run_pipeline(self, skip_quality_tests: bool = False) -> bool:
        """Run the complete pipeline."""
        logger.info("🚀 Starting Market Data Pipeline")
        logger.info("=" * 50)
        
        try:
            # Step 1: Connect to database
            if not self.connect_database():
                return False
            
            # Step 2: Fetch recent papers from API
            if not self.fetch_recent_papers():
                logger.error("❌ Failed to fetch papers from API")
                return False
            
            if not self.papers:
                logger.warning("⚠️ No papers found, pipeline completed")
                return True
            
            # Step 3: Save papers to JSON (optional, for backup)
            json_file = self.save_papers_to_json()
            if json_file:
                logger.info(f"💾 Papers backed up to: {json_file}")
            
            # Step 4: Ensure database table exists
            if not self.ensure_table_exists():
                logger.error("❌ Failed to ensure table exists")
                return False
            
            # Step 5: Upload papers to database
            if not self.upload_papers_to_database():
                logger.error("❌ Failed to upload papers to database")
                return False
            
            # Step 6: Run data quality tests (optional)
            if not skip_quality_tests:
                if not self.run_data_quality_tests():
                    logger.warning("⚠️ Data quality tests failed, but pipeline completed")
            else:
                logger.info("⏭️ Skipping data quality tests as requested")
            
            logger.info("🎉 Pipeline completed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            return False
        
        finally:
            # Clean up
            if self.connection:
                close_connection(self.connection)
                logger.info("🔌 Database connection closed")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of the pipeline execution."""
        return {
            'papers_fetched': self.stats['papers_fetched'],
            'papers_inserted': self.stats['papers_inserted'],
            'papers_skipped': self.stats['papers_skipped'],
            'errors': self.stats['errors'],
            'success_rate': (
                (self.stats['papers_inserted'] + self.stats['papers_skipped']) / 
                max(self.stats['papers_fetched'], 1) * 100
            ) if self.stats['papers_fetched'] > 0 else 0
        }


def main():
    """Main function to run the pipeline."""
    parser = argparse.ArgumentParser(description='Market Data Pipeline - Consolidated Pipeline')
    parser.add_argument('--days', type=int, default=3, help='Number of days to look back for papers (default: 3)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for database operations (default: 100)')
    parser.add_argument('--force', action='store_true', help='Force recreation of papers table')
    parser.add_argument('--skip-quality-tests', action='store_true', help='Skip data quality tests')
    
    args = parser.parse_args()
    
    print("🤖 Market Data Pipeline - Consolidated Pipeline")
    print("=" * 60)
    
    # Create and run pipeline
    pipeline = MarketDataPipeline(
        days=args.days,
        batch_size=args.batch_size,
        force_recreate=args.force
    )
    
    success = pipeline.run_pipeline(skip_quality_tests=args.skip_quality_tests)
    
    if success:
        # Show summary
        summary = pipeline.get_summary()
        print("\n📊 Pipeline Summary")
        print("-" * 30)
        print(f"Papers fetched: {summary['papers_fetched']}")
        print(f"Papers inserted: {summary['papers_inserted']}")
        print(f"Papers skipped: {summary['papers_skipped']}")
        print(f"Errors: {summary['errors']}")
        print(f"Success rate: {summary['success_rate']:.1f}%")
        print("\n✅ Pipeline completed successfully!")
    else:
        print("\n❌ Pipeline failed!")
        exit(1)


if __name__ == "__main__":
    main()
