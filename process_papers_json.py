#!/usr/bin/env python3
"""
Script to process JSON data from OpenAlex API and insert papers into the database.

This script:
1. Loads JSON data from a specified filepath
2. Connects to the database using db_connection.py
3. Creates papers table if necessary using create_papers_table.py
4. Processes the data for the papers table
5. Inserts the data with deduplication
"""

import json
import logging
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional
import psycopg2

from db_connection import get_database_connection, close_connection
from create_papers_table import create_papers_table, check_table_exists

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_json_data(filepath: str) -> Dict[str, Any]:
    """
    Load JSON data from the specified filepath.
    
    Args:
        filepath: Path to the JSON file
        
    Returns:
        Dictionary containing the JSON data
        
    Raises:
        FileNotFoundError: If the file doesn't exist
        json.JSONDecodeError: If the JSON is invalid
    """
    try:
        logger.info(f"Loading JSON data from: {filepath}")
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
        
        logger.info(f"✅ Successfully loaded {len(data.get('papers', []))} papers from JSON")
        return data
        
    except FileNotFoundError:
        logger.error(f"❌ File not found: {filepath}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid JSON format: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Error loading JSON file: {e}")
        raise


def extract_paper_id(paper: Dict[str, Any]) -> str:
    """
    Extract the unique paper ID from the paper data.
    
    Args:
        paper: Paper data dictionary
        
    Returns:
        Paper ID string
    """
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


def transform_paper_data(paper: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform paper data from OpenAlex format to database schema format.
    
    Args:
        paper: Paper data from OpenAlex API
        
    Returns:
        Transformed paper data for database insertion
    """
    # Extract basic information
    paper_id = extract_paper_id(paper)
    
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


def check_paper_exists(connection: psycopg2.extensions.connection, paper_id: str) -> bool:
    """
    Check if a paper already exists in the database.
    
    Args:
        connection: Database connection
        paper_id: Paper ID to check
        
    Returns:
        True if paper exists, False otherwise
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT 1 FROM papers WHERE id = %s", (paper_id,))
        exists = cursor.fetchone() is not None
        cursor.close()
        return exists
    except Exception as e:
        logger.error(f"❌ Error checking if paper exists: {e}")
        return False


def insert_paper(connection: psycopg2.extensions.connection, paper_data: Dict[str, Any]) -> bool:
    """
    Insert a single paper into the database.
    
    Args:
        connection: Database connection
        paper_data: Transformed paper data
        
    Returns:
        True if insertion successful, False otherwise
    """
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
        cursor = connection.cursor()
        
        # Prepare values in the correct order
        values = [paper_data.get(field) for field in fields]
        
        cursor.execute(sql, values)
        cursor.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Error inserting paper {paper_data.get('id', 'unknown')}: {e}")
        return False


def process_papers_batch(connection: psycopg2.extensions.connection, papers: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Process a batch of papers and insert them into the database.
    
    Args:
        connection: Database connection
        papers: List of paper data
        
    Returns:
        Dictionary with processing statistics
    """
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
            transformed_paper = transform_paper_data(paper)
            
            # Check if paper already exists
            if check_paper_exists(connection, transformed_paper['id']):
                stats['skipped'] += 1
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(papers)} papers processed (skipped: {stats['skipped']})")
                continue
            
            # Insert the paper
            if insert_paper(connection, transformed_paper):
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


def main():
    """
    Main function to process JSON papers data and insert into database.
    """
    parser = argparse.ArgumentParser(description='Process JSON papers data and insert into database')
    parser.add_argument('json_filepath', help='Path to the JSON file containing papers data')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing (default: 100)')
    parser.add_argument('--force', action='store_true', help='Force recreation of papers table')
    
    args = parser.parse_args()
    
    print("📚 Processing Papers JSON Data and Inserting into Database")
    print("=" * 70)
    
    try:
        # Load JSON data
        data = load_json_data(args.json_filepath)
        papers = data.get('papers', [])
        
        if not papers:
            logger.error("❌ No papers found in the JSON data")
            return
        
        # Get database connection
        logger.info("Connecting to database...")
        connection = get_database_connection()
        
        if not connection:
            logger.error("❌ Failed to connect to database")
            return
        
        try:
            # Check if papers table exists
            if not check_table_exists(connection) or args.force:
                logger.info("Creating papers table...")
                if not create_papers_table(connection):
                    logger.error("❌ Failed to create papers table")
                    return
                logger.info("✅ Papers table created successfully!")
            else:
                logger.info("✅ Papers table already exists")
            
            # Process papers in batches
            total_papers = len(papers)
            logger.info(f"Processing {total_papers} papers in batches of {args.batch_size}")
            
            overall_stats = {
                'total': total_papers,
                'inserted': 0,
                'updated': 0,
                'skipped': 0,
                'errors': 0
            }
            
            # Process in batches
            for i in range(0, total_papers, args.batch_size):
                batch = papers[i:i + args.batch_size]
                batch_stats = process_papers_batch(connection, batch)
                
                # Update overall stats
                overall_stats['inserted'] += batch_stats['inserted']
                overall_stats['skipped'] += batch_stats['skipped']
                overall_stats['errors'] += batch_stats['errors']
                
                # Commit after each batch
                connection.commit()
                logger.info(f"✅ Batch {i//args.batch_size + 1} completed and committed")
            
            # Final summary
            print("\n📊 Processing Summary")
            print("-" * 40)
            print(f"Total papers processed: {overall_stats['total']}")
            print(f"Papers inserted: {overall_stats['inserted']}")
            print(f"Papers skipped (already exist): {overall_stats['skipped']}")
            print(f"Errors: {overall_stats['errors']}")
            print(f"Success rate: {((overall_stats['inserted'] + overall_stats['skipped']) / overall_stats['total'] * 100):.1f}%")
            
        finally:
            # Close the connection
            close_connection(connection)
            
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
