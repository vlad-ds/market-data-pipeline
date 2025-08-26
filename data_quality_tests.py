#!/usr/bin/env python3
"""
Data Quality Testing Script for Papers Table

This script runs essential data quality tests on the papers table:
1. Missing Required Fields - Check for NULL values in required fields
2. Citation Count Validation - Ensure citation counts are reasonable
3. Score Range Validation - Verify topic scores are within expected ranges
4. Duplicate Detection - Find duplicate IDs, DOIs, and titles

Usage:
    python data_quality_tests.py
"""

import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any
import psycopg2

from db_connection import get_database_connection, close_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityTester:
    """Class to run data quality tests on the papers table."""
    
    def __init__(self, connection):
        """Initialize with database connection."""
        self.connection = connection
        self.test_results = {}
        
    def run_all_tests(self) -> Dict[str, Any]:
        """Run all data quality tests and return results."""
        logger.info("🔍 Starting data quality tests...")
        
        try:
            # Test 1: Missing Required Fields
            self.test_missing_required_fields()
            
            # Test 2: Citation Count Validation
            self.test_citation_count_validation()
            
            # Test 3: Score Range Validation
            self.test_score_range_validation()
            
            # Test 4: Duplicate Detection
            self.test_duplicate_detection()
            
            logger.info("✅ All tests completed successfully!")
            return self.test_results
            
        except Exception as e:
            logger.error(f"❌ Error running tests: {e}")
            raise
    
    def test_missing_required_fields(self):
        """Test for missing required fields (title, id)."""
        logger.info("Testing missing required fields...")
        
        query = """
        SELECT 
            COUNT(*) as total_papers,
            COUNT(CASE WHEN id IS NULL THEN 1 END) as missing_id,
            COUNT(CASE WHEN title IS NULL THEN 1 END) as missing_title,
            COUNT(CASE WHEN id IS NULL OR title IS NULL THEN 1 END) as missing_any_required
        FROM papers;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            total_papers, missing_id, missing_title, missing_any = result
            
            # Get examples of papers with missing required fields
            examples_query = """
            SELECT id, title, doi, publication_year
            FROM papers 
            WHERE id IS NULL OR title IS NULL
            LIMIT 5;
            """
            
            cursor = self.connection.cursor()
            cursor.execute(examples_query)
            examples = cursor.fetchall()
            cursor.close()
            
            self.test_results['missing_required_fields'] = {
                'total_papers': total_papers,
                'missing_id': missing_id,
                'missing_title': missing_title,
                'missing_any_required': missing_any,
                'examples': examples,
                'status': 'PASS' if missing_any == 0 else 'FAIL'
            }
            
            logger.info(f"Missing required fields test: {missing_any} papers with missing required fields out of {total_papers}")
            
        except Exception as e:
            logger.error(f"Error in missing required fields test: {e}")
            self.test_results['missing_required_fields'] = {'error': str(e), 'status': 'ERROR'}
    
    def test_citation_count_validation(self):
        """Test citation count validation (non-negative, reasonable upper bound)."""
        logger.info("Testing citation count validation...")
        
        query = """
        SELECT 
            COUNT(*) as total_papers,
            COUNT(CASE WHEN cited_by_count < 0 THEN 1 END) as negative_citations,
            COUNT(CASE WHEN cited_by_count > 100000 THEN 1 END) as extremely_high_citations,
            COUNT(CASE WHEN cited_by_count IS NULL THEN 1 END) as null_citations,
            MIN(cited_by_count) as min_citations,
            MAX(cited_by_count) as max_citations,
            AVG(cited_by_count) as avg_citations
        FROM papers;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            total_papers, negative_citations, extremely_high, null_citations, min_citations, max_citations, avg_citations = result
            
            # Get examples of papers with citation anomalies
            examples_query = """
            SELECT id, title, cited_by_count, publication_year
            FROM papers 
            WHERE cited_by_count < 0 OR cited_by_count > 100000
            ORDER BY ABS(cited_by_count) DESC
            LIMIT 5;
            """
            
            cursor = self.connection.cursor()
            cursor.execute(examples_query)
            examples = cursor.fetchall()
            cursor.close()
            
            # Determine status based on anomalies
            has_anomalies = negative_citations > 0 or extremely_high > 0
            
            self.test_results['citation_count_validation'] = {
                'total_papers': total_papers,
                'negative_citations': negative_citations,
                'extremely_high_citations': extremely_high,
                'null_citations': null_citations,
                'min_citations': min_citations,
                'max_citations': max_citations,
                'avg_citations': round(avg_citations, 2) if avg_citations else None,
                'examples': examples,
                'status': 'PASS' if not has_anomalies else 'FAIL'
            }
            
            logger.info(f"Citation count validation: {negative_citations + extremely_high} papers with citation anomalies out of {total_papers}")
            
        except Exception as e:
            logger.error(f"Error in citation count validation test: {e}")
            self.test_results['citation_count_validation'] = {'error': str(e), 'status': 'ERROR'}
    
    def test_score_range_validation(self):
        """Test that topic scores are within expected ranges (0-1)."""
        logger.info("Testing score range validation...")
        
        query = """
        SELECT 
            COUNT(*) as total_papers,
            COUNT(CASE WHEN primary_topic_score < 0 THEN 1 END) as negative_scores,
            COUNT(CASE WHEN primary_topic_score > 1 THEN 1 END) as scores_above_one,
            COUNT(CASE WHEN primary_topic_score IS NULL THEN 1 END) as null_scores,
            MIN(primary_topic_score) as min_score,
            MAX(primary_topic_score) as max_score,
            AVG(primary_topic_score) as avg_score
        FROM papers
        WHERE primary_topic_score IS NOT NULL;
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.fetchall()  # Clear any remaining results
            cursor.close()
            
            total_papers, negative_scores, scores_above_one, null_scores, min_score, max_score, avg_score = result
            
            # Get examples of papers with score anomalies
            examples_query = """
            SELECT id, title, primary_topic_score, primary_topic_name
            FROM papers 
            WHERE (primary_topic_score < 0 OR primary_topic_score > 1) 
                AND primary_topic_score IS NOT NULL
            ORDER BY ABS(primary_topic_score - 0.5) DESC
            LIMIT 5;
            """
            
            cursor = self.connection.cursor()
            cursor.execute(examples_query)
            examples = cursor.fetchall()
            cursor.close()
            
            # Determine status based on anomalies
            has_anomalies = negative_scores > 0 or scores_above_one > 0
            
            self.test_results['score_range_validation'] = {
                'total_papers_with_scores': total_papers,
                'negative_scores': negative_scores,
                'scores_above_one': scores_above_one,
                'null_scores': null_scores,
                'min_score': min_score,
                'max_score': max_score,
                'avg_score': round(avg_score, 4) if avg_score else None,
                'examples': examples,
                'status': 'PASS' if not has_anomalies else 'FAIL'
            }
            
            logger.info(f"Score range validation: {negative_scores + scores_above_one} papers with score anomalies out of {total_papers}")
            
        except Exception as e:
            logger.error(f"Error in score range validation test: {e}")
            self.test_results['score_range_validation'] = {'error': str(e), 'status': 'ERROR'}
    
    def test_duplicate_detection(self):
        """Test for duplicate IDs and DOIs only."""
        logger.info("Testing duplicate detection...")
        
        # Check for duplicate IDs
        duplicate_ids_query = """
        SELECT id, COUNT(*) as count
        FROM papers 
        WHERE id IS NOT NULL
        GROUP BY id 
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10;
        """
        
        # Check for duplicate DOIs
        duplicate_dois_query = """
        SELECT doi, COUNT(*) as count
        FROM papers 
        WHERE doi IS NOT NULL
        GROUP BY doi 
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10;
        """
        
        try:
            cursor = self.connection.cursor()
            
            # Check duplicate IDs
            cursor.execute(duplicate_ids_query)
            duplicate_ids = cursor.fetchall()
            
            # Check duplicate DOIs
            cursor.execute(duplicate_dois_query)
            duplicate_dois = cursor.fetchall()
            
            cursor.close()
            
            # Count total duplicates
            total_duplicate_ids = len(duplicate_ids)
            total_duplicate_dois = len(duplicate_dois)
            
            # Determine overall status
            has_duplicates = total_duplicate_ids > 0 or total_duplicate_dois > 0
            
            self.test_results['duplicate_detection'] = {
                'duplicate_ids': {
                    'count': total_duplicate_ids,
                    'examples': duplicate_ids[:5]  # Top 5 most duplicated
                },
                'duplicate_dois': {
                    'count': total_duplicate_dois,
                    'examples': duplicate_dois[:5]  # Top 5 most duplicated
                },
                'total_duplicates': total_duplicate_ids + total_duplicate_dois,
                'status': 'PASS' if not has_duplicates else 'FAIL'
            }
            
            logger.info(f"Duplicate detection: Found {total_duplicate_ids} duplicate IDs, {total_duplicate_dois} duplicate DOIs")
            
        except Exception as e:
            logger.error(f"Error in duplicate detection test: {e}")
            self.test_results['duplicate_detection'] = {'error': str(e), 'status': 'ERROR'}
    
    def generate_report(self) -> str:
        """Generate a comprehensive test report."""
        report = []
        report.append("=" * 80)
        report.append("📊 DATA QUALITY TEST REPORT")
        report.append("=" * 80)
        report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("")
        
        # Summary
        total_tests = len(self.test_results)
        passed_tests = sum(1 for test in self.test_results.values() if test.get('status') == 'PASS')
        failed_tests = sum(1 for test in self.test_results.values() if test.get('status') == 'FAIL')
        error_tests = sum(1 for test in self.test_results.values() if test.get('status') == 'ERROR')
        
        report.append("📈 TEST SUMMARY")
        report.append("-" * 40)
        report.append(f"Total Tests: {total_tests}")
        report.append(f"✅ Passed: {passed_tests}")
        report.append(f"❌ Failed: {failed_tests}")
        report.append(f"⚠️  Errors: {error_tests}")
        report.append("")
        
        # Detailed results for each test
        for test_name, results in self.test_results.items():
            report.append(f"🔍 {test_name.upper().replace('_', ' ')}")
            report.append("-" * 40)
            
            if 'error' in results:
                report.append(f"❌ ERROR: {results['error']}")
            else:
                status_icon = "✅" if results['status'] == 'PASS' else "❌"
                report.append(f"{status_icon} Status: {results['status']}")
                
                # Add specific details for each test
                if test_name == 'missing_required_fields':
                    report.append(f"   Total Papers: {results['total_papers']}")
                    report.append(f"   Missing ID: {results['missing_id']}")
                    report.append(f"   Missing Title: {results['missing_title']}")
                    if results['examples']:
                        report.append("   Examples of problematic papers:")
                        for example in results['examples']:
                            report.append(f"     - ID: {example[0]}, Title: {example[1]}")
                
                elif test_name == 'citation_count_validation':
                    report.append(f"   Total Papers: {results['total_papers']}")
                    report.append(f"   Negative Citations: {results['negative_citations']}")
                    report.append(f"   Extremely High Citations: {results['extremely_high_citations']}")
                    report.append(f"   Citation Range: {results['min_citations']} to {results['max_citations']}")
                    report.append(f"   Average Citations: {results['avg_citations']}")
                
                elif test_name == 'score_range_validation':
                    report.append(f"   Papers with Scores: {results['total_papers_with_scores']}")
                    report.append(f"   Negative Scores: {results['negative_scores']}")
                    report.append(f"   Scores Above 1: {results['scores_above_one']}")
                    report.append(f"   Score Range: {results['min_score']} to {results['max_score']}")
                    report.append(f"   Average Score: {results['avg_score']}")
                
                elif test_name == 'duplicate_detection':
                    report.append(f"   Duplicate IDs: {results['duplicate_ids']['count']}")
                    report.append(f"   Duplicate DOIs: {results['duplicate_dois']['count']}")
                    report.append(f"   Total Duplicates: {results['total_duplicates']}")
                    
                    # Show examples of duplicate IDs if any
                    if results['duplicate_ids']['examples']:
                        report.append("   Examples of duplicate IDs:")
                        for example in results['duplicate_ids']['examples']:
                            report.append(f"     - '{example[0]}' (appears {example[1]} times)")
                    
                    # Show examples of duplicate DOIs if any
                    if results['duplicate_dois']['examples']:
                        report.append("   Examples of duplicate DOIs:")
                        for example in results['duplicate_dois']['examples']:
                            report.append(f"     - '{example[0]}' (appears {example[1]} times)")
            
            report.append("")
        
        report.append("=" * 80)
        report.append("🏁 END OF REPORT")
        report.append("=" * 80)
        
        return "\n".join(report)


def main():
    """Main function to run data quality tests."""
    print("🔍 Data Quality Testing for Papers Table")
    print("=" * 60)
    
    # Get database connection
    logger.info("Connecting to database...")
    connection = get_database_connection()
    
    if not connection:
        logger.error("❌ Failed to connect to database")
        return
    
    try:
        # Initialize tester and run tests
        tester = DataQualityTester(connection)
        results = tester.run_all_tests()
        
        # Generate and display report
        report = tester.generate_report()
        print("\n" + report)
        
        # Save report to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"reports/data_quality_report_{timestamp}.txt"
        
        with open(report_filename, 'w', encoding='utf-8') as report_file:
            report_file.write(report)
        
        print(f"\n📄 Report saved to: {report_filename}")
        
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        print(f"\n❌ Error running tests: {e}")
        
    finally:
        # Close the connection
        close_connection(connection)
        print("\n🔌 Database connection closed")


if __name__ == "__main__":
    main()

