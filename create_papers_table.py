#!/usr/bin/env python3
"""
Script to create the papers table in PostgreSQL database using the simplified schema.
Uses the existing db_connection.py module for database connectivity.
"""

import logging
from db_connection import get_database_connection, close_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_papers_table(connection):
    """
    Create the papers table with the simplified schema if it doesn't exist.
    
    Args:
        connection: PostgreSQL database connection
    """
    
    # SQL to create the papers table
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS papers (
        id VARCHAR(255) PRIMARY KEY,  -- OpenAlex ID
        doi VARCHAR(500) UNIQUE,      -- DOI URL
        title TEXT NOT NULL,          -- Paper title
        display_name TEXT,            -- Alternative display name
        
        -- Temporal data
        publication_year INTEGER,
        publication_date DATE,
        created_date DATE,
        updated_date TIMESTAMP,
        
        -- Basic metadata
        language VARCHAR(10),
        paper_type VARCHAR(50),       -- article, review, etc.
        type_crossref VARCHAR(100),   -- More specific type
        
        -- Open Access information
        is_open_access BOOLEAN,
        oa_status VARCHAR(50),        -- gold, diamond, hybrid, etc.
        oa_url TEXT,
        
        -- Quantitative measures (key for dashboard metrics)
        cited_by_count INTEGER DEFAULT 0,
        referenced_works_count INTEGER DEFAULT 0,
        authors_count INTEGER DEFAULT 0,
        countries_distinct_count INTEGER DEFAULT 0,
        institutions_distinct_count INTEGER DEFAULT 0,
        
        -- Citation metrics
        citation_normalized_percentile DECIMAL(5,4),
        is_in_top_1_percent BOOLEAN DEFAULT FALSE,
        is_in_top_10_percent BOOLEAN DEFAULT FALSE,
        
        -- Source/Journal information
        journal_name TEXT,
        journal_issn VARCHAR(20),
        journal_is_oa BOOLEAN,
        journal_is_indexed_scopus BOOLEAN,
        journal_is_core BOOLEAN,
        journal_host_organization TEXT,
        
        -- Topic classification (flattened)
        primary_topic_name TEXT,
        primary_topic_score DECIMAL(6,4),
        primary_subfield_name TEXT,
        primary_field_name TEXT,
        primary_domain_name TEXT,
        
        -- Additional metadata
        is_retracted BOOLEAN DEFAULT FALSE,
        is_paratext BOOLEAN DEFAULT FALSE,
        has_fulltext BOOLEAN DEFAULT FALSE,
        
        -- Timestamps for tracking
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # SQL to create indexes for efficient querying
    create_indexes_sql = [
        "CREATE INDEX IF NOT EXISTS idx_papers_publication_year ON papers(publication_year);",
        "CREATE INDEX IF NOT EXISTS idx_papers_cited_by_count ON papers(cited_by_count);",
        "CREATE INDEX IF NOT EXISTS idx_papers_is_open_access ON papers(is_open_access);",
        "CREATE INDEX IF NOT EXISTS idx_papers_primary_domain ON papers(primary_domain_name);",
        "CREATE INDEX IF NOT EXISTS idx_papers_journal ON papers(journal_name);",
        "CREATE INDEX IF NOT EXISTS idx_papers_created_at ON papers(created_at);"
    ]
    
    try:
        cursor = connection.cursor()
        
        # Create the table
        logger.info("Creating papers table...")
        cursor.execute(create_table_sql)
        logger.info("✅ Papers table created successfully!")
        
        # Create indexes
        logger.info("Creating indexes...")
        for index_sql in create_indexes_sql:
            cursor.execute(index_sql)
        logger.info("✅ All indexes created successfully!")
        
        # Commit the changes
        connection.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error creating papers table: {e}")
        connection.rollback()
        return False

def check_table_exists(connection, table_name="papers"):
    """
    Check if the papers table already exists.
    
    Args:
        connection: PostgreSQL database connection
        table_name: Name of the table to check
        
    Returns:
        True if table exists, False otherwise
    """
    
    check_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = %s
    );
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(check_sql, (table_name,))
        exists = cursor.fetchone()[0]
        cursor.close()
        return exists
        
    except Exception as e:
        logger.error(f"❌ Error checking if table exists: {e}")
        return False

def get_table_info(connection, table_name="papers"):
    """
    Get information about the papers table structure.
    
    Args:
        connection: PostgreSQL database connection
        table_name: Name of the table to describe
        
    Returns:
        List of column information
    """
    
    info_sql = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns 
    WHERE table_name = %s 
    ORDER BY ordinal_position;
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(info_sql, (table_name,))
        columns = cursor.fetchall()
        cursor.close()
        return columns
        
    except Exception as e:
        logger.error(f"❌ Error getting table info: {e}")
        return []

def main():
    """
    Main function to create the papers table.
    """
    
    print("📚 Creating Papers Table in PostgreSQL Database")
    print("=" * 60)
    
    # Get database connection
    logger.info("Connecting to database...")
    connection = get_database_connection()
    
    if not connection:
        logger.error("❌ Failed to connect to database")
        return
    
    try:
        # Check if table already exists
        if check_table_exists(connection):
            logger.info("ℹ️ Papers table already exists!")
            
            # Show table structure
            print("\n📋 Current table structure:")
            print("-" * 40)
            columns = get_table_info(connection)
            for col in columns:
                nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                default = f" DEFAULT {col[3]}" if col[3] else ""
                print(f"{col[0]:<25} {col[1]:<15} {nullable}{default}")
            
            print(f"\n✅ Table 'papers' is ready for use!")
            
        else:
            # Create the table
            logger.info("Creating papers table...")
            if create_papers_table(connection):
                print("\n🎉 Papers table created successfully!")
                print("✅ Table is ready for storing AI papers data!")
                
                # Show the new table structure
                print("\n📋 New table structure:")
                print("-" * 40)
                columns = get_table_info(connection)
                for col in columns:
                    nullable = "NULL" if col[2] == "YES" else "NOT NULL"
                    default = f" DEFAULT {col[3]}" if col[3] else ""
                    print(f"{col[0]:<25} {col[1]:<15} {nullable}{default}")
            else:
                print("\n❌ Failed to create papers table!")
                
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        
    finally:
        # Close the connection
        close_connection(connection)
        print("\n🔌 Database connection closed")

if __name__ == "__main__":
    main()
