#!/usr/bin/env python3
"""
Database connection module for PostgreSQL using Neon database.

This module provides functionality to connect to a PostgreSQL database
using connection string with password loaded from environment variables.
"""

import os
import psycopg2
from dotenv import load_dotenv
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_environment():
    """Load environment variables from .env file."""
    load_dotenv()
    return os.getenv('DB_PASSWORD')


def create_connection_string(password: str) -> str:
    """
    Create the complete PostgreSQL connection string with password.
    
    Args:
        password: Database password from environment variable
        
    Returns:
        Complete PostgreSQL connection string
    """
    base_connection = "postgresql://neondb_owner:{password}@ep-round-cell-a2bmtbcv-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
    return base_connection.format(password=password)


def connect_to_database(connection_string: str) -> Optional[psycopg2.extensions.connection]:
    """
    Establish connection to PostgreSQL database.
    
    Args:
        connection_string: Complete PostgreSQL connection string
        
    Returns:
        Database connection object or None if connection fails
    """
    try:
        logger.info("Attempting to connect to PostgreSQL database...")
        connection = psycopg2.connect(connection_string)
        logger.info("✅ Successfully connected to PostgreSQL database!")
        return connection
    
    except psycopg2.Error as e:
        logger.error(f"❌ PostgreSQL connection error: {e}")
        return None
    
    except Exception as e:
        logger.error(f"❌ Unexpected error during database connection: {e}")
        return None


def test_connection(connection: psycopg2.extensions.connection) -> bool:
    """
    Test the database connection by executing a simple query.
    
    Args:
        connection: Database connection object
        
    Returns:
        True if connection test succeeds, False otherwise
    """
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"📊 Database version: {version[0]}")
        cursor.close()
        return True
    
    except Exception as e:
        logger.error(f"❌ Connection test failed: {e}")
        return False


def get_database_connection() -> Optional[psycopg2.extensions.connection]:
    """
    Main function to get a database connection.
    
    This function:
    1. Loads the DB_PASSWORD from .env file
    2. Creates the connection string
    3. Establishes the database connection
    4. Tests the connection
    
    Returns:
        Database connection object or None if any step fails
    """
    # Load password from environment
    password = load_environment()
    if not password:
        logger.error("❌ DB_PASSWORD not found in environment variables")
        return None
    
    logger.info("✅ Successfully loaded DB_PASSWORD from environment")
    
    # Create connection string
    connection_string = create_connection_string(password)
    
    # Connect to database
    connection = connect_to_database(connection_string)
    if not connection:
        return None
    
    # Test connection
    if not test_connection(connection):
        connection.close()
        return None
    
    return connection


def close_connection(connection: psycopg2.extensions.connection):
    """
    Safely close database connection.
    
    Args:
        connection: Database connection object to close
    """
    try:
        if connection:
            connection.close()
            logger.info("✅ Database connection closed successfully")
    except Exception as e:
        logger.error(f"❌ Error closing database connection: {e}")


if __name__ == "__main__":
    """
    Test the database connection when script is run directly.
    """
    print("🔌 Testing PostgreSQL Database Connection")
    print("=" * 50)
    
    # Get database connection
    conn = get_database_connection()
    
    if conn:
        print("\n🎉 Database connection test successful!")
        
        # Example usage: Execute a sample query
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT current_database(), current_user, now();")
            result = cursor.fetchone()
            print(f"📊 Connected to database: {result[0]}")
            print(f"👤 Current user: {result[1]}")
            print(f"🕐 Server time: {result[2]}")
            cursor.close()
        except Exception as e:
            print(f"❌ Error executing sample query: {e}")
        
        # Close connection
        close_connection(conn)
    else:
        print("\n❌ Database connection test failed!")
        exit(1)
    
    print("\n✅ Connection test completed!")
