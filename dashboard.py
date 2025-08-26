#!/usr/bin/env python3
"""
Streamlit Dashboard for AI Papers Data

This dashboard provides insights into the AI papers dataset stored in the Neon database.
It displays various metrics, charts, and analysis of the research papers.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta
import logging

# Import our database connection module
from db_connection import get_database_connection, close_connection

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="AI Papers Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .chart-container {
        background-color: white;
        padding: 1rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

def get_database_data():
    """Fetch data from the database and return as a pandas DataFrame."""
    try:
        connection = get_database_connection()
        if not connection:
            st.error("❌ Failed to connect to database. Please check your connection settings.")
            return None
        
        # Query to get all papers data
        query = """
        SELECT 
            id, title, doi, publication_year, publication_date, created_date,
            is_open_access, oa_status, cited_by_count, referenced_works_count,
            authors_count, countries_distinct_count, institutions_distinct_count,
            citation_normalized_percentile, is_in_top_1_percent, is_in_top_10_percent,
            journal_name, primary_topic_name, primary_subfield_name, primary_field_name,
            primary_domain_name, paper_type, language, created_at
        FROM papers
        ORDER BY created_at DESC
        """
        
        df = pd.read_sql_query(query, connection)
        connection.close()
        
        # Convert date columns
        date_columns = ['publication_date', 'created_date', 'created_at']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        # Convert numeric columns
        numeric_columns = ['cited_by_count', 'referenced_works_count', 'authors_count', 
                          'countries_distinct_count', 'institutions_distinct_count']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        return df
    
    except Exception as e:
        st.error(f"❌ Error fetching data: {str(e)}")
        return None

def display_header():
    """Display the main header and description."""
    st.markdown('<h1 class="main-header">📊 AI Papers Dashboard</h1>', unsafe_allow_html=True)
    st.markdown("""
    This dashboard provides comprehensive insights into AI research papers from the OpenAlex database.
    Explore publication trends, citation metrics, open access statistics, and more.
    """)
    st.markdown("---")

def display_key_metrics(df):
    """Display key metrics in cards."""
    st.subheader("📈 Key Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_papers = len(df)
        st.metric("Total Papers", f"{total_papers:,}")
    
    with col2:
        recent_papers = len(df[df['created_at'] >= datetime.now() - timedelta(days=7)])
        st.metric("Papers (Last 7 Days)", f"{recent_papers:,}")
    
    with col3:
        # Calculate average citations excluding papers with 0 or null citations
        papers_with_citations = df[df['cited_by_count'] > 0]
        if len(papers_with_citations) > 0:
            avg_citations = papers_with_citations['cited_by_count'].mean()
            st.metric("Avg Citations (Non-zero)", f"{avg_citations:.1f}")
        else:
            st.metric("Avg Citations (Non-zero)", "N/A")
    
    with col4:
        papers_with_citations = df[df['cited_by_count'] > 0]
        papers_with_citations_count = len(papers_with_citations)
        papers_with_citations_pct = (papers_with_citations_count / len(df)) * 100
        st.metric("Papers with Citations", f"{papers_with_citations_count:,} ({papers_with_citations_pct:.1f}%)")

def display_publication_trends(df):
    """Display publication trends over time."""
    st.subheader("📅 Publication Trends")
    
    # Filter out papers without publication date
    df_with_date = df[df['publication_date'].notna()]
    
    if len(df_with_date) > 0:
        # Papers published by day
        daily_counts = df_with_date.groupby(df_with_date['publication_date'].dt.date).size()
        
        fig = px.line(
            x=daily_counts.index,
            y=daily_counts.values,
            title="Papers Published by Day",
            labels={'x': 'Publication Date', 'y': 'Number of Papers'},
            markers=True
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

def display_citation_analysis(df):
    """Display citation analysis and metrics."""
    st.subheader("📚 Citation Analysis")
    
    # Top cited papers
    top_papers = df.nlargest(10, 'cited_by_count')[['title', 'cited_by_count', 'publication_year']]
    
    fig = px.bar(
        x=top_papers['cited_by_count'],
        y=top_papers['title'].str[:50] + '...',
        orientation='h',
        title="Top 10 Most Cited Papers",
        labels={'x': 'Citations', 'y': 'Paper Title'}
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)



def display_topic_analysis(df):
    """Display topic and field analysis."""
    st.subheader("🏷️ Topic Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Primary domain distribution
        domain_counts = df['primary_domain_name'].value_counts().head(10)
        
        fig = px.bar(
            x=domain_counts.values,
            y=domain_counts.index,
            orientation='h',
            title="Top 10 Primary Domains",
            labels={'x': 'Number of Papers', 'y': 'Domain'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Primary field distribution
        field_counts = df['primary_field_name'].value_counts().head(10)
        
        fig2 = px.bar(
            x=field_counts.values,
            y=field_counts.index,
            orientation='h',
            title="Top 10 Primary Fields",
            labels={'x': 'Number of Papers', 'y': 'Field'}
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

def display_journal_analysis(df):
    """Display journal and source analysis."""
    st.subheader("📖 Journal Analysis")
    
    # Top journals by paper count
    journal_counts = df['journal_name'].value_counts().head(15)
    
    fig = px.bar(
        x=journal_counts.values,
        y=journal_counts.index,
        orientation='h',
        title="Top 15 Journals by Number of Papers",
        labels={'x': 'Number of Papers', 'y': 'Journal Name'}
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

def display_data_quality_metrics(df):
    """Display data quality metrics."""
    st.subheader("🔍 Data Quality Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Missing data percentages
        missing_data = df.isnull().sum()
        missing_pct = (missing_data / len(df)) * 100
        
        st.metric("Papers with DOI", f"{df['doi'].notna().sum():,}")
        st.metric("Papers with Date", f"{df['publication_date'].notna().sum():,}")
        st.metric("Papers with Citations", f"{df['cited_by_count'].notna().sum():,}")
    
    with col2:
        # Data completeness
        complete_records = df.dropna(subset=['title', 'publication_date', 'cited_by_count']).shape[0]
        completeness_pct = (complete_records / len(df)) * 100
        
        st.metric("Complete Records", f"{complete_records:,}")
        st.metric("Completeness %", f"{completeness_pct:.1f}%")
        st.metric("Total Fields", len(df.columns))
    
    with col3:
        # Recent activity
        last_update = df['created_at'].max()
        days_since_update = (datetime.now() - last_update).days if last_update else 0
        
        st.metric("Last Update", last_update.strftime('%Y-%m-%d') if last_update else 'N/A')
        st.metric("Days Since Update", days_since_update)
        st.metric("Unique Journals", df['journal_name'].nunique())

def main():
    """Main function to run the dashboard."""
    try:
        # Display header
        display_header()
        
        # Load data
        with st.spinner("🔄 Loading data from database..."):
            df = get_database_data()
        
        if df is None:
            st.error("❌ Could not load data. Please check your database connection.")
            return
        
        # Display success message
        st.success(f"✅ Successfully loaded {len(df):,} papers from database!")
        
        # Display key metrics
        display_key_metrics(df)
        st.markdown("---")
        
        # Display publication trends
        display_publication_trends(df)
        st.markdown("---")
        
        # Display citation analysis
        display_citation_analysis(df)
        st.markdown("---")
        
        # Display topic analysis
        display_topic_analysis(df)
        st.markdown("---")
        
        # Display journal analysis
        display_journal_analysis(df)
        st.markdown("---")
        
        # Display data quality metrics
        display_data_quality_metrics(df)
        
        # Footer
        st.markdown("---")
        st.markdown("*Dashboard generated on " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "*")
        
    except Exception as e:
        st.error(f"❌ An error occurred: {str(e)}")
        logger.error(f"Dashboard error: {str(e)}")

if __name__ == "__main__":
    main()
