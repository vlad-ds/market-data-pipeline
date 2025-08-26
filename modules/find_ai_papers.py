#!/usr/bin/env python3
"""
Script to find recent AI research papers using the OpenAlex API.

This script:
1. Searches OpenAlex Concepts for "artificial intelligence"
2. Gets the concept ID from the search results
3. Uses that concept ID to filter Works from the last 3 days
4. Saves all papers with all fields in a timestamped JSON file
"""

import json
import pyalex
from datetime import datetime, timedelta
from pathlib import Path


def get_ai_identifiers():
    """Get AI-related identifiers for filtering papers by topics."""
    print("🔍 Setting up AI topic filtering...")
    
    # AI subfield ID - use short format for API filtering
    ai_subfield_id = "1702"  # Computer Science -> Artificial Intelligence
    
    print(f"✅ Using AI subfield ID: {ai_subfield_id}")
    print(f"📋 This will find papers where any topic has 'Artificial Intelligence' as the subfield")
    
    return ai_subfield_id


def get_recent_ai_papers(ai_subfield_id, days=3):
    """Get AI papers from the last N days using direct API filtering."""
    print(f"\n🗓️ Searching for AI papers from the last {days} days...")
    
    # Calculate date range (last N days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Format dates for OpenAlex API (YYYY-MM-DD)
    start_date_str = start_date.strftime('%Y-%m-%d')
    end_date_str = end_date.strftime('%Y-%m-%d')
    
    print(f"📅 Date range: {start_date_str} to {end_date_str}")
    
    # Filter directly by AI subfield using the API
    print("🎯 Filtering by AI subfield using direct API filtering...")
    works_query = pyalex.Works().filter(
        **{'topics.subfield.id': ai_subfield_id},
        from_publication_date=start_date_str,
        to_publication_date=end_date_str
    )
    
    # Check total count
    total_count = works_query.count()
    print(f"📊 AI papers available: {total_count}")
    
    # Get all results using pagination
    print("📥 Fetching all papers (this may take a moment)...")
    all_papers = []
    per_page = 200  # Maximum allowed by OpenAlex
    
    try:
        # Create paginator and iterate through all pages
        paginator = works_query.paginate(per_page=per_page)
        
        for page_num, page_results in enumerate(paginator, 1):
            all_papers.extend(page_results)
            print(f"  📄 Page {page_num}: fetched {len(page_results)} papers (total: {len(all_papers)})")
            
            # Safety check to prevent excessive API calls
            if page_num >= 50:  # Reasonable upper limit for ~10,000 papers
                print("⚠️ Reached maximum page limit, stopping")
                break
        
        print(f"✅ Successfully fetched {len(all_papers)} papers ({len(all_papers)/total_count*100:.1f}% of total)")
        
    except Exception as e:
        print(f"⚠️ Error during pagination: {e}")
        print("🔄 Falling back to single page fetch...")
        # Fallback to simple get() if pagination fails
        all_papers = works_query.get()
        print(f"📄 Fallback: fetched {len(all_papers)} papers")
    
    return all_papers


def save_papers_to_json(papers):
    """Save papers to a timestamped JSON file in temp/ folder."""
    # Create timestamp for filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"ai_field_subfield_papers_{timestamp}.json"
    
    # Ensure temp directory exists
    temp_dir = Path('temp')
    temp_dir.mkdir(exist_ok=True)
    
    # Full file path
    file_path = temp_dir / filename
    
    print(f"\n💾 Saving papers to: {file_path}")
    
    # Prepare data for JSON serialization
    output_data = {
        'metadata': {
            'timestamp': datetime.now().isoformat(),
            'total_papers': len(papers),
            'date_range_days': 3,
            'filter_criteria': 'Papers where Artificial Intelligence is the subfield (topics.subfield.id=1702)',
            'ai_subfield_id': '1702',
            'ai_subfield_full_id': 'https://openalex.org/subfields/1702',
            'source': 'OpenAlex API - Direct Filtering'
        },
        'papers': papers
    }
    
    # Save to JSON file
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"✅ Successfully saved {len(papers)} papers to {file_path}")
    
    return str(file_path)


def print_paper_summary(papers):
    """Print a summary of the found papers."""
    if not papers:
        print("\n❌ No papers found")
        return
    
    print(f"\n📊 Summary of {len(papers)} papers:")
    print("-" * 50)
    
    for i, paper in enumerate(papers[:5], 1):  # Show first 5 papers
        title = paper.get('title', 'No title')
        authors = paper.get('authorships', [])
        author_names = [auth.get('author', {}).get('display_name', 'Unknown') for auth in authors[:3]]
        author_str = ', '.join(author_names)
        if len(authors) > 3:
            author_str += f" et al. ({len(authors)} total)"
        
        publication_date = paper.get('publication_date', 'Unknown date')
        venue = paper.get('primary_location', {}).get('source', {}).get('display_name', 'Unknown venue')
        
        print(f"{i}. {title}")
        print(f"   Authors: {author_str}")
        print(f"   Date: {publication_date}")
        print(f"   Venue: {venue}")
        print()
    
    if len(papers) > 5:
        print(f"... and {len(papers) - 5} more papers")


def main():
    """Main function to orchestrate the AI paper search."""
    print("🤖 AI Research Paper Finder")
    print("=" * 40)
    
    try:
        # Step 1: Get AI identifiers
        ai_subfield_id = get_ai_identifiers()
        
        # Step 2: Get recent AI papers (filtered by field/subfield)
        papers = get_recent_ai_papers(ai_subfield_id, days=3)
        
        # Step 3: Save to JSON
        if papers:
            file_path = save_papers_to_json(papers)
            print_paper_summary(papers)
        else:
            print("\n❌ No papers found with AI as field/subfield in the last 3 days")
            
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return 1
    
    print(f"\n🎉 Script completed successfully!")
    return 0


if __name__ == "__main__":
    exit(main())
