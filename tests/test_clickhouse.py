#!/usr/bin/env python3
"""
Test script for ClickHouse library - fetch unprocessed records
"""

from libs.clickhouse_lib import ClickHouseClient

def main():
    """Test ClickHouse fetch_unprocessed_records functionality"""
    print("Testing ClickHouse Library - Fetch Unprocessed Records")
    print("=" * 55)
    
    try:
        # Initialize ClickHouse client
        ch_client = ClickHouseClient()
        print(f"Connecting to: {ch_client.host}:{ch_client.port}")
        print(f"Database: {ch_client.ch_database}")
        print(f"User: {ch_client.ch_user}")
        print("-" * 40)
        
        # Test fetching unprocessed records with limit 5
        print("Fetching unprocessed records (limit 5)...")
        unprocessed_records = ch_client.fetch_unprocessed_records(limit=5)
        
        if unprocessed_records is not None:
            print(f"✅ Found {len(unprocessed_records)} unprocessed records:")
            print("-" * 40)
            
            for i, record in enumerate(unprocessed_records, 1):
                record_id, title, ai_response, ai_usage_input, ai_usage_output, is_ai_processed, row_id = record
                print(f"Record #{i}:")
                print(f"  ID: {record_id}")
                print(f"  Title: {title}")
                print(f"  AI Response: {ai_response}")
                print(f"  AI Usage Input: {ai_usage_input}")
                print(f"  AI Usage Output: {ai_usage_output}")
                print(f"  Is AI Processed: {is_ai_processed}")
                print(f"  Row ID: {row_id}")
                print("-" * 20)
        else:
            print("❌ Failed to fetch unprocessed records")
        
        # Close connection
        ch_client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
