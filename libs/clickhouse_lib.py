#!/usr/bin/env python3
"""
ClickHouse connection library using environment variables
"""

import os
import clickhouse_connect
from dotenv import load_dotenv
from urllib.parse import urlparse
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ClickHouseClient:
    """Simple ClickHouse client wrapper"""
    
    def __init__(self):
        """Initialize ClickHouse client with environment variables"""
        # Load environment variables
        load_dotenv()
        
        # Get configuration from environment
        self.ch_url = os.getenv('CH_URL')
        self.ch_user = os.getenv('CH_USER')
        self.ch_pass = os.getenv('CH_PASS')
        self.ch_database = os.getenv('CH_DATABASE')
        
        # Validate required environment variables
        if not all([self.ch_url, self.ch_user, self.ch_pass, self.ch_database]):
            raise ValueError("Missing required ClickHouse environment variables")
        
        # Parse URL to extract host
        parsed_url = urlparse(self.ch_url)
        self.host = parsed_url.hostname
        self.port = parsed_url.port or (443 if parsed_url.scheme == 'https' else 8123)
        self.secure = parsed_url.scheme == 'https'
        
        self.client = None
        
    def connect(self):
        """Establish connection to ClickHouse"""
        try:
            self.client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                username=self.ch_user,
                password=self.ch_pass,
                database=self.ch_database,
                secure=self.secure
            )
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            return False
    
    def execute_query(self, query):
        """Execute a query and return results"""
        if not self.client:
            if not self.connect():
                return None
        
        try:
            result = self.client.query(query)
            logger.info(f"Query executed successfully: {query[:50]}...")
            return result.result_rows
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return None
    
    def fetch_records(self, limit=100):
        """Fetch records from records table with limit"""
        if not self.client:
            if not self.connect():
                return None
        
        try:
            # Read last processed RowID from file
            last_row_id = self.get_last_record_id()
            
            query = f"""
            SELECT id, title, RowID
            FROM records 
            WHERE RowID > {last_row_id}
            ORDER BY RowID ASC 
            LIMIT {limit}
            """
            result = self.client.query(query)
            logger.info(f"Fetched {len(result.result_rows)} records starting from RowID: {last_row_id}")
            return result.result_rows
        except Exception as e:
            logger.error(f"Failed to fetch records: {e}")
            return None
    
    def get_last_record_id(self):
        """Get the last processed RowID from lastRecord.txt"""
        try:
            with open('lastRecord.txt', 'r') as f:
                last_row_id = f.read().strip()
                return int(last_row_id) if last_row_id else 0
        except FileNotFoundError:
            # If file doesn't exist, start from 0
            return 0
        except Exception as e:
            logger.error(f"Error reading lastRecord.txt: {e}")
            return 0
    
    def update_last_record_id(self, row_id):
        """Update the last processed RowID in lastRecord.txt"""
        try:
            with open('lastRecord.txt', 'w') as f:
                f.write(str(row_id))
            logger.info(f"Updated lastRecord.txt with RowID: {row_id}")
            return True
        except Exception as e:
            logger.error(f"Error updating lastRecord.txt: {e}")
            return False
    
    def fetch_records_failed_from_json(self, json_file_path="failed_records.json", start=0, limit=100):
        """Fetch records from ClickHouse based on failed records in JSON file

        Args:
            json_file_path (str): Path to the failed records JSON file
            start (int, optional): Starting index in the failed records list. Defaults to 0.
            limit (int, optional): Maximum number of records to fetch. Defaults to 100.
        """
        if not self.client:
            if not self.connect():
                return None
        
        try:
            # Read the failed records JSON file
            import json
            with open(json_file_path, 'r') as f:
                failed_data = json.load(f)
            
            # Extract row_ids from the failed records
            row_ids = [record['row_id'] for record in failed_data['removed_documents']]
            
            if not row_ids:
                logger.info("No failed records found in JSON file")
                return []
            
            # Apply start and limit
            end_index = start + limit if limit is not None else None
            row_ids = row_ids[start:end_index]
            logger.info(f"Fetched failed records from index {start} to {start + len(row_ids)}")
            
            # Create a comma-separated string of row_ids for the SQL IN clause
            row_ids_str = ','.join(map(str, row_ids))
            
            # Query ClickHouse to fetch records based on row_ids
            query = f"""
            SELECT id, title, RowID
            FROM records 
            WHERE RowID IN ({row_ids_str})
            ORDER BY RowID ASC
            """
            
            result = self.client.query(query)
            logger.info(f"Fetched {len(result.result_rows)} failed records from ClickHouse")
            return result.result_rows
            
        except FileNotFoundError:
            logger.error(f"Failed records JSON file not found: {json_file_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON file: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to fetch failed records from ClickHouse: {e}")
            return None
    
    def close(self):
        """Close the connection"""
        if self.client:
            self.client.close()
            logger.info("ClickHouse connection closed")

def main():
    """Demo function to test the ClickHouse library"""
    print("ClickHouse Library Demo")
    print("=" * 30)
    
    try:
        # Create ClickHouse client
        ch_client = ClickHouseClient()
        print(f"Connecting to: {ch_client.host}:{ch_client.port}")
        print(f"Database: {ch_client.ch_database}")
        print(f"User: {ch_client.ch_user}")
        
        # Connect to database
        if ch_client.connect():
            print("✅ Connected successfully")
        else:
            print("❌ Connection failed")
        
        # Close connection
        ch_client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
