#!/usr/bin/env python3
"""
Simple MongoDB library for inserting processed records
"""

import logging
import os
from pymongo import MongoClient
from datetime import datetime
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MongoDBClient:
    """Simple MongoDB client for inserting processed records"""
    
    def __init__(self):
        """Initialize MongoDB client using environment variables"""
        # Load environment variables
        load_dotenv()
        
        # Get MongoDB configuration from environment
        self.host = os.getenv('MONGO_HOST')
        self.port = os.getenv('MONGO_PORT')
        self.username = os.getenv('MONGO_USER')
        self.password = os.getenv('MONGO_PASS')
        self.database_name = os.getenv('MONGO_DATABASE')
        self.collection_name = os.getenv('MONGO_COLLECTION')
        
        # Validate required environment variables
        if not all([self.host, self.port, self.username, self.password, self.database_name, self.collection_name]):
            raise ValueError("Missing required MongoDB environment variables")
        
        # URL encode the password to handle special characters
        encoded_password = quote_plus(self.password)
        self.connection_string = f"mongodb://{self.username}:{encoded_password}@{self.host}:{self.port}/?tls=false"
        
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(self.connection_string)
            self.db = self.client[self.database_name]
            self.collection = self.db[self.collection_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {self.database_name}.{self.collection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def insert_record(self, record_data):
        """Insert a single processed record into MongoDB"""
        if self.collection is None:
            if not self.connect():
                return False
        
        try:
            # Create a copy of the data to avoid modifying the original
            mongo_data = record_data.copy()
            
            # Add insertion timestamp as ISO string
            mongo_data['inserted_at'] = datetime.now().isoformat()
            
            # Insert the record
            result = self.collection.insert_one(mongo_data)
            
            if result.inserted_id:
                logger.info(f"Successfully inserted record to MongoDB: {record_data.get('record_id', 'N/A')}")
                return True
            else:
                logger.error(f"Failed to insert record: {record_data.get('record_id', 'N/A')}")
                return False
                
        except Exception as e:
            logger.error(f"Error inserting record to MongoDB: {e}")
            return False
    
    def close(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Demo function to test MongoDB library"""
    print("MongoDB Library Demo")
    print("=" * 30)
    
    try:
        # Create MongoDB client (uses .env credentials)
        mongo_client = MongoDBClient()
        
        # Test connection
        if mongo_client.connect():
            print("✅ Connected to MongoDB successfully")
            
            # Test record insertion
            test_record = {
                'record_id': 'test-123',
                'title': 'Test Record',
                'ai_result': {'test': 'data'},
                'processed_at': datetime.now().isoformat()
            }
            
            if mongo_client.insert_record(test_record):
                print("✅ Test record inserted successfully")
            else:
                print("❌ Failed to insert test record")
        else:
            print("❌ Failed to connect to MongoDB")
        
        # Close connection
        mongo_client.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
