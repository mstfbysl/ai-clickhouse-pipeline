#!/usr/bin/env python3
"""
Batch Manager for processing ClickHouse records with AI service
"""

import json
import logging
import asyncio
import aiohttp
from datetime import datetime
from libs.clickhouse_lib import ClickHouseClient
from libs.ai_service_lib import AIService
from libs.mongodb_lib import MongoDBClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchManager:
    """Batch manager for processing records from ClickHouse"""
    
    def __init__(self, ai_provider="gemini", ai_model="gemini-2.5-flash-lite"):
        """Initialize batch manager with AI service, ClickHouse client, and MongoDB client"""
        self.ch_client = ClickHouseClient()
        self.ai_service = AIService(ai_provider)
        self.mongo_client = MongoDBClient()
        self.ai_model = ai_model
        
        logger.info(f"BatchManager initialized with provider: {ai_provider}, model: {ai_model}")
    
    def fetch_records(self, total_limit=100):
        """Fetch records from ClickHouse"""
        logger.info(f"Fetching {total_limit} records from ClickHouse")
        # records = self.ch_client.fetch_records(limit=total_limit)
        records = self.ch_client.fetch_records_failed_from_json(start=200, limit=total_limit)

        if records:
            logger.info(f"Successfully fetched {len(records)} records")
            return records
        else:
            logger.warning("No records fetched")
            return []
    
    def create_batches(self, records, batch_size=10):
        """Split records into batches of specified size"""
        batches = []
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            batches.append(batch)
        
        logger.info(f"Created {len(batches)} batches with batch size {batch_size}")
        return batches
    
    async def process_record(self, record):
        """Process a single record with AI service (async)"""
        record_id, title, row_id = record
        
        logger.info(f"Processing record ID: {record_id}")
        
        # Extract vehicle information using AI service (now async)
        result = await self.ai_service.extract_vehicle_info(title, self.ai_model)
        
        if result:
            logger.info(f"Successfully processed record {record_id}")
            
            # Data for MongoDB (includes ai_result as parsed JSON array)
            mongo_data = {
                'record_id': record_id,
                'title': title,
                'success': True,
                'ai_result': result['content'],  # This is now a parsed JSON array
                'input_tokens': result['input_tokens'],
                'output_tokens': result['output_tokens'],
                'processed_at': datetime.now().isoformat(),
                'row_id': row_id
            }
            
            # Data for batch results JSON (excludes ai_result)
            batch_data = {
                'record_id': record_id,
                'title': title,
                'success': True,
                'input_tokens': result['input_tokens'],
                'output_tokens': result['output_tokens'],
                'processed_at': datetime.now().isoformat(),
                'row_id': row_id
            }
            
            # Send to MongoDB
            self.mongo_client.insert_record(mongo_data)
            
            return batch_data
        else:
            logger.error(f"AI processing failed for record {record_id}")
            failed_data = {
                'record_id': record_id,
                'title': title,
                'success': False,
                'error': 'AI processing failed',
                'processed_at': datetime.now().isoformat(),
                'row_id': row_id
            }
            
            # Still send failed records to MongoDB for tracking
            self.mongo_client.insert_record(failed_data)
            
            return failed_data
    
    async def process_batch_async(self, batch, batch_number):
        """Process a batch of records asynchronously with true concurrency"""
        logger.info(f"Processing batch {batch_number} with {len(batch)} records using async concurrency")
        
        # Create async tasks for all records in the batch (true async concurrency)
        tasks = [
            self.process_record(record)
            for record in batch
        ]
        
        # Wait for all tasks to complete concurrently
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle any exceptions that occurred
        processed_results = []
        for i, result in enumerate(batch_results):
            if isinstance(result, Exception):
                record_id, title, row_id = batch[i]
                logger.error(f"Exception processing record {record_id}: {result}")
                # Create failed result for exceptions
                failed_result = {
                    'record_id': record_id,
                    'title': title,
                    'success': False,
                    'error': f'Processing exception: {str(result)}',
                    'processed_at': datetime.now().isoformat(),
                    'row_id': row_id
                }
                processed_results.append(failed_result)
            else:
                processed_results.append(result)
        
        # Count successful and failed results
        successful_count = sum(1 for result in processed_results if result['success'])
        failed_count = len(processed_results) - successful_count
        
        logger.info(f"Batch {batch_number} completed: {successful_count} successful, {failed_count} failed")
        
        return {
            'batch_number': batch_number,
            'total_records': len(batch),
            'successful': successful_count,
            'failed': failed_count,
            'results': processed_results
        }
    
    
    def process_batch(self, batch, batch_number):
        """Process a batch of records (synchronous wrapper)"""
        return asyncio.run(self.process_batch_async(batch, batch_number))
    
    async def run_batch_processing_async(self, total_limit=100, batch_size=10, batch_delay=1.0):
        """Run the complete batch processing workflow sequentially (one batch at a time)"""
        start_time = datetime.now()
        logger.info(f"Starting sequential batch processing: {total_limit} records, batch size {batch_size}")
        logger.info(f"Processing 1 batch at a time with {batch_delay}s delay between batches")
        logger.info(f"Within each batch: {batch_size} concurrent async HTTP requests")
        
        try:
            # Step 1: Fetch records
            records = self.fetch_records(total_limit)

            if not records:
                logger.warning("No records to process")
                return {
                    'success': False,
                    'message': 'No records found to process',
                    'start_time': start_time.isoformat(),
                    'end_time': datetime.now().isoformat()
                }
            
            # Step 2: Create batches
            batches = self.create_batches(records, batch_size)
            
            # Step 3: Process batches sequentially (one at a time)
            logger.info(f"Processing {len(batches)} batches sequentially...")
            
            all_results = []
            
            for i, batch in enumerate(batches, 1):
                logger.info(f"Processing batch {i}/{len(batches)}")
                
                try:
                    # Process current batch
                    batch_result = await self.process_batch_async(batch, i)
                    all_results.append(batch_result)
                    
                    # Log batch completion
                    logger.info(f"Batch {i} completed: {batch_result['successful']} successful, {batch_result['failed']} failed")
                    
                    # Update lastRecord.txt with the last processed RowID from this batch
                    if batch_result['results']:
                        # Get the last RowID from this batch
                        last_row_id = batch_result['results'][-1]['row_id']
                        self.ch_client.update_last_record_id(last_row_id)
                    
                except asyncio.CancelledError:
                    logger.warning("Batch processing was cancelled")
                    self.ch_client.close()
                    return {
                        'success': False,
                        'error': 'Processing was cancelled',
                        'start_time': start_time.isoformat(),
                        'end_time': datetime.now().isoformat()
                    }
                except Exception as e:
                    logger.error(f"Batch {i} processing error: {e}")
                    # Create a failed batch result
                    all_results.append({
                        'batch_number': i,
                        'total_records': len(batch),
                        'successful': 0,
                        'failed': len(batch),
                        'results': [],
                        'error': str(e)
                    })
                
                # Add delay between batches (except for the last batch)
                if i < len(batches):
                    logger.info(f"Waiting {batch_delay}s before processing next batch...")
                    await asyncio.sleep(batch_delay)
            
            # Calculate totals
            total_successful = sum(result.get('successful', 0) for result in all_results)
            total_failed = sum(result.get('failed', 0) for result in all_results)
            
            # Step 4: Close connections
            self.ch_client.close()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Sequential batch processing completed: {total_successful} successful, {total_failed} failed in {duration:.2f} seconds")
            
            return {
                'success': True,
                'total_records_processed': len(records),
                'total_successful': total_successful,
                'total_failed': total_failed,
                'batches_processed': len(batches),
                'processing_mode': 'sequential',
                'batch_delay': batch_delay,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat(),
                'duration_seconds': duration,
                'batch_results': all_results
            }
            
        except KeyboardInterrupt:
            logger.warning("Batch processing interrupted by user")
            self.ch_client.close()
            return {
                'success': False,
                'error': 'Processing interrupted by user',
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
        except Exception as e:
            logger.error(f"Sequential batch processing failed: {e}")
            self.ch_client.close()
            return {
                'success': False,
                'error': str(e),
                'start_time': start_time.isoformat(),
                'end_time': datetime.now().isoformat()
            }
    
    def save_results_to_json(self, results, filename=None):
        """Save batch processing results to JSON file"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"batch_results_{timestamp}.json"
        
        try:
            # Custom JSON encoder to handle datetime objects and BSON ObjectIds
            def json_serializer(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                # Handle BSON ObjectId
                if hasattr(obj, '__class__') and obj.__class__.__name__ == 'ObjectId':
                    return str(obj)
                raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False, default=json_serializer)
            
            logger.info(f"Results saved to {filename}")
            return filename
        except Exception as e:
            logger.error(f"Failed to save results to JSON: {e}")
            return None
    
    def run_batch_processing(self, total_limit=100, batch_size=10, batch_delay=1.0, save_to_json=True):
        """Run the complete batch processing workflow (synchronous wrapper)"""
        result = asyncio.run(self.run_batch_processing_async(total_limit, batch_size, batch_delay))
        
        # Save results to JSON if requested
        if save_to_json and result.get('success'):
            filename = self.save_results_to_json(result)
            if filename:
                result['json_file'] = filename
        
        return result

def main():
    """Demo function for batch manager"""
    print("Batch Manager Demo")
    print("=" * 30)
    
    try:
        # Initialize batch manager
        batch_manager = BatchManager()
        
        # Run batch processing with 20 records, batch size 5
        result = batch_manager.run_batch_processing(total_limit=1000, batch_size=1)
        
        if result['success']:
            print(f"✅ Batch processing completed successfully!")
            print(f"Total records processed: {result['total_records_processed']}")
            print(f"Successful: {result['total_successful']}")
            print(f"Failed: {result['total_failed']}")
            print(f"Batches processed: {result['batches_processed']}")
        else:
            print(f"❌ Batch processing failed: {result.get('error', result.get('message'))}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
