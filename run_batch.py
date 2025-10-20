#!/usr/bin/env python3
"""
Test script for sequential batch processing
"""

from batch_manager import BatchManager

def main():
    """Test sequential batch processing with different configurations"""
    print("Sequential Batch Processing Test")
    print("=" * 40)
    
    try:
        # Initialize batch manager
        batch_manager = BatchManager()
        
        print("Testing sequential processing with:")
        print("-" * 40)
        
        #Run sequential batch processing
        result = batch_manager.run_batch_processing(
            total_limit=100,
            batch_size=50,
            batch_delay=0.5,
            save_to_json=True
        )


        if result['success']:
            print("\n✅ Sequential batch processing completed!")
            print(f"Processing mode: {result.get('processing_mode', 'N/A')}")
            print(f"Total records processed: {result['total_records_processed']}")
            print(f"Successful: {result['total_successful']}")
            print(f"Failed: {result['total_failed']}")
            print(f"Batches processed: {result['batches_processed']}")
            print(f"Batch delay: {result.get('batch_delay', 'N/A')}s")
            print(f"Total duration: {result['duration_seconds']:.2f}s")
            if 'json_file' in result:
                print(f"Results saved to: {result['json_file']}")
        else:
            print(f"\n❌ Sequential batch processing failed!")
            print(f"Error: {result.get('error', result.get('message'))}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
