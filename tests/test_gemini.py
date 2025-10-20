#!/usr/bin/env python3
"""
Test script for Gemini AI provider
"""

from libs.ai_service_lib import AIService
import json

def main():
    """Test the Gemini AI service with vehicle extraction"""
    print("Testing Gemini AI Service")
    print("=" * 40)
    
    # Test product title
    test_title = "ÖN ÇAMURLUK SOL MAREA LANCİA 0001-7780983"
    
    try:
        # Initialize AI service with gemini provider
        ai_service = AIService("gemini")
        print(f"✅ AI Service initialized with Gemini provider")
        print(f"Testing with product title: {test_title}")
        print("-" * 40)
        
        # Extract vehicle information
        result = ai_service.extract_vehicle_info(test_title, "gemini-2.5-flash-lite")
        
        if result:
            print("✅ Gemini AI processing successful!")
            print(f"Content: {result['content']}")
            print(f"Input tokens: {result['input_tokens']}")
            print(f"Output tokens: {result['output_tokens']}")
            
            # Try to parse the JSON response
            try:
                vehicle_data = json.loads(result['content'])
                print("\n📋 Parsed vehicle information:")
                for vehicle in vehicle_data:
                    print(f"  Brand: {vehicle.get('brand', 'N/A')}")
                    print(f"  Model: {vehicle.get('model', 'N/A')}")
                    print(f"  Submodel: {vehicle.get('submodel', 'N/A')}")
                    print(f"  Category: {vehicle.get('category', 'N/A')}")
                    print(f"  Years: {vehicle.get('years', 'N/A')}")
                    print("-" * 20)
            except json.JSONDecodeError as e:
                print(f"⚠️  Response is not valid JSON: {e}")
                print(f"Raw content: {result['content']}")
                
        else:
            print("❌ Gemini AI processing failed")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
