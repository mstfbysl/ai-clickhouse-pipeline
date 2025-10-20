#!/usr/bin/env python3
"""
AI Service library for working with different AI providers
"""

import os
import importlib
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AIService:
    """AI Service wrapper for different providers"""
    
    def __init__(self, provider_name="gemini"):
        """Initialize AI service with specified provider"""
        load_dotenv()
        
        self.provider_name = provider_name
        self.provider = None
        
        # Load the provider
        self._load_provider()
    
    def _load_provider(self):
        """Load the specified provider module"""
        try:
            provider_module = importlib.import_module(f"providers.{self.provider_name}")
            self.provider = provider_module.Provider()
            logger.info(f"Loaded provider: {self.provider_name}")
        except ImportError as e:
            logger.error(f"Failed to load provider {self.provider_name}: {e}")
            raise
    
    async def process_text(self, text, model="gpt-4o-mini"):
        """Process text using the loaded provider (async)"""
        if not self.provider:
            raise ValueError("No provider loaded")
        
        return await self.provider.process_text(text, model)
    
    async def extract_vehicle_info(self, product_title, model="gpt-4o-mini"):
        """Extract vehicle information from product title (async)"""
        if not self.provider:
            raise ValueError("No provider loaded")
        
        return await self.provider.extract_vehicle_info(product_title, model)
    
    async def close(self):
        """Close any persistent connections in the provider"""
        if self.provider and hasattr(self.provider, 'close'):
            await self.provider.close()

def main():
    """Demo function"""
    print("AI Service Library Demo")
    print("=" * 30)
    
    try:
        ai_service = AIService("gemini")
        print(f"✅ AI Service initialized with provider: gemini")
        
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
