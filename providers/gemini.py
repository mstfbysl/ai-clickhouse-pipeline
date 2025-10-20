#!/usr/bin/env python3
"""
Gemini AI provider implementation
"""

import os
import aiohttp
import asyncio
import json
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class Provider:
    """Gemini AI provider"""
    
    def __init__(self):
        """Initialize Gemini provider"""
        load_dotenv()
        
        self.api_url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-lite:generateContent"
        self.api_key = os.getenv('GEMINI_API_KEY')
        
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY environment variable is required")
        
        self.headers = {
            'Content-Type': 'application/json'
        }
    
    async def process_text(self, text, model="gemini-2.5-flash-lite"):
        """Process text using Gemini API (async)"""
        # Construct the API URL with the API key
        url = f"{self.api_url}?key={self.api_key}"
        
        payload = {
            "contents": [
                {
                    "parts": [
                        {
                            "text": text
                        }
                    ]
                }
            ],
            "generationConfig": {
                "temperature": 0.1,
                "topK": 1,
                "topP": 1,
                "maxOutputTokens": 8192,
                "stopSequences": []
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=self.headers, json=payload) as response:
                    if response.status == 429:
                        logger.warning("Gemini API rate limit exceeded")
                        return None
                    elif response.status >= 400:
                        error_text = await response.text()
                        logger.error(f"Gemini API error {response.status}: {error_text}")
                        return None
                    
                    result = await response.json()
            
            logger.info(f"Gemini API request successful")
            
            # Extract usage information
            usage = result.get('usageMetadata', {})
            input_tokens = usage.get('promptTokenCount', 0)
            output_tokens = usage.get('candidatesTokenCount', 0)
            
            # Extract response content
            candidates = result.get('candidates', [])
            if not candidates:
                logger.error("No candidates found in Gemini response")
                logger.debug(f"Full response: {result}")
                return None
            
            candidate = candidates[0]
            
            # Check for safety ratings or finish reason
            finish_reason = candidate.get('finishReason')
            if finish_reason and finish_reason != 'STOP':
                logger.warning(f"Gemini response finished with reason: {finish_reason}")
                if finish_reason in ['SAFETY', 'RECITATION']:
                    logger.warning("Content was blocked by safety filters")
                    return {
                        'content': [],
                        'input_tokens': input_tokens,
                        'output_tokens': output_tokens
                    }
                elif finish_reason == 'MAX_TOKENS':
                    logger.warning("Response was truncated due to max tokens limit - will attempt to parse partial JSON")
            
            if 'content' not in candidate:
                logger.error("No content field in candidate")
                logger.debug(f"Candidate structure: {candidate}")
                return None
            
            parts = candidate['content'].get('parts', [])
            if not parts or 'text' not in parts[0]:
                logger.error("No text found in content parts")
                logger.debug(f"Parts structure: {parts}")
                return None
            
            content = parts[0]['text']
            logger.debug(f"Raw content received: {repr(content[:100])}...")
            
            # Check if content is empty or whitespace
            if not content or not content.strip():
                logger.warning("Gemini API returned empty or whitespace-only content")
                return {
                    'content': [],
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens
                }
            
            # Clean content if it's wrapped in markdown code blocks
            cleaned_content = self._clean_json_response(content)
            
            # Check if cleaned content is empty
            if not cleaned_content or not cleaned_content.strip():
                logger.warning("Content became empty after cleaning")
                logger.debug(f"Original content: {repr(content)}")
                return {
                    'content': [],
                    'input_tokens': input_tokens,
                    'output_tokens': output_tokens
                }
            
            # Try to parse as JSON array
            try:
                parsed_content = json.loads(cleaned_content)
                if isinstance(parsed_content, list):
                    ai_result = parsed_content
                else:
                    # If not a list, wrap in array or handle as needed
                    ai_result = [parsed_content] if parsed_content else []
                logger.info(f"Successfully parsed JSON with {len(ai_result)} items")
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse AI response as JSON: {e}")
                logger.debug(f"Original content length: {len(content)}")
                logger.debug(f"Original content: {repr(content[:500])}")
                logger.debug(f"Cleaned content length: {len(cleaned_content)}")
                logger.debug(f"Cleaned content: {repr(cleaned_content[:500])}")
                
                # If MAX_TOKENS was hit, try to fix truncated JSON
                if finish_reason == 'MAX_TOKENS':
                    logger.info("Attempting to fix truncated JSON due to MAX_TOKENS")
                    fixed_content = self._fix_truncated_json(cleaned_content)
                    if fixed_content:
                        try:
                            parsed_content = json.loads(fixed_content)
                            if isinstance(parsed_content, list):
                                ai_result = parsed_content
                                logger.info(f"Successfully parsed fixed JSON with {len(ai_result)} items")
                            else:
                                ai_result = []
                        except json.JSONDecodeError:
                            logger.warning("Could not fix truncated JSON")
                            ai_result = []
                    else:
                        ai_result = []
                else:
                    ai_result = []
            
            return {
                'content': ai_result,  # Now returns parsed JSON array
                'input_tokens': input_tokens,
                'output_tokens': output_tokens
            }
            
        except aiohttp.ClientError as e:
            logger.error(f"Gemini API request failed: {e}")
            return None
    
    def _clean_json_response(self, content):
        """Clean JSON response by removing markdown code blocks if present"""
        content = content.strip()
        
        # Check if response is wrapped in markdown code blocks
        if content.startswith('```json'):
            # Find the closing ``` and extract JSON
            end_marker = content.rfind('```')
            if end_marker > 7:  # Make sure we found a closing marker after the opening ```json
                return content[7:end_marker].strip()  # Remove ```json and closing ```
            else:
                # No closing marker found, remove just the opening ```json
                return content[7:].strip()
        elif content.startswith('```'):
            # Handle generic code blocks
            end_marker = content.rfind('```')
            if end_marker > 3:  # Make sure we found a closing marker after the opening ```
                return content[3:end_marker].strip()  # Remove ``` and closing ```
            else:
                # No closing marker found, remove just the opening ```
                return content[3:].strip()
        
        # Return content as is if no markdown blocks found
        return content
    
    def _fix_truncated_json(self, content):
        """Attempt to fix truncated JSON by closing incomplete structures"""
        content = content.strip()
        
        # Count open and close brackets/braces
        open_brackets = content.count('[')
        close_brackets = content.count(']')
        open_braces = content.count('{')
        close_braces = content.count('}')
        
        # If we have unmatched opening brackets/braces, try to close them
        if open_brackets > close_brackets or open_braces > close_braces:
            # Find the last complete object
            last_complete_obj = -1
            brace_count = 0
            bracket_count = 0
            
            for i, char in enumerate(content):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        last_complete_obj = i
                elif char == '[':
                    bracket_count += 1
                elif char == ']':
                    bracket_count -= 1
            
            if last_complete_obj > 0:
                # Truncate to last complete object and close the array
                truncated = content[:last_complete_obj + 1]
                if not truncated.strip().endswith(']'):
                    truncated += ']'
                return truncated
        
        return None
    
    async def extract_vehicle_info(self, product_title, model="gemini-2.5-flash-lite"):
        """Extract vehicle information from product title (async)"""
        prompt = f"""You are a car parts expert. Given the product description below, extract compatible vehicle models and output them in JSON format. EXAMPLE JSON OUTPUT: [ {{ 'brand': 'Ford', 'model': 'Focus', 'submodel': 'IV. Nesil', 'category': 'Silgeç Takımı', 'years': '2018-' }} ] Product: {product_title} Please respond with only the JSON array, no additional text."""
        
        return await self.process_text(prompt, model)
