import base64
import os
import csv
import io
from google import genai
from google.genai import types
import pandas as pd
from typing import List, Dict, Any
import re
import random
from datetime import datetime

class GeminiMarkdownToCSVConverter:
    def __init__(self, api_key: str):
        """
        Initialize the converter with Gemini API key
        
        Args:
            api_key (str): Your Google AI API key
        """
        # Normalize to a clean list of non-empty keys. Accepts either a single
        # key string (backward compatible) or a list/tuple of keys for
        # round-robin load-balancing + automatic failover.
        if isinstance(api_key, (list, tuple)):
            self.api_keys = [str(k).strip() for k in api_key if k and str(k).strip()]
        else:
            self.api_keys = [str(api_key).strip()] if api_key and str(api_key).strip() else []

        if not self.api_keys:
            raise ValueError("No Gemini API key provided")

        self.model = "gemini-2.5-flash"
        # One cached client per key (created lazily on first use).
        self._clients = {}
        # Randomized round-robin start so parallel sessions/users don't all
        # hammer the same key first.
        self._rr_index = random.randrange(len(self.api_keys))

    def _get_client(self, api_key: str):
        """Return a cached genai.Client for the given API key."""
        client = self._clients.get(api_key)
        if client is None:
            client = genai.Client(api_key=api_key)
            self._clients[api_key] = client
        return client

    @staticmethod
    def _is_retryable_error(error: Exception) -> bool:
        """True for rate-limit / quota / transient errors that are worth
        retrying on a different API key."""
        msg = str(error).lower()
        markers = [
            "429", "resource_exhausted", "resource exhausted", "quota",
            "rate limit", "rate-limit", "too many requests", "exceeded",
            "503", "unavailable", "overloaded", "500", "internal error",
            "deadline",
        ]
        return any(marker in msg for marker in markers)
        
    def create_prompt(self) -> str:
        """
        Create the system prompt for CSV conversion
        """
        return """Bạn là một chuyên gia xử lý dữ liệu cấm vận. Nhiệm vụ của bạn là trích xuất thông tin từ file markdown chứa danh sách cấm vận và chuyển đổi thành format CSV với các trường sau:

Cấu trúc CSV yêu cầu:
Name: Tên đầy đủ (ký tự Latin, loại bỏ tiền tố pháp lý không cần thiết)
Aliases: Các tên khác, bí danh (phân cách bằng dấu chấm phẩy nếu có nhiều)
Type: Individual/Entity/Vessel/Port/Airport/Airplane
Date of Birth: dd.mm.yyyy hoặc dd/mm/yyyy (chỉ với cá nhân)
Place of Birth: Nơi sinh (chỉ với cá nhân)
Gender: Male/Female/Unknown (chỉ với cá nhân)
Nationality: Quốc tịch
COUNTRY: quốc gia (theo địa chỉ hoặc quốc tịch hoặc ngữ cảnh)
ID_1: Số định danh chính (IMO, Registration Number, INN, Passport, v.v.)
ID_Type1: Loại ID_1
ID_2: Số định danh phụ (nếu có)
ID_Type2: Loại ID_2
Date of listing: Ngày niêm yết (yyyy-mm-dd hoặc dd/mm/yyyy)
Watchlist: Nguồn danh sách là mã REGULATION hoặc DECISION ở đầu văn bản (ví dụ: "2025/1578")
Other info: Thông tin bổ sung (địa chỉ, lý do, ghi chú)
DOB_DJ: Năm sinh hoặc ngày sinh (định dạng 20 Jun 2023 hoặc 2023)
DOB_YEAR: Năm sinh (yyyy)

Quy tắc xử lý:
Làm sạch tên để chỉ chứa ký tự Latin
Xác định Type dựa trên ngữ cảnh (công ty = Entity, tàu = Vessel, v.v.)
Suy ra COUNTRY từ địa chỉ hoặc ngữ cảnh
Trích xuất các số định danh từ văn bản (IMO, INN, Registration Number, v.v.)
Đặt giá trị "None" cho các trường không có thông tin

Hãy trả lời CHÍNH XÁC bằng format CSV, không giải thích thêm."""

    def process_markdown_file(self, markdown_content: str, file_type: str = "text") -> str:
        """
        Process markdown content and convert to CSV
        
        Args:
            markdown_content (str): Content of markdown file
            file_type (str): "text" for plain text or "base64" for base64 encoded
            
        Returns:
            str: CSV formatted string
        """
        try:
            # Prepare content for API
            if file_type == "base64":
                content_part = types.Part.from_bytes(
                    mime_type="text/markdown",
                    data=base64.b64decode(markdown_content)
                )
            else:
                content_part = types.Part.from_text(text=markdown_content)
            
            # Create the conversation
            contents = [
                types.Content(
                    role="user",
                    parts=[
                        content_part,
                        types.Part.from_text(text=self.create_prompt())
                    ],
                ),
            ]
            
            # Configure generation
            generate_content_config = types.GenerateContentConfig(
                thinking_config=types.ThinkingConfig(thinking_budget=-1),
            )
            
            # Generate response, trying each API key in round-robin order and
            # failing over to the next key on rate-limit / quota / transient
            # errors.
            n = len(self.api_keys)
            response = None
            last_error = None
            for attempt in range(n):
                api_key = self.api_keys[(self._rr_index + attempt) % n]
                try:
                    response = self._get_client(api_key).models.generate_content(
                        model=self.model,
                        contents=contents,
                        config=generate_content_config,
                    )
                    # Advance the pointer so the next call starts at the
                    # following key (spreads load across all keys).
                    self._rr_index = (self._rr_index + attempt + 1) % n
                    break
                except Exception as key_error:
                    last_error = key_error
                    if n > 1 and self._is_retryable_error(key_error):
                        continue
                    raise
            if response is None:
                raise Exception(
                    f"All {n} Gemini API key(s) failed (rate-limited/quota). "
                    f"Last error: {last_error}"
                )
            
            return response.text
            
        except Exception as e:
            raise Exception(f"Error processing markdown: {str(e)}")
    
    def clean_csv_response(self, csv_text: str) -> str:
        """
        Clean the CSV response from API
        
        Args:
            csv_text (str): Raw CSV text from API
            
        Returns:
            str: Cleaned CSV text
        """
        # Remove markdown code blocks if present
        csv_text = re.sub(r'```csv\s*\n?', '', csv_text)
        csv_text = re.sub(r'```\s*$', '', csv_text)
        
        # Remove any leading/trailing whitespace
        csv_text = csv_text.strip()
        
        return csv_text
    
    def validate_csv_structure(self, csv_text: str) -> bool:
        """
        Validate if the CSV has the correct structure
        
        Args:
            csv_text (str): CSV text to validate
            
        Returns:
            bool: True if valid structure
        """
        try:
            csv_reader = csv.reader(io.StringIO(csv_text))
            headers = next(csv_reader)
            
            expected_headers = [
                'Name', 'Aliases', 'Type', 'Date of Birth', 'Place of Birth', 
                'Gender', 'Nationality', 'COUNTRY', 'ID_1', 'ID_Type1', 
                'ID_2', 'ID_Type2', 'Date of listing', 'Watchlist', 
                'Other info', 'DOB_DJ', 'DOB_YEAR'
            ]
            
            return len(headers) == len(expected_headers)
            
        except Exception:
            return False
    
    def save_csv_file(self, csv_content: str, output_path: str) -> bool:
        """
        Save CSV content to file
        
        Args:
            csv_content (str): CSV content to save
            output_path (str): Path to save the file
            
        Returns:
            bool: True if successful
        """
        try:
            with open(output_path, 'w', encoding='utf-8', newline='') as f:
                f.write(csv_content)
            return True
        except Exception as e:
            print(f"Error saving file: {str(e)}")
            return False
    
    def convert_markdown_to_csv(self, 
                               markdown_file_path: str = None,
                               markdown_content: str = None,
                               output_path: str = None,
                               is_base64: bool = False) -> Dict[str, Any]:
        """
        Main method to convert markdown to CSV
        
        Args:
            markdown_file_path (str, optional): Path to markdown file
            markdown_content (str, optional): Direct markdown content
            output_path (str, optional): Output CSV file path
            is_base64 (bool): Whether markdown_content is base64 encoded
            
        Returns:
            Dict: Result dictionary with success status and details
        """
        try:
            # Read markdown content
            if markdown_file_path:
                with open(markdown_file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            elif markdown_content:
                content = markdown_content
            else:
                return {
                    'success': False,
                    'error': 'No markdown content provided'
                }
            
            # Process with Gemini API
            print("Processing markdown with Gemini API...")
            file_type = "base64" if is_base64 else "text"
            csv_response = self.process_markdown_file(content, file_type)
            
            # Clean the response
            csv_content = self.clean_csv_response(csv_response)
            
            # Validate structure
            if not self.validate_csv_structure(csv_content):
                print("Warning: CSV structure may not be as expected")
            
            # Save to file if output path provided
            if output_path:
                success = self.save_csv_file(csv_content, output_path)
                if not success:
                    return {
                        'success': False,
                        'error': 'Failed to save CSV file'
                    }
            
            return {
                'success': True,
                'csv_content': csv_content,
                'output_path': output_path,
                'message': 'Conversion completed successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }

def main():
    """
    Example usage of the converter
    """
    # Initialize with your API key
    API_KEY = os.getenv('GEMINI_API_KEY')  # Set your API key as environment variable
    
    if not API_KEY:
        print("Please set GEMINI_API_KEY environment variable")
        return
    
    converter = GeminiMarkdownToCSVConverter(API_KEY)
    
    # Example usage scenarios:
    
    # Scenario 1: Convert from file
    """
    result = converter.convert_markdown_to_csv(
        markdown_file_path='path/to/your/sanctions.md',
        output_path='output/sanctions.csv'
    )
    """
    
    # Scenario 2: Convert from string content
    """
    markdown_content = '''
    # EU Sanctions List
    
    ## Annex IV - Entities
    |Name|Registration Number|Date of listing|
    |---|---|---|
    |Company ABC|123456789|20.7.2025|
    '''
    
    result = converter.convert_markdown_to_csv(
        markdown_content=markdown_content,
        output_path='output/test_sanctions.csv'
    )
    """
    
    # Scenario 3: Convert from base64 content
    """
    base64_content = "base64_encoded_markdown_here"
    result = converter.convert_markdown_to_csv(
        markdown_content=base64_content,
        output_path='output/sanctions_from_base64.csv',
        is_base64=True
    )
    """
    
    # Print result
    print("Example converter initialized successfully!")
    print("Use the methods above to convert your markdown files.")
    print("\nMethods available:")
    print("- convert_markdown_to_csv(): Main conversion method")
    print("- process_markdown_file(): Process with Gemini API")
    print("- validate_csv_structure(): Check CSV format")
    print("- save_csv_file(): Save to file")

if __name__ == "__main__":
    main()
