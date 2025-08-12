#!/usr/bin/env python3
"""
Markdown to Base64 Converter
Converts markdown files to base64 encoding for API usage
"""

import base64
import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import json
from datetime import datetime

class MarkdownBase64Converter:
    def __init__(self):
        """Initialize the converter"""
        self.supported_extensions = ['.md', '.markdown', '.txt']
    
    def file_to_base64(self, file_path: str, encoding: str = 'utf-8') -> Dict[str, Any]:
        """
        Convert a single file to base64
        
        Args:
            file_path (str): Path to the markdown file
            encoding (str): File encoding, default utf-8
            
        Returns:
            Dict: Result with base64 content and metadata
        """
        try:
            file_path = Path(file_path)
            
            # Check if file exists
            if not file_path.exists():
                return {
                    'success': False,
                    'error': f'File not found: {file_path}',
                    'file_path': str(file_path)
                }
            
            # Check file extension
            if file_path.suffix.lower() not in self.supported_extensions:
                print(f"Warning: {file_path.suffix} is not a typical markdown extension")
            
            # Read file content
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            
            # Convert to base64
            content_bytes = content.encode(encoding)
            base64_content = base64.b64encode(content_bytes).decode('ascii')
            
            # Get file metadata
            stat = file_path.stat()
            
            return {
                'success': True,
                'file_path': str(file_path),
                'file_name': file_path.name,
                'file_size_bytes': stat.st_size,
                'file_size_readable': self._format_file_size(stat.st_size),
                'encoding': encoding,
                'base64_content': base64_content,
                'base64_size': len(base64_content),
                'original_lines': len(content.splitlines()),
                'conversion_time': datetime.now().isoformat()
            }
            
        except UnicodeDecodeError as e:
            return {
                'success': False,
                'error': f'Encoding error: {str(e)}. Try different encoding.',
                'file_path': str(file_path)
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'file_path': str(file_path)
            }
    
    def string_to_base64(self, content: str, encoding: str = 'utf-8') -> Dict[str, Any]:
        """
        Convert string content to base64
        
        Args:
            content (str): String content to convert
            encoding (str): String encoding, default utf-8
            
        Returns:
            Dict: Result with base64 content and metadata
        """
        try:
            # Convert to base64
            content_bytes = content.encode(encoding)
            base64_content = base64.b64encode(content_bytes).decode('ascii')
            
            return {
                'success': True,
                'content_type': 'string',
                'original_size_bytes': len(content_bytes),
                'original_size_readable': self._format_file_size(len(content_bytes)),
                'encoding': encoding,
                'base64_content': base64_content,
                'base64_size': len(base64_content),
                'original_lines': len(content.splitlines()),
                'conversion_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'content_type': 'string'
            }
    
    def base64_to_string(self, base64_content: str, encoding: str = 'utf-8') -> Dict[str, Any]:
        """
        Convert base64 back to string (for verification)
        
        Args:
            base64_content (str): Base64 encoded content
            encoding (str): Target encoding, default utf-8
            
        Returns:
            Dict: Result with decoded content
        """
        try:
            # Decode from base64
            content_bytes = base64.b64decode(base64_content)
            content = content_bytes.decode(encoding)
            
            return {
                'success': True,
                'decoded_content': content,
                'decoded_size_bytes': len(content_bytes),
                'decoded_size_readable': self._format_file_size(len(content_bytes)),
                'encoding': encoding,
                'decoded_lines': len(content.splitlines()),
                'conversion_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def batch_convert_directory(self, 
                               input_dir: str, 
                               output_dir: Optional[str] = None,
                               save_metadata: bool = True) -> Dict[str, Any]:
        """
        Convert all markdown files in a directory to base64
        
        Args:
            input_dir (str): Input directory path
            output_dir (str, optional): Output directory for base64 files
            save_metadata (bool): Whether to save metadata files
            
        Returns:
            Dict: Batch conversion results
        """
        try:
            input_path = Path(input_dir)
            
            if not input_path.exists() or not input_path.is_dir():
                return {
                    'success': False,
                    'error': f'Directory not found: {input_path}'
                }
            
            # Setup output directory
            if output_dir:
                output_path = Path(output_dir)
                output_path.mkdir(parents=True, exist_ok=True)
            else:
                output_path = input_path / 'base64_output'
                output_path.mkdir(exist_ok=True)
            
            # Find all markdown files
            md_files = []
            for ext in self.supported_extensions:
                md_files.extend(input_path.glob(f'*{ext}'))
            
            if not md_files:
                return {
                    'success': False,
                    'error': f'No markdown files found in {input_path}'
                }
            
            results = []
            successful_conversions = 0
            
            for md_file in md_files:
                print(f"Converting {md_file.name}...")
                
                # Convert to base64
                result = self.file_to_base64(str(md_file))
                
                if result['success']:
                    # Save base64 content to file
                    base64_file = output_path / f"{md_file.stem}_base64.txt"
                    with open(base64_file, 'w') as f:
                        f.write(result['base64_content'])
                    
                    # Save metadata if requested
                    if save_metadata:
                        metadata_file = output_path / f"{md_file.stem}_metadata.json"
                        metadata = {k: v for k, v in result.items() if k != 'base64_content'}
                        metadata['base64_file'] = str(base64_file)
                        
                        with open(metadata_file, 'w', encoding='utf-8') as f:
                            json.dump(metadata, f, indent=2, ensure_ascii=False)
                    
                    result['base64_file'] = str(base64_file)
                    successful_conversions += 1
                
                results.append(result)
            
            return {
                'success': True,
                'input_directory': str(input_path),
                'output_directory': str(output_path),
                'total_files': len(md_files),
                'successful_conversions': successful_conversions,
                'failed_conversions': len(md_files) - successful_conversions,
                'results': results,
                'conversion_time': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def save_base64_to_file(self, 
                           base64_content: str, 
                           output_path: str,
                           include_metadata: Dict = None) -> bool:
        """
        Save base64 content to file
        
        Args:
            base64_content (str): Base64 encoded content
            output_path (str): Output file path
            include_metadata (Dict, optional): Additional metadata to save
            
        Returns:
            bool: Success status
        """
        try:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Save base64 content
            with open(output_file, 'w') as f:
                f.write(base64_content)
            
            # Save metadata if provided
            if include_metadata:
                metadata_file = output_file.with_suffix('.json')
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(include_metadata, f, indent=2, ensure_ascii=False)
            
            return True
            
        except Exception as e:
            print(f"Error saving base64 file: {e}")
            return False
    
    def _format_file_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0B"
        
        size_names = ["B", "KB", "MB", "GB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1
        
        return f"{size_bytes:.1f}{size_names[i]}"

def main():
    """Main function with CLI interface"""
    converter = MarkdownBase64Converter()
    
    if len(sys.argv) < 2:
        print("🔧 Markdown to Base64 Converter")
        print("=" * 40)
        print("Usage:")
        print("  python markdown_to_base64.py file <file_path>                    # Convert single file")
        print("  python markdown_to_base64.py string '<content>'                  # Convert string content")
        print("  python markdown_to_base64.py batch <input_dir> [output_dir]     # Batch convert directory")
        print("  python markdown_to_base64.py decode '<base64_content>'          # Decode base64 to verify")
        print("  python markdown_to_base64.py interactive                        # Interactive mode")
        print("\nExample:")
        print("  python markdown_to_base64.py file sanctions.md")
        print("  python markdown_to_base64.py batch ./markdown_files ./base64_output")
        return
    
    command = sys.argv[1].lower()
    
    if command == 'file':
        if len(sys.argv) < 3:
            print("❌ Please provide file path")
            return
        
        file_path = sys.argv[2]
        result = converter.file_to_base64(file_path)
        
        if result['success']:
            print(f"✅ File converted successfully!")
            print(f"📁 Original file: {result['file_name']}")
            print(f"📏 Original size: {result['file_size_readable']}")
            print(f"📏 Base64 size: {result['base64_size']} characters")
            print(f"📄 Lines: {result['original_lines']}")
            
            # Save to file
            output_file = f"{Path(file_path).stem}_base64.txt"
            success = converter.save_base64_to_file(
                result['base64_content'], 
                output_file,
                {k: v for k, v in result.items() if k != 'base64_content'}
            )
            
            if success:
                print(f"💾 Base64 saved to: {output_file}")
            
            # Show preview
            preview_len = min(100, len(result['base64_content']))
            print(f"\n📋 Base64 Preview (first {preview_len} chars):")
            print(result['base64_content'][:preview_len] + "...")
            
        else:
            print(f"❌ Conversion failed: {result['error']}")
    
    elif command == 'string':
        if len(sys.argv) < 3:
            print("❌ Please provide string content")
            return
        
        content = sys.argv[2]
        result = converter.string_to_base64(content)
        
        if result['success']:
            print(f"✅ String converted successfully!")
            print(f"📏 Original size: {result['original_size_readable']}")
            print(f"📏 Base64 size: {result['base64_size']} characters")
            print(f"📄 Lines: {result['original_lines']}")
            print(f"\n📋 Base64 Content:")
            print(result['base64_content'])
        else:
            print(f"❌ Conversion failed: {result['error']}")
    
    elif command == 'batch':
        if len(sys.argv) < 3:
            print("❌ Please provide input directory")
            return
        
        input_dir = sys.argv[2]
        output_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        result = converter.batch_convert_directory(input_dir, output_dir)
        
        if result['success']:
            print(f"✅ Batch conversion completed!")
            print(f"📂 Input directory: {result['input_directory']}")
            print(f"📂 Output directory: {result['output_directory']}")
            print(f"📊 Results: {result['successful_conversions']}/{result['total_files']} files converted")
            
            if result['failed_conversions'] > 0:
                print(f"\n❌ Failed conversions:")
                for res in result['results']:
                    if not res['success']:
                        print(f"   - {res.get('file_path', 'Unknown')}: {res['error']}")
        else:
            print(f"❌ Batch conversion failed: {result['error']}")
    
    elif command == 'decode':
        if len(sys.argv) < 3:
            print("❌ Please provide base64 content")
            return
        
        base64_content = sys.argv[2]
        result = converter.base64_to_string(base64_content)
        
        if result['success']:
            print(f"✅ Base64 decoded successfully!")
            print(f"📏 Decoded size: {result['decoded_size_readable']}")
            print(f"📄 Lines: {result['decoded_lines']}")
            print(f"\n📋 Decoded Content:")
            print("-" * 40)
            print(result['decoded_content'])
            print("-" * 40)
        else:
            print(f"❌ Decoding failed: {result['error']}")
    
    elif command == 'interactive':
        print("🎯 Interactive Markdown to Base64 Converter")
        print("=" * 50)
        
        while True:
            print("\nOptions:")
            print("1. Convert file to base64")
            print("2. Convert text to base64")
            print("3. Decode base64 to text")
            print("4. Batch convert directory")
            print("5. Exit")
            
            choice = input("\nEnter your choice (1-5): ").strip()
            
            if choice == '1':
                file_path = input("Enter markdown file path: ").strip()
                result = converter.file_to_base64(file_path)
                
                if result['success']:
                    print(f"\n✅ Conversion successful!")
                    print(f"Base64 content length: {result['base64_size']} characters")
                    
                    save = input("Save to file? (y/n): ").strip().lower()
                    if save == 'y':
                        output_file = input("Output filename (default: auto): ").strip()
                        if not output_file:
                            output_file = f"{Path(file_path).stem}_base64.txt"
                        
                        converter.save_base64_to_file(result['base64_content'], output_file)
                        print(f"💾 Saved to: {output_file}")
                    
                    show = input("Show base64 content? (y/n): ").strip().lower()
                    if show == 'y':
                        print("\n📋 Base64 Content:")
                        print(result['base64_content'])
                else:
                    print(f"❌ Error: {result['error']}")
            
            elif choice == '2':
                print("Enter markdown content (end with '###END###' on a new line):")
                lines = []
                while True:
                    line = input()
                    if line.strip() == "###END###":
                        break
                    lines.append(line)
                
                content = "\n".join(lines)
                result = converter.string_to_base64(content)
                
                if result['success']:
                    print(f"\n✅ Conversion successful!")
                    print(f"📋 Base64 Content:")
                    print(result['base64_content'])
                else:
                    print(f"❌ Error: {result['error']}")
            
            elif choice == '3':
                base64_content = input("Enter base64 content to decode: ").strip()
                result = converter.base64_to_string(base64_content)
                
                if result['success']:
                    print(f"\n✅ Decoding successful!")
                    print(f"📋 Decoded Content:")
                    print("-" * 40)
                    print(result['decoded_content'])
                    print("-" * 40)
                else:
                    print(f"❌ Error: {result['error']}")
            
            elif choice == '4':
                input_dir = input("Enter input directory path: ").strip()
                output_dir = input("Enter output directory path (optional): ").strip()
                
                if not output_dir:
                    output_dir = None
                
                result = converter.batch_convert_directory(input_dir, output_dir)
                
                if result['success']:
                    print(f"✅ Batch conversion completed!")
                    print(f"Results: {result['successful_conversions']}/{result['total_files']} files converted")
                else:
                    print(f"❌ Error: {result['error']}")
            
            elif choice == '5':
                print("👋 Goodbye!")
                break
            
            else:
                print("❌ Invalid choice! Please enter 1-5.")
    
    else:
        print(f"❌ Unknown command: {command}")
        print("Available commands: file, string, batch, decode, interactive")

if __name__ == "__main__":
    main()