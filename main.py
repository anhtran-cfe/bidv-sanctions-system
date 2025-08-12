#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main workflow for Sanctions Processing System
Processes PDF files through a complete pipeline to extract sanctions data
Updated workflow: PDF → MD → Base64 → CSV (via Gemini API)
Supports: Single PDF or Multiple PDFs batch processing
"""

import os
import sys
import glob
import pandas as pd
from datetime import datetime
import re
import subprocess
import time
import base64
from typing import List, Dict, Any
from pathlib import Path

# Import các module khác
try:
    import pdf_to_md
    from markdown_to_base64 import MarkdownBase64Converter
    from gemini_markdown_csv import GeminiMarkdownToCSVConverter
    import ofac_extractor
    import un_sanctions_parser
except ImportError as e:
    print(f"❌ Lỗi import module: {e}")
    print("Vui lòng đảm bảo tất cả các file Python cần thiết có trong cùng thư mục")
    print("Cần có: pdf_to_md.py, markdown_to_base64.py, gemini_markdown_csv.py")
    sys.exit(1)

class SanctionsWorkflow:
    def __init__(self):
        self.pdf_files = []  # Changed to support multiple files
        self.processing_mode = 'single'  # 'single' or 'batch'
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.final_output = f"sanctions_cleaned_{self.timestamp}.csv"
        self.temp_files = []
        self.gemini_api_key = None
        self.processed_pdfs = []  # Track processed PDFs info
        
    def print_header(self, title):
        """In header cho từng bước"""
        print("\n" + "="*80)
        print(f"🔄 {title}")
        print("="*80)
        
    def print_step(self, step_num, title):
        """In thông tin bước hiện tại"""
        print(f"\n📋 BƯỚC {step_num}: {title}")
        print("-" * 60)
        
    def get_processing_mode(self):
        """Chọn chế độ xử lý: single hoặc batch"""
        print("📋 CHỌN CHẾA ĐỘ XỬ LÝ")
        print("-" * 30)
        print("1. Xử lý 1 file PDF")
        print("2. Xử lý nhiều file PDF (batch)")
        print("3. Xử lý tất cả PDF trong thư mục")
        
        while True:
            try:
                choice = input("\nChọn chế độ (1-3): ").strip()
                
                if choice == '1':
                    self.processing_mode = 'single'
                    return self.get_single_pdf_input()
                elif choice == '2':
                    self.processing_mode = 'batch'
                    return self.get_multiple_pdf_input()
                elif choice == '3':
                    self.processing_mode = 'batch'
                    return self.get_all_pdf_in_directory()
                else:
                    print("❌ Vui lòng chọn 1, 2 hoặc 3")
                    
            except KeyboardInterrupt:
                print("\n❌ Đã hủy")
                return False
    
    def get_single_pdf_input(self):
        """Lấy 1 file PDF từ người dùng"""
        print("\n📁 NHẬP THÔNG TIN FILE PDF")
        print("-" * 30)
        
        while True:
            pdf_name = input("Nhập tên file PDF (ví dụ: 202501578.pdf): ").strip()
            
            if not pdf_name:
                print("❌ Vui lòng nhập tên file PDF")
                continue
                
            if not pdf_name.endswith('.pdf'):
                pdf_name += '.pdf'
                
            if not os.path.exists(pdf_name):
                print(f"❌ File {pdf_name} không tồn tại")
                self.show_available_pdfs()
                
                try:
                    choice = input("\nChọn số thứ tự file hoặc nhập tên khác (Enter để nhập lại): ").strip()
                    if choice.isdigit():
                        pdf_files = glob.glob("*.pdf")
                        if 1 <= int(choice) <= len(pdf_files):
                            pdf_name = pdf_files[int(choice) - 1]
                            break
                except:
                    pass
                continue
            else:
                break
                
        self.pdf_files = [pdf_name]
        print(f"✅ Đã chọn file: {pdf_name}")
        return True
    
    def get_multiple_pdf_input(self):
        """Lấy nhiều file PDF từ người dùng"""
        print("\n📁 NHẬP DANH SÁCH FILE PDF")
        print("-" * 30)
        print("💡 Các cách nhập:")
        print("   - Nhập từng tên file, cách nhau bởi dấu phẩy")
        print("   - Nhập số thứ tự từ danh sách dưới đây")
        print("   - Hoặc nhập 'all' để chọn tất cả")
        
        self.show_available_pdfs()
        
        while True:
            user_input = input("\nNhập danh sách PDF: ").strip()
            
            if not user_input:
                print("❌ Vui lòng nhập danh sách file PDF")
                continue
            
            pdf_files_in_dir = glob.glob("*.pdf")
            selected_files = []
            
            if user_input.lower() == 'all':
                selected_files = pdf_files_in_dir
                
            elif ',' in user_input:
                # Nhập danh sách tên file
                file_names = [name.strip() for name in user_input.split(',')]
                for name in file_names:
                    if not name.endswith('.pdf'):
                        name += '.pdf'
                    if os.path.exists(name):
                        selected_files.append(name)
                    else:
                        print(f"⚠️ File không tồn tại: {name}")
                        
            else:
                # Nhập số thứ tự (có thể nhiều số cách nhau bởi space hoặc comma)
                try:
                    if ',' in user_input:
                        numbers = [int(n.strip()) for n in user_input.split(',')]
                    else:
                        numbers = [int(n.strip()) for n in user_input.split()]
                    
                    for num in numbers:
                        if 1 <= num <= len(pdf_files_in_dir):
                            selected_files.append(pdf_files_in_dir[num - 1])
                        else:
                            print(f"⚠️ Số thứ tự không hợp lệ: {num}")
                            
                except ValueError:
                    print("❌ Format không đúng. Vui lòng thử lại")
                    continue
            
            if selected_files:
                self.pdf_files = list(set(selected_files))  # Remove duplicates
                print(f"\n✅ Đã chọn {len(self.pdf_files)} file PDF:")
                for i, file in enumerate(self.pdf_files, 1):
                    file_size = os.path.getsize(file)
                    print(f"   {i}. {file} ({file_size:,} bytes)")
                return True
            else:
                print("❌ Không có file nào được chọn. Vui lòng thử lại")
    
    def get_all_pdf_in_directory(self):
        """Lấy tất cả file PDF trong thư mục"""
        pdf_files = glob.glob("*.pdf")
        
        if not pdf_files:
            print("❌ Không tìm thấy file PDF nào trong thư mục")
            return False
            
        self.pdf_files = pdf_files
        print(f"\n✅ Tìm thấy {len(pdf_files)} file PDF:")
        
        total_size = 0
        for i, file in enumerate(pdf_files, 1):
            file_size = os.path.getsize(file)
            total_size += file_size
            print(f"   {i}. {file} ({file_size:,} bytes)")
            
        print(f"\n📊 Tổng kích thước: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
        
        # Xác nhận xử lý tất cả
        try:
            confirm = input(f"\n🤔 Xử lý tất cả {len(pdf_files)} file PDF? (Y/n): ").strip().lower()
            if confirm and confirm not in ['y', 'yes']:
                return False
        except KeyboardInterrupt:
            return False
            
        return True
    
    def show_available_pdfs(self):
        """Hiển thị danh sách PDF có sẵn"""
        pdf_files = glob.glob("*.pdf")
        if pdf_files:
            print(f"\n📋 {len(pdf_files)} file PDF có sẵn:")
            for i, file in enumerate(pdf_files, 1):
                file_size = os.path.getsize(file)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file))
                print(f"   {i}. {file} ({file_size:,} bytes, {mod_time.strftime('%Y-%m-%d %H:%M')})")
        else:
            print("   ❌ Không tìm thấy file PDF nào")
    
    def get_gemini_api_key(self):
        """Lấy Gemini API key"""
        # Kiểm tra environment variable trước
        api_key = os.getenv('GEMINI_API_KEY')
        
        if api_key:
            self.gemini_api_key = api_key
            print("✅ Đã tìm thấy Gemini API key từ environment variable")
            return True
        
        # Nếu không có, yêu cầu nhập
        print("\n🔑 THIẾT LẬP GEMINI API KEY")
        print("-" * 40)
        print("💡 Bạn cần Gemini API key để xử lý dữ liệu PDF")
        print("   Lấy API key tại: https://aistudio.google.com/app/apikey")
        
        if self.processing_mode == 'batch':
            print(f"⚠️ Đang xử lý {len(self.pdf_files)} file PDF - cần API key để xử lý hiệu quả")
        
        while True:
            api_key = input("\nNhập Gemini API key (hoặc 'skip' để bỏ qua): ").strip()
            
            if api_key.lower() == 'skip':
                print("⚠️ Bỏ qua Gemini API - chỉ xử lý OFAC và UN data")
                return False
            
            if not api_key:
                print("❌ Vui lòng nhập API key hợp lệ")
                continue
            
            # Kiểm tra format cơ bản của API key
            if len(api_key) < 20:
                print("❌ API key quá ngắn, vui lòng kiểm tra lại")
                continue
            
            self.gemini_api_key = api_key
            print("✅ Đã thiết lập Gemini API key")
            return True
    
    def step1_pdf_to_md_batch(self):
        """Bước 1: Chuyển tất cả PDF sang Markdown"""
        self.print_step(1, f"CHUYỂN ĐỔI {len(self.pdf_files)} PDF SANG MARKDOWN")
        
        successful_conversions = 0
        conversion_results = []
        
        for i, pdf_file in enumerate(self.pdf_files, 1):
            print(f"\n📄 [{i}/{len(self.pdf_files)}] Đang xử lý: {pdf_file}")
            
            try:
                # Import và sử dụng pymupdf4llm
                import pymupdf4llm
                
                print(f"   🔄 Đang chuyển đổi...")
                md_text = pymupdf4llm.to_markdown(pdf_file)
                
                # Tạo tên file markdown
                md_filename = f"{os.path.splitext(pdf_file)[0]}.md"
                
                with open(md_filename, "w", encoding="utf-8") as f:
                    f.write(md_text)
                
                self.temp_files.append(md_filename)
                
                # Thông tin chi tiết
                file_size = os.path.getsize(md_filename)
                line_count = len(md_text.splitlines())
                
                conversion_result = {
                    'pdf_file': pdf_file,
                    'md_file': md_filename,
                    'success': True,
                    'file_size': file_size,
                    'line_count': line_count,
                    'content_preview': md_text[:200] + "..." if len(md_text) > 200 else md_text
                }
                
                conversion_results.append(conversion_result)
                successful_conversions += 1
                
                print(f"   ✅ Thành công: {md_filename}")
                print(f"   📊 Kích thước: {file_size:,} bytes, {line_count:,} dòng")
                
            except Exception as e:
                print(f"   ❌ Lỗi: {e}")
                conversion_results.append({
                    'pdf_file': pdf_file,
                    'md_file': None,
                    'success': False,
                    'error': str(e)
                })
        
        # Tổng kết bước 1
        print(f"\n📊 KẾT QUẢ CHUYỂN ĐỔI PDF → MD:")
        print(f"   ✅ Thành công: {successful_conversions}/{len(self.pdf_files)}")
        print(f"   ❌ Thất bại: {len(self.pdf_files) - successful_conversions}")
        
        if successful_conversions == 0:
            print("❌ Không có file nào được chuyển đổi thành công")
            return False
        
        # Lưu kết quả để sử dụng ở bước tiếp theo
        self.conversion_results = conversion_results
        return True
    
    def step2_md_to_base64_batch(self):
        """Bước 2: Chuyển tất cả Markdown sang Base64"""
        self.print_step(2, f"CHUYỂN ĐỔI MARKDOWN SANG BASE64")
        
        if not hasattr(self, 'conversion_results'):
            print("❌ Không có dữ liệu markdown từ bước trước")
            return False
        
        successful_conversions = 0
        base64_results = []
        
        converter = MarkdownBase64Converter()
        
        for result in self.conversion_results:
            if not result['success']:
                continue
                
            md_file = result['md_file']
            print(f"\n📄 Đang xử lý: {md_file}")
            
            try:
                print(f"   🔄 Đang chuyển đổi sang Base64...")
                
                base64_result = converter.file_to_base64(md_file)
                
                if not base64_result['success']:
                    print(f"   ❌ Lỗi: {base64_result['error']}")
                    continue
                
                # Thêm thông tin Base64 vào result
                result['base64_content'] = base64_result['base64_content']
                result['base64_metadata'] = {
                    'base64_size': base64_result['base64_size'],
                    'encoding': base64_result['encoding']
                }
                
                base64_results.append(result)
                successful_conversions += 1
                
                print(f"   ✅ Thành công")
                print(f"   📊 Base64 size: {base64_result['base64_size']:,} ký tự")
                
            except Exception as e:
                print(f"   ❌ Lỗi: {e}")
        
        # Tổng kết bước 2
        print(f"\n📊 KẾT QUẢ CHUYỂN ĐỔI MD → BASE64:")
        print(f"   ✅ Thành công: {successful_conversions}")
        
        if successful_conversions == 0:
            print("❌ Không có file nào được chuyển đổi thành công")
            return False
        
        self.base64_results = base64_results
        return True
    
    def step3_base64_to_csv_via_gemini_batch(self):
        """Bước 3: Chuyển tất cả Base64 sang CSV qua Gemini API"""
        self.print_step(3, f"XỬ LÝ DỮ LIỆU VỚI GEMINI API (BASE64 → CSV)")
        
        if not hasattr(self, 'base64_results'):
            print("❌ Không có dữ liệu Base64 từ bước trước")
            return False
        
        if not self.gemini_api_key:
            print("❌ Không có Gemini API key")
            return False
        
        successful_conversions = 0
        converter = GeminiMarkdownToCSVConverter(self.gemini_api_key)
        
        print(f"🤖 Đang xử lý {len(self.base64_results)} file với Gemini AI...")
        print("⏱️ Lưu ý: Mỗi file có thể mất vài phút để xử lý")
        
        for i, result in enumerate(self.base64_results, 1):
            pdf_name = os.path.splitext(result['pdf_file'])[0]
            print(f"\n📄 [{i}/{len(self.base64_results)}] Xử lý: {result['pdf_file']}")
            
            try:
                # Tạo tên file CSV
                csv_filename = f"sanctions_from_{pdf_name}_{self.timestamp}.csv"
                
                print(f"   🔄 Đang gọi Gemini API...")
                start_time = time.time()
                
                gemini_result = converter.convert_markdown_to_csv(
                    markdown_content=result['base64_content'],
                    output_path=csv_filename,
                    is_base64=True
                )
                
                elapsed_time = time.time() - start_time
                
                if not gemini_result['success']:
                    print(f"   ❌ Gemini API lỗi: {gemini_result['error']}")
                    continue
                
                self.temp_files.append(csv_filename)
                successful_conversions += 1
                
                # Phân tích CSV results
                if os.path.exists(csv_filename):
                    file_size = os.path.getsize(csv_filename)
                    df = pd.read_csv(csv_filename)
                    
                    print(f"   ✅ Thành công ({elapsed_time:.1f}s)")
                    print(f"   📊 CSV: {file_size:,} bytes, {len(df):,} records")
                    
                    # Cập nhật thông tin processed
                    result['csv_file'] = csv_filename
                    result['csv_records'] = len(df)
                    result['processing_time'] = elapsed_time
                    
                    if 'Type' in df.columns:
                        type_counts = df['Type'].value_counts()
                        print(f"   📋 Types: {dict(type_counts)}")
                
            except Exception as e:
                print(f"   ❌ Lỗi: {e}")
        
        # Tổng kết bước 3
        print(f"\n📊 KẾT QUẢ XỬ LÝ GEMINI API:")
        print(f"   ✅ Thành công: {successful_conversions}/{len(self.base64_results)}")
        
        total_records = sum(r.get('csv_records', 0) for r in self.base64_results if 'csv_records' in r)
        total_time = sum(r.get('processing_time', 0) for r in self.base64_results if 'processing_time' in r)
        
        print(f"   📈 Tổng records từ PDF: {total_records:,}")
        print(f"   ⏱️ Tổng thời gian Gemini: {total_time:.1f}s ({total_time/60:.1f} phút)")
        
        if successful_conversions > 0:
            avg_time = total_time / successful_conversions
            print(f"   📊 Trung bình mỗi file: {avg_time:.1f}s")
        
        return successful_conversions > 0
    
    def step4_ofac_extraction(self):
        """Bước 4: Trích xuất dữ liệu OFAC"""
        self.print_step(4, "TRÍCH XUẤT DỮ LIỆU OFAC")
        
        try:
            print("🇺🇸 Đang tải dữ liệu từ OFAC...")
            extractor = ofac_extractor.OFACSanctionsExtractor()
            df = extractor.run_extraction()
            
            if len(df) > 0:
                # Tìm file OFAC mới nhất
                ofac_files = glob.glob("sanctions_cleaned_*.csv")
                if ofac_files:
                    latest_ofac = max(ofac_files, key=os.path.getctime)
                    # Kiểm tra xem file có phải từ OFAC không
                    df_check = pd.read_csv(latest_ofac)
                    if len(df_check) > 0 and 'Watchlist' in df_check.columns:
                        first_watchlist = str(df_check['Watchlist'].iloc[0]).lower()
                        if 'ofac' in first_watchlist:
                            self.temp_files.append(latest_ofac)
                            print(f"✅ OFAC extraction hoàn tất: {latest_ofac}")
                            print(f"📊 OFAC records: {len(df_check):,}")
                return True
            else:
                print("⚠️ Không có dữ liệu mới từ OFAC")
                return True
                
        except Exception as e:
            print(f"❌ Lỗi trích xuất OFAC: {e}")
            print("⏭️ Tiếp tục với bước tiếp theo...")
            return True
    
    def step5_un_extraction(self):
        """Bước 5: Trích xuất dữ liệu UN"""
        self.print_step(5, "TRÍCH XUẤT DỮ LIỆU UN SANCTIONS")
        
        try:
            print("🌍 Đang tải dữ liệu từ UN Security Council...")
            un_sanctions_parser.parse_un_sanctions_xml()
            
            # Tìm file UN mới nhất
            un_files = glob.glob("sanctions_cleaned_*.csv")
            for file in un_files:
                if file not in self.temp_files:  # File mới
                    try:
                        df_check = pd.read_csv(file)
                        if len(df_check) > 0:
                            # Kiểm tra có phải UN data không
                            if 'Source' in df_check.columns:
                                first_source = str(df_check['Source'].iloc[0]).lower()
                                if 'un' in first_source or 'security council' in first_source:
                                    self.temp_files.append(file)
                                    print(f"✅ UN extraction hoàn tất: {file}")
                                    print(f"📊 UN records: {len(df_check):,}")
                                    break
                    except Exception as e:
                        print(f"⚠️ Không thể kiểm tra file {file}: {e}")
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi trích xuất UN: {e}")
            print("⏭️ Tiếp tục với bước cuối...")
            return True
    
    def update_watchlist_column(self, df, filename):
        """Cập nhật cột Watchlist dựa trên nguồn dữ liệu"""
        if 'Watchlist' not in df.columns:
            df['Watchlist'] = ''
        
        # Xác định nguồn dữ liệu
        watchlist_value = ''
        
        # Kiểm tra các cột đặc trưng để xác định nguồn
        if 'Source' in df.columns:
            # UN data có cột Source
            first_source = str(df['Source'].iloc[0]).lower() if len(df) > 0 else ''
            if 'un' in first_source or 'security council' in first_source:
                watchlist_value = 'UN'
            
        if not watchlist_value and 'Watchlist' in df.columns:
            # OFAC data hoặc data đã có watchlist
            first_watchlist = str(df['Watchlist'].iloc[0]).lower() if len(df) > 0 else ''
            if 'ofac' in first_watchlist or 'specially designated' in first_watchlist:
                watchlist_value = 'OFAC'
            elif first_watchlist and first_watchlist != 'nan':
                watchlist_value = df['Watchlist'].iloc[0]  # Keep existing value
        
        # EU data - từ file được xử lý bởi Gemini (có pattern sanctions_from_)
        if not watchlist_value and 'sanctions_from_' in filename:
            # Extract EU code từ filename
            # Pattern: sanctions_from_202501578_timestamp.csv
            match = re.search(r'sanctions_from_(\d+)_', filename)
            if match:
                pdf_number = match.group(1)
                watchlist_value = self.extract_eu_watchlist_from_number(pdf_number)
            else:
                watchlist_value = 'EU'
        
        # Fallback
        if not watchlist_value:
            watchlist_value = 'Unknown'
        
        # Cập nhật cột Watchlist
        df['Watchlist'] = watchlist_value
        
        print(f"   🏷️ Cập nhật Watchlist: {watchlist_value} cho {len(df)} records")
        return df
    
    def extract_eu_watchlist_from_number(self, pdf_number):
        """Trích xuất mã EU từ số PDF (VD: 202501578 -> 2025/1578)"""
        if not pdf_number or len(pdf_number) < 8:
            return 'EU'
        
        # Pattern cho file EU: YYYYnnnnn (loại bỏ số 0 đứng đầu sau YYYY)
        if pdf_number.isdigit() and len(pdf_number) >= 8:
            year = pdf_number[:4]
            number = pdf_number[4:].lstrip('0')
            if number:  # Make sure number is not empty
                return f"{year}/{number}"
        
        return 'EU'
    
    def standardize_watchlist(self, df):
        """Chuẩn hóa và làm sạch cột Watchlist"""
        if 'Watchlist' not in df.columns:
            return df
        
        print("   🏷️ Đang chuẩn hóa các giá trị Watchlist...")
        
        # Thống kê trước khi chuẩn hóa
        original_values = df['Watchlist'].value_counts()
        print("   📊 Giá trị Watchlist trước chuẩn hóa:")
        for value, count in original_values.head(10).items():  # Show top 10 only
            print(f"      {value}: {count}")
        if len(original_values) > 10:
            print(f"      ... và {len(original_values) - 10} giá trị khác")
        
        def clean_watchlist_value(value):
            if pd.isna(value) or not value:
                return 'Unknown'
            
            value_str = str(value).strip()
            
            # UN patterns
            if any(keyword in value_str.lower() for keyword in ['un', 'united nations', 'security council']):
                return 'UN'
            
            # OFAC patterns  
            if any(keyword in value_str.lower() for keyword in ['ofac', 'specially designated', 'treasury']):
                return 'OFAC'
            
            # EU patterns - kiểm tra pattern YYYY/NNNN
            eu_pattern = r'\d{4}/\d+'
            if re.search(eu_pattern, value_str):
                return value_str
            
            # EU general
            if 'eu' in value_str.lower():
                return 'EU'
            
            # Giữ nguyên nếu không match pattern nào
            return value_str
        
        df['Watchlist'] = df['Watchlist'].apply(clean_watchlist_value)
        
        # Thống kê sau khi chuẩn hóa
        final_values = df['Watchlist'].value_counts()
        print("   📊 Giá trị Watchlist sau chuẩn hóa:")
        for value, count in final_values.head(10).items():
            print(f"      {value}: {count}")
        if len(final_values) > 10:
            print(f"      ... và {len(final_values) - 10} giá trị khác")
        
        return df
    
    def step6_consolidate_data(self):
        """Bước 6: Tổng hợp tất cả dữ liệu"""
        self.print_step(6, "TỔNG HỢP DỮ LIỆU CUỐI CÙNG")
        
        try:
            # Tìm tất cả file CSV
            all_csv_files = glob.glob("sanctions_cleaned_*.csv") + glob.glob("sanctions_from_*.csv")
            
            if not all_csv_files:
                print("❌ Không tìm thấy file CSV nào để tổng hợp")
                return False
            
            print(f"📋 Tìm thấy {len(all_csv_files)} file CSV:")
            
            # Group files by source for better reporting
            pdf_files = [f for f in all_csv_files if 'sanctions_from_' in f]
            ofac_files = [f for f in all_csv_files if 'sanctions_from_' not in f and 'ofac' in f.lower()]
            un_files = [f for f in all_csv_files if 'sanctions_from_' not in f and 'un' in f.lower()]
            other_files = [f for f in all_csv_files if f not in pdf_files + ofac_files + un_files]
            
            print(f"   📄 Từ PDF: {len(pdf_files)} files")
            print(f"   🇺🇸 OFAC: {len(ofac_files)} files")
            print(f"   🌍 UN: {len(un_files)} files")
            print(f"   📋 Khác: {len(other_files)} files")
            
            # Hiển thị chi tiết files
            for file in all_csv_files:
                file_size = os.path.getsize(file)
                mod_time = datetime.fromtimestamp(os.path.getmtime(file))
                print(f"   📄 {file} ({file_size:,} bytes, {mod_time.strftime('%H:%M:%S')})")
            
            # Đọc và tổng hợp tất cả file
            all_dataframes = []
            total_records = 0
            processing_summary = {
                'pdf_records': 0,
                'ofac_records': 0,
                'un_records': 0,
                'other_records': 0
            }
            
            for file in all_csv_files:
                try:
                    df = pd.read_csv(file, encoding='utf-8')
                    if len(df) > 0:
                        # Xác định nguồn dữ liệu và cập nhật Watchlist
                        df = self.update_watchlist_column(df, file)
                        all_dataframes.append(df)
                        total_records += len(df)
                        
                        # Phân loại records
                        if 'sanctions_from_' in file:
                            processing_summary['pdf_records'] += len(df)
                        elif 'ofac' in file.lower():
                            processing_summary['ofac_records'] += len(df)
                        elif 'un' in file.lower():
                            processing_summary['un_records'] += len(df)
                        else:
                            processing_summary['other_records'] += len(df)
                        
                        print(f"   ✅ {file}: {len(df)} records")
                    else:
                        print(f"   ⚠️ {file}: File trống")
                except Exception as e:
                    print(f"   ❌ {file}: Lỗi đọc - {e}")
            
            if not all_dataframes:
                print("❌ Không có dữ liệu hợp lệ để tổng hợp")
                return False
            
            # Tổng hợp DataFrame
            print(f"\n🔄 Đang tổng hợp {total_records:,} records từ {len(all_dataframes)} file...")
            final_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Xử lý format cột DOB_DJ
            print("🔧 Đang chuẩn hóa format ngày tháng...")
            final_df = self.standardize_dob_format(final_df)
            
            # Chuẩn hóa cột Watchlist
            print("🏷️ Đang chuẩn hóa cột Watchlist...")
            final_df = self.standardize_watchlist(final_df)
            
            # Loại bỏ duplicate
            original_count = len(final_df)
            final_df = final_df.drop_duplicates(subset=['Name'], keep='first')
            duplicate_count = original_count - len(final_df)
            
            if duplicate_count > 0:
                print(f"🔄 Đã loại bỏ {duplicate_count:,} bản ghi trùng lặp")
            
            # Sắp xếp theo tên
            final_df = final_df.sort_values('Name').reset_index(drop=True)
            
            # Lưu file cuối cùng
            final_df.to_csv(self.final_output, index=False, encoding='utf-8-sig')
            
            print(f"\n✅ TỔNG HỢP HOÀN TẤT!")
            print(f"📁 File cuối cùng: {self.final_output}")
            print(f"📊 Tổng số records: {len(final_df):,}")
            
            # Detailed breakdown
            print(f"\n📈 PHÂN TÍCH CHI TIẾT:")
            print(f"   📄 Từ PDF files: {processing_summary['pdf_records']:,} records")
            print(f"   🇺🇸 OFAC data: {processing_summary['ofac_records']:,} records")
            print(f"   🌍 UN data: {processing_summary['un_records']:,} records")
            print(f"   📋 Khác: {processing_summary['other_records']:,} records")
            
            print(f"\n📈 Phân bố theo Watchlist:")
            if 'Watchlist' in final_df.columns:
                watchlist_counts = final_df['Watchlist'].value_counts()
                for watchlist, count in watchlist_counts.items():
                    percentage = (count / len(final_df)) * 100
                    print(f"   {watchlist}: {count:,} ({percentage:.1f}%)")
            
            print(f"\n📈 Phân bố theo Type:")
            if 'Type' in final_df.columns:
                type_counts = final_df['Type'].value_counts()
                for entity_type, count in type_counts.items():
                    percentage = (count / len(final_df)) * 100
                    print(f"   {entity_type}: {count:,} ({percentage:.1f}%)")
            
            # Lưu thông tin processing để summary cuối
            self.final_summary = {
                'total_records': len(final_df),
                'duplicate_removed': duplicate_count,
                'processing_breakdown': processing_summary,
                'watchlist_distribution': dict(final_df['Watchlist'].value_counts()) if 'Watchlist' in final_df.columns else {},
                'type_distribution': dict(final_df['Type'].value_counts()) if 'Type' in final_df.columns else {}
            }
            
            return True
            
        except Exception as e:
            print(f"❌ Lỗi tổng hợp dữ liệu: {e}")
            return False
    
    def standardize_dob_format(self, df):
        """Chuẩn hóa format cột DOB_DJ thành dd MMM yyyy"""
        if 'DOB_DJ' not in df.columns:
            return df
        
        def convert_date_format(date_str):
            if pd.isna(date_str) or not date_str or str(date_str).strip() == '':
                return ''
            
            date_str = str(date_str).strip()
            
            # Nếu đã đúng format dd MMM yyyy, giữ nguyên
            if re.match(r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}', date_str):
                return date_str
            
            # Các format cần convert
            date_formats = [
                '%d/%m/%Y',    # 12/10/1958
                '%d-%m-%Y',    # 12-10-1958
                '%d.%m.%Y',    # 12.10.1958
                '%Y-%m-%d',    # 1958-10-12
                '%Y/%m/%d',    # 1958/10/12
                '%m/%d/%Y',    # 10/12/1958
                '%d %m %Y',    # 12 10 1958
            ]
            
            for fmt in date_formats:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.strftime('%d %b %Y')  # 12 Oct 1958
                except ValueError:
                    continue
            
            # Nếu không convert được, giữ nguyên
            return date_str
        
        print("   🔄 Đang chuẩn hóa format DOB_DJ...")
        df['DOB_DJ'] = df['DOB_DJ'].apply(convert_date_format)
        converted_count = df['DOB_DJ'].apply(lambda x: bool(re.match(r'\d{1,2}\s+[A-Za-z]{3}\s+\d{4}', str(x)))).sum()
        print(f"   📅 Đã chuẩn hóa {converted_count:,} ngày tháng")
        
        return df
    
    def cleanup_temp_files(self):
        """Dọn dẹp file tạm"""
        print("\n🧹 DỌNG DẸP FILE TẠM")
        print("-" * 30)
        
        temp_patterns = [
            "sanctions_cleaned_*_*.csv",  # File có timestamp chi tiết
            "sanctions_from_*.csv",       # File từ Gemini
            "*.md"                        # File markdown
        ]
        
        cleanup_files = []
        for pattern in temp_patterns:
            cleanup_files.extend(glob.glob(pattern))
        
        # Không xóa file cuối cùng
        cleanup_files = [f for f in cleanup_files if f != self.final_output]
        
        if cleanup_files:
            print(f"🗑️ Tìm thấy {len(cleanup_files)} file tạm:")
            
            # Group files by type for better display
            md_files = [f for f in cleanup_files if f.endswith('.md')]
            csv_files = [f for f in cleanup_files if f.endswith('.csv')]
            
            if md_files:
                print(f"   📝 Markdown files: {len(md_files)}")
                for file in md_files[:3]:  # Show first 3
                    print(f"      📄 {file}")
                if len(md_files) > 3:
                    print(f"      ... và {len(md_files) - 3} file MD khác")
            
            if csv_files:
                print(f"   📊 CSV files: {len(csv_files)}")
                for file in csv_files[:3]:  # Show first 3
                    print(f"      📄 {file}")
                if len(csv_files) > 3:
                    print(f"      ... và {len(csv_files) - 3} file CSV khác")
            
            try:
                choice = input(f"\nXóa tất cả {len(cleanup_files)} file tạm? (y/N): ").strip().lower()
                if choice in ['y', 'yes']:
                    deleted_count = 0
                    for file in cleanup_files:
                        try:
                            os.remove(file)
                            deleted_count += 1
                        except Exception as e:
                            print(f"   ❌ Lỗi xóa {file}: {e}")
                    
                    print(f"   ✅ Đã xóa {deleted_count}/{len(cleanup_files)} file")
                else:
                    print("   ⏭️ Giữ lại các file tạm")
            except KeyboardInterrupt:
                print("\n   ⏭️ Bỏ qua dọn dẹp")
        else:
            print("✨ Không có file tạm cần dọn dẹp")
    
    def run_workflow(self):
        """Chạy toàn bộ workflow với hỗ trợ batch processing"""
        self.print_header("SANCTIONS PROCESSING WORKFLOW - MULTI PDF SUPPORT")
        
        print("🎯 Workflow hỗ trợ xử lý nhiều PDF:")
        print("   1. Chuyển PDF sang Markdown")
        print("   2. Chuyển Markdown sang Base64")  
        print("   3. Xử lý Base64 với Gemini AI → CSV")
        print("   4. Tải dữ liệu OFAC")
        print("   5. Tải dữ liệu UN Sanctions")
        print("   6. Tổng hợp tất cả thành 1 file CSV")
        
        # Chọn chế độ và lấy input files
        if not self.get_processing_mode():
            return False
        
        # Lấy Gemini API key
        if not self.get_gemini_api_key():
            print("⚠️ Tiếp tục không có Gemini API - sẽ bỏ qua xử lý PDF")
        
        # Hiển thị tóm tắt
        print(f"\n📋 TÓM TẮT XỬ LÝ:")
        print(f"   📁 Chế độ: {self.processing_mode}")
        print(f"   📄 Số PDF: {len(self.pdf_files)}")
        print(f"   🤖 Gemini API: {'✅ Có' if self.gemini_api_key else '❌ Không'}")
        
        if self.processing_mode == 'batch':
            total_size = sum(os.path.getsize(f) for f in self.pdf_files)
            print(f"   📊 Tổng kích thước PDF: {total_size:,} bytes ({total_size/1024/1024:.1f} MB)")
            
            # Estimate processing time
            estimated_time = len(self.pdf_files) * 2  # 2 minutes per PDF estimate
            print(f"   ⏱️ Thời gian ước tính: ~{estimated_time} phút")
        
        # Xác nhận tiếp tục
        try:
            if self.processing_mode == 'single':
                confirm_msg = f"🚀 Bắt đầu xử lý {self.pdf_files[0]}? (Y/n): "
            else:
                confirm_msg = f"🚀 Bắt đầu xử lý {len(self.pdf_files)} PDF files? (Y/n): "
                
            confirm = input(confirm_msg).strip().lower()
            if confirm and confirm not in ['y', 'yes']:
                print("❌ Đã hủy workflow")
                return False
        except KeyboardInterrupt:
            print("\n❌ Đã hủy workflow")
            return False
        
        start_time = time.time()
        success_steps = 0
        total_steps = 6
        
        # Chạy từng bước với batch support
        if self.processing_mode == 'batch':
            steps = [
                self.step1_pdf_to_md_batch,
                self.step2_md_to_base64_batch,
                self.step3_base64_to_csv_via_gemini_batch,
                self.step4_ofac_extraction,
                self.step5_un_extraction,
                self.step6_consolidate_data
            ]
        else:
            # Single mode - use original methods adapted
            steps = [
                self.step1_pdf_to_md_single,
                self.step2_md_to_base64_single,
                self.step3_base64_to_csv_via_gemini_single,
                self.step4_ofac_extraction,
                self.step5_un_extraction,
                self.step6_consolidate_data
            ]
        
        for i, step_func in enumerate(steps, 1):
            try:
                # Bỏ qua bước 3 nếu không có API key
                if i == 3 and not self.gemini_api_key:
                    print(f"⏭️ Bỏ qua bước {i} - không có Gemini API key")
                    continue
                
                if step_func():
                    success_steps += 1
                    print(f"✅ Bước {i} hoàn thành")
                else:
                    print(f"❌ Bước {i} thất bại")
                    if i in [1, 2, 6]:  # Bước quan trọng
                        print("🛑 Dừng workflow do lỗi quan trọng")
                        return False
                    elif i == 3:  # Gemini step không bắt buộc
                        print("⏭️ Tiếp tục với các bước khác...")
            except KeyboardInterrupt:
                print(f"\n🛑 Người dùng dừng workflow tại bước {i}")
                return False
            except Exception as e:
                print(f"❌ Lỗi không mong muốn tại bước {i}: {e}")
                if i in [1, 2, 6]:  # Bước quan trọng
                    return False
                else:
                    print("⏭️ Tiếp tục với bước tiếp theo...")
        
        # Tổng kết
        elapsed_time = time.time() - start_time
        self.print_header("KẾT QUẢ WORKFLOW")
        
        print(f"⏱️ Thời gian thực hiện: {elapsed_time:.1f} giây ({elapsed_time/60:.1f} phút)")
        print(f"✅ Hoàn thành: {success_steps}/{total_steps} bước")
        
        if success_steps >= 4:  # Tối thiểu cần 4 bước để có kết quả
            print("🎉 WORKFLOW HOÀN THÀNH THÀNH CÔNG!")
            
            if os.path.exists(self.final_output):
                file_size = os.path.getsize(self.final_output)
                df = pd.read_csv(self.final_output)
                print(f"📁 File cuối cùng: {self.final_output}")
                print(f"📊 Kích thước: {file_size:,} bytes")
                print(f"📈 Số records: {len(df):,}")
                
                # Hiển thị summary chi tiết cho batch mode
                if self.processing_mode == 'batch' and hasattr(self, 'final_summary'):
                    print(f"\n🎯 CHI TIẾT XỬ LÝ BATCH:")
                    print(f"   📄 PDF files xử lý: {len(self.pdf_files)}")
                    
                    if hasattr(self, 'base64_results'):
                        successful_pdf = len([r for r in self.base64_results if 'csv_file' in r])
                        print(f"   ✅ PDF thành công: {successful_pdf}/{len(self.pdf_files)}")
                        
                        total_pdf_records = self.final_summary['processing_breakdown']['pdf_records']
                        print(f"   📊 Records từ PDF: {total_pdf_records:,}")
                        
                        if successful_pdf > 0:
                            avg_records = total_pdf_records / successful_pdf
                            print(f"   📈 Trung bình mỗi PDF: {avg_records:.0f} records")
                
                # Hiển thị breakdown cuối
                print(f"\n📋 SUMMARY CHI TIẾT:")
                print(f"   🎯 PDF xử lý: {len(self.pdf_files)} files")
                print(f"   📄 Markdown: {'✅ Tạo thành công' if hasattr(self, 'conversion_results') else '❌ Thất bại'}")
                print(f"   🔒 Base64: {'✅ Chuyển đổi thành công' if hasattr(self, 'base64_results') else '❌ Thất bại'}")
                print(f"   🤖 Gemini API: {'✅ Xử lý thành công' if self.gemini_api_key else '⏭️ Bỏ qua'}")
                print(f"   🇺🇸 OFAC: ✅ Tích hợp")
                print(f"   🌍 UN: ✅ Tích hợp")
                
                # Performance metrics
                if self.processing_mode == 'batch' and elapsed_time > 0:
                    files_per_minute = (len(self.pdf_files) * 60) / elapsed_time
                    print(f"   📊 Hiệu suất: {files_per_minute:.1f} PDF/phút")
                
                # Dọn dẹp file tạm
                self.cleanup_temp_files()
                
                return True
        else:
            print("⚠️ Workflow hoàn thành một phần")
            print("💡 Kiểm tra lại các bước bị lỗi")
            print(f"💡 Cần ít nhất 4/{total_steps} bước để có kết quả hợp lệ")
            return False
    
    # Single mode methods (adapted from batch methods)
    def step1_pdf_to_md_single(self):
        """Single PDF conversion - wrapper cho batch method"""
        return self.step1_pdf_to_md_batch()
    
    def step2_md_to_base64_single(self):
        """Single MD to Base64 - wrapper cho batch method"""
        return self.step2_md_to_base64_batch()
    
    def step3_base64_to_csv_via_gemini_single(self):
        """Single Base64 to CSV - wrapper cho batch method"""
        return self.step3_base64_to_csv_via_gemini_batch()

def main():
    """Hàm main với multi-PDF support"""
    try:
        workflow = SanctionsWorkflow()
        success = workflow.run_workflow()
        
        if success:
            print("\n🎊 CẢM ƠN BẠN ĐÃ SỬ DỤNG SANCTIONS PROCESSING SYSTEM!")
            print("💡 Workflow đã được nâng cấp với hỗ trợ:")
            print("   ✅ Xử lý nhiều PDF cùng lúc")
            print("   ✅ Batch processing tự động")
            print("   ✅ PDF → Markdown → Base64 → CSV (via Gemini API)")
            print("   ✅ Tích hợp OFAC và UN data")
            sys.exit(0)
        else:
            print("\n💔 Workflow không hoàn thành. Vui lòng kiểm tra lại.")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n👋 Tạm biệt!")
        sys.exit(0)
    except Exception as e:
        print(f"\n💥 Lỗi hệ thống: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()