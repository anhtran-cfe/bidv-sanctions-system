import streamlit as st
import pandas as pd
import io
import base64
import time
import os
from datetime import datetime
import tempfile
import zipfile

# Import your modules (make sure these files are in the same directory)
try:
    from gemini_markdown_csv import GeminiMarkdownToCSVConverter
    from markdown_to_base64 import MarkdownBase64Converter
    from ofac_extractor import OFACSanctionsExtractor
    from un_sanctions_parser import parse_un_sanctions_xml
    import pymupdf4llm
except ImportError as e:
    st.error(f"❌ Missing required modules: {e}")
    st.stop()

# Page config
st.set_page_config(
    page_title="BIDV Sanctions Processing System",
    page_icon="🔒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Authentication function
def check_authentication():
    """Check if user is authenticated"""
    return st.session_state.get('authenticated', False)

def login_page():
    """Display login page"""
    st.markdown("""
    <div style='text-align: center; padding: 2rem;'>
        <h1>🔒 BIDV Sanctions Processing System</h1>
        <p>Please login to access the system</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("👤 Username", placeholder="Enter your username")
            password = st.text_input("🔑 Password", type="password", placeholder="Enter your password")
            submitted = st.form_submit_button("🚀 Login", use_container_width=True)
            
            if submitted:
                if username == "BIDV" and password == "CSCV123":
                    st.session_state.authenticated = True
                    st.success("✅ Login successful! Redirecting...")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Invalid credentials!")

def logout():
    """Logout function"""
    st.session_state.authenticated = False
    st.session_state.clear()
    st.rerun()

# Main app functions
def pdf_to_markdown(pdf_file):
    """Convert PDF to Markdown"""
    try:
        # Save uploaded file temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
            tmp_file.write(pdf_file.read())
            tmp_path = tmp_file.name
        
        # Convert PDF to markdown
        md_text = pymupdf4llm.to_markdown(tmp_path)
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        return md_text
    except Exception as e:
        st.error(f"Error converting PDF: {e}")
        return None

def process_with_gemini(markdown_content, api_key):
    """Process markdown with Gemini API"""
    try:
        # Convert to base64
        converter_base64 = MarkdownBase64Converter()
        base64_result = converter_base64.string_to_base64(markdown_content)
        
        if not base64_result['success']:
            return None, f"Base64 conversion failed: {base64_result['error']}"
        
        # Process with Gemini
        converter_gemini = GeminiMarkdownToCSVConverter(api_key)
        result = converter_gemini.convert_markdown_to_csv(
            markdown_content=base64_result['base64_content'],
            is_base64=True
        )
        
        if result['success']:
            return result['csv_content'], None
        else:
            return None, result['error']
            
    except Exception as e:
        return None, str(e)

def get_ofac_data():
    """Get OFAC data"""
    try:
        extractor = OFACSanctionsExtractor()
        df = extractor.run_extraction(save_file=False)
        return df
    except Exception as e:
        st.error(f"Error fetching OFAC data: {e}")
        return pd.DataFrame()

def get_un_data():
    """Get UN Sanctions data"""
    try:
        # Use temporary file for UN data
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            temp_path = tmp_file.name
        
        parse_un_sanctions_xml(output_file=temp_path)
        
        # Read the generated CSV
        csv_files = [f for f in os.listdir('.') if f.startswith('sanctions_cleaned_')]
        if csv_files:
            latest_file = max(csv_files, key=os.path.getctime)
            df = pd.read_csv(latest_file)
            os.unlink(latest_file)  # Clean up
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error fetching UN data: {e}")
        return pd.DataFrame()

# Main application
def main_app():
    """Main application interface"""
    
    # Sidebar
    with st.sidebar:
        st.title("🔒 BIDV Sanctions System")
        st.markdown("---")
        
        # User info
        st.markdown("👤 **Logged in as:** BIDV User")
        
        if st.button("🚪 Logout", use_container_width=True):
            logout()
        
        st.markdown("---")
        
        # Navigation
        page = st.selectbox(
            "📋 Select Function",
            ["📊 Dashboard", "📄 PDF Processing", "🇺🇸 OFAC Data", "🌍 UN Sanctions", "⚙️ Batch Processing"]
        )
    
    # Main content
    if page == "📊 Dashboard":
        dashboard_page()
    elif page == "📄 PDF Processing":
        pdf_processing_page()
    elif page == "🇺🇸 OFAC Data":
        ofac_page()
    elif page == "🌍 UN Sanctions":
        un_page()
    elif page == "⚙️ Batch Processing":
        batch_processing_page()

def dashboard_page():
    """Dashboard page"""
    st.title("📊 BIDV Sanctions Processing Dashboard")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("📄 PDF Processed", "0", "0")
    
    with col2:
        st.metric("🇺🇸 OFAC Records", "Loading...", "0")
    
    with col3:
        st.metric("🌍 UN Records", "Loading...", "0")
    
    with col4:
        st.metric("✅ Total Processed", "0", "0")
    
    st.markdown("---")
    
    # System status
    st.subheader("🔧 System Status")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.success("✅ PDF Converter: Ready")
        st.success("✅ Base64 Converter: Ready")
        
        # Check Gemini API
        gemini_api_key = st.secrets.get("GEMINI_API_KEY", "")
        if gemini_api_key:
            st.success("✅ Gemini API: Connected")
        else:
            st.warning("⚠️ Gemini API: Not configured")
    
    with col2:
        st.success("✅ OFAC Service: Ready")
        st.success("✅ UN Service: Ready")
        st.info("ℹ️ System ready for processing")

def pdf_processing_page():
    """PDF processing page"""
    st.title("📄 PDF Processing")
    
    # Check API key
    gemini_api_key = st.secrets.get("GEMINI_API_KEY", "")
    if not gemini_api_key:
        st.error("❌ Gemini API Key not configured. Please set GEMINI_API_KEY in Streamlit secrets.")
        return
    
    # File upload
    uploaded_file = st.file_uploader(
        "📁 Upload PDF File",
        type=['pdf'],
        help="Upload a PDF file containing sanctions data"
    )
    
    if uploaded_file is not None:
        st.success(f"✅ File uploaded: {uploaded_file.name}")
        
        # File info
        file_size = len(uploaded_file.read())
        uploaded_file.seek(0)  # Reset file pointer
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("📏 File Size", f"{file_size:,} bytes")
        with col2:
            st.metric("📁 File Name", uploaded_file.name)
        
        if st.button("🚀 Process PDF", type="primary"):
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Step 1: Convert PDF to Markdown
            status_text.text("📄 Converting PDF to Markdown...")
            progress_bar.progress(20)
            
            markdown_content = pdf_to_markdown(uploaded_file)
            
            if markdown_content:
                st.success("✅ PDF converted to Markdown")
                
                # Step 2: Process with Gemini
                status_text.text("🤖 Processing with Gemini AI...")
                progress_bar.progress(60)
                
                csv_content, error = process_with_gemini(markdown_content, gemini_api_key)
                
                if csv_content and not error:
                    progress_bar.progress(100)
                    status_text.text("✅ Processing completed!")
                    
                    st.success("🎉 PDF processed successfully!")
                    
                    # Display results
                    try:
                        df = pd.read_csv(io.StringIO(csv_content))
                        
                        st.subheader("📊 Processing Results")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("📈 Total Records", len(df))
                        with col2:
                            if 'Type' in df.columns:
                                type_counts = df['Type'].value_counts()
                                st.metric("👥 Individuals", type_counts.get('Individual', 0))
                        with col3:
                            if 'Type' in df.columns:
                                st.metric("🏢 Entities", type_counts.get('Entity', 0))
                        
                        # Display data
                        st.subheader("📋 Extracted Data")
                        st.dataframe(df, use_container_width=True)
                        
                        # Download button
                        csv_download = df.to_csv(index=False)
                        st.download_button(
                            label="💾 Download CSV",
                            data=csv_download,
                            file_name=f"sanctions_{uploaded_file.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            mime="text/csv"
                        )
                        
                    except Exception as e:
                        st.error(f"Error displaying results: {e}")
                        st.text("Raw CSV Content:")
                        st.text_area("CSV Output", csv_content, height=300)
                        
                else:
                    progress_bar.progress(0)
                    status_text.text("❌ Processing failed")
                    st.error(f"Gemini processing failed: {error}")
            else:
                progress_bar.progress(0)
                status_text.text("❌ PDF conversion failed")
                st.error("Failed to convert PDF to Markdown")

def ofac_page():
    """OFAC data page"""
    st.title("🇺🇸 OFAC Sanctions Data")
    
    if st.button("🔄 Fetch Latest OFAC Data", type="primary"):
        with st.spinner("Fetching OFAC data..."):
            df = get_ofac_data()
        
        if not df.empty:
            st.success(f"✅ Fetched {len(df)} OFAC records")
            
            # Display summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📈 Total Records", len(df))
            with col2:
                if 'Type' in df.columns:
                    individuals = len(df[df['Type'] == 'Individual'])
                    st.metric("👥 Individuals", individuals)
            with col3:
                if 'Type' in df.columns:
                    entities = len(df[df['Type'] == 'Entity'])
                    st.metric("🏢 Entities", entities)
            
            # Display data
            st.subheader("📋 OFAC Data")
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv_download = df.to_csv(index=False)
            st.download_button(
                label="💾 Download OFAC CSV",
                data=csv_download,
                file_name=f"ofac_sanctions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ No OFAC data retrieved")

def un_page():
    """UN Sanctions page"""
    st.title("🌍 UN Security Council Sanctions")
    
    if st.button("🔄 Fetch Latest UN Data", type="primary"):
        with st.spinner("Fetching UN sanctions data..."):
            df = get_un_data()
        
        if not df.empty:
            st.success(f"✅ Fetched {len(df)} UN records")
            
            # Display summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📈 Total Records", len(df))
            with col2:
                if 'Type' in df.columns:
                    individuals = len(df[df['Type'] == 'Individual'])
                    st.metric("👥 Individuals", individuals)
            with col3:
                if 'Type' in df.columns:
                    entities = len(df[df['Type'] == 'Entity'])
                    st.metric("🏢 Entities", entities)
            
            # Display data
            st.subheader("📋 UN Sanctions Data")
            st.dataframe(df, use_container_width=True)
            
            # Download button
            csv_download = df.to_csv(index=False)
            st.download_button(
                label="💾 Download UN CSV",
                data=csv_download,
                file_name=f"un_sanctions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ No UN data retrieved")

def batch_processing_page():
    """Batch processing page"""
    st.title("⚙️ Batch Processing")
    
    # Check API key
    gemini_api_key = st.secrets.get("GEMINI_API_KEY", "")
    if not gemini_api_key:
        st.error("❌ Gemini API Key not configured. Please set GEMINI_API_KEY in Streamlit secrets.")
        return
    
    # Multiple file upload
    uploaded_files = st.file_uploader(
        "📁 Upload Multiple PDF Files",
        type=['pdf'],
        accept_multiple_files=True,
        help="Upload multiple PDF files for batch processing"
    )
    
    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} files uploaded")
        
        # Display file info
        st.subheader("📋 Files to Process")
        for i, file in enumerate(uploaded_files, 1):
            col1, col2, col3 = st.columns([1, 2, 1])
            with col1:
                st.write(f"{i}.")
            with col2:
                st.write(file.name)
            with col3:
                st.write(f"{len(file.read()):,} bytes")
                file.seek(0)  # Reset file pointer
        
        # Processing options
        st.subheader("⚙️ Processing Options")
        
        col1, col2 = st.columns(2)
        with col1:
            include_ofac = st.checkbox("🇺🇸 Include OFAC data", value=True)
        with col2:
            include_un = st.checkbox("🌍 Include UN data", value=True)
        
        if st.button("🚀 Start Batch Processing", type="primary"):
            
            # Initialize progress tracking
            total_steps = len(uploaded_files) + (1 if include_ofac else 0) + (1 if include_un else 0) + 1
            current_step = 0
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_dataframes = []
            
            # Process each PDF
            for i, file in enumerate(uploaded_files, 1):
                status_text.text(f"📄 Processing file {i}/{len(uploaded_files)}: {file.name}")
                
                # Convert PDF to markdown
                markdown_content = pdf_to_markdown(file)
                
                if markdown_content:
                    # Process with Gemini
                    csv_content, error = process_with_gemini(markdown_content, gemini_api_key)
                    
                    if csv_content and not error:
                        try:
                            df = pd.read_csv(io.StringIO(csv_content))
                            df['Source_File'] = file.name
                            all_dataframes.append(df)
                            st.success(f"✅ Processed {file.name}: {len(df)} records")
                        except Exception as e:
                            st.error(f"❌ Error processing {file.name}: {e}")
                    else:
                        st.error(f"❌ Failed to process {file.name}: {error}")
                else:
                    st.error(f"❌ Failed to convert {file.name} to markdown")
                
                current_step += 1
                progress_bar.progress(current_step / total_steps)
            
            # Add OFAC data if requested
            if include_ofac:
                status_text.text("🇺🇸 Fetching OFAC data...")
                ofac_df = get_ofac_data()
                if not ofac_df.empty:
                    ofac_df['Source_File'] = 'OFAC_API'
                    all_dataframes.append(ofac_df)
                    st.success(f"✅ Added OFAC data: {len(ofac_df)} records")
                
                current_step += 1
                progress_bar.progress(current_step / total_steps)
            
            # Add UN data if requested
            if include_un:
                status_text.text("🌍 Fetching UN data...")
                un_df = get_un_data()
                if not un_df.empty:
                    un_df['Source_File'] = 'UN_API'
                    all_dataframes.append(un_df)
                    st.success(f"✅ Added UN data: {len(un_df)} records")
                
                current_step += 1
                progress_bar.progress(current_step / total_steps)
            
            # Consolidate all data
            if all_dataframes:
                status_text.text("📊 Consolidating all data...")
                
                final_df = pd.concat(all_dataframes, ignore_index=True, sort=False)
                
                # Remove duplicates based on Name
                original_count = len(final_df)
                final_df = final_df.drop_duplicates(subset=['Name'], keep='first')
                duplicate_count = original_count - len(final_df)
                
                progress_bar.progress(1.0)
                status_text.text("✅ Batch processing completed!")
                
                st.success("🎉 Batch processing completed successfully!")
                
                # Display results
                st.subheader("📊 Batch Processing Results")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("📈 Total Records", len(final_df))
                with col2:
                    st.metric("📁 Files Processed", len(uploaded_files))
                with col3:
                    st.metric("🔄 Duplicates Removed", duplicate_count)
                with col4:
                    if 'Source_File' in final_df.columns:
                        unique_sources = final_df['Source_File'].nunique()
                        st.metric("📋 Data Sources", unique_sources)
                
                # Source breakdown
                if 'Source_File' in final_df.columns:
                    st.subheader("📋 Records by Source")
                    source_counts = final_df['Source_File'].value_counts()
                    st.bar_chart(source_counts)
                
                # Display consolidated data
                st.subheader("📋 Consolidated Data")
                st.dataframe(final_df, use_container_width=True)
                
                # Download options
                col1, col2 = st.columns(2)
                
                with col1:
                    # CSV download
                    csv_download = final_df.to_csv(index=False)
                    st.download_button(
                        label="💾 Download Consolidated CSV",
                        data=csv_download,
                        file_name=f"consolidated_sanctions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
                
                with col2:
                    # Excel download
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                        final_df.to_excel(writer, sheet_name='Consolidated_Data', index=False)
                        
                        # Add summary sheet
                        summary_data = {
                            'Metric': ['Total Records', 'Files Processed', 'Duplicates Removed', 'Data Sources'],
                            'Value': [len(final_df), len(uploaded_files), duplicate_count, 
                                     final_df['Source_File'].nunique() if 'Source_File' in final_df.columns else 0]
                        }
                        summary_df = pd.DataFrame(summary_data)
                        summary_df.to_excel(writer, sheet_name='Summary', index=False)
                    
                    st.download_button(
                        label="📊 Download Excel Report",
                        data=buffer.getvalue(),
                        file_name=f"consolidated_sanctions_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            else:
                progress_bar.progress(1.0)
                status_text.text("❌ No data processed")
                st.error("❌ No data was successfully processed")

# Main app logic
def main():
    """Main app entry point"""
    
    # Check authentication
    if not check_authentication():
        login_page()
    else:
        main_app()

if __name__ == "__main__":
    main()