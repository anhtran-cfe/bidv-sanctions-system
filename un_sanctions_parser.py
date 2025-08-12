import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import os
import requests
from bs4 import BeautifulSoup
import urllib.parse

def download_un_xml_file(output_filename='consolidated_list.xml'):
    """
    Download UN Sanctions XML file from official website
    
    Args:
        output_filename: Local filename to save the XML file
        
    Returns:
        str: Path to downloaded file or None if failed
    """
    
    try:
        print("Downloading UN Sanctions XML file...")
        
        # First, get the main page to find the XML download link
        main_url = "https://main.un.org/securitycouncil/en/content/un-sc-consolidated-list"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(main_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Parse HTML to find XML download link
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Try the specific selector first
        xml_link = None
        try:
            selector = "#block-un3-sc-content > div > article > div > div.mb-m.mb-lg-8._none > div > div > div.bg-xl-12.col-lg-8.col-md-12.col-12 > div > div > div > div > div > p:nth-child(5) > a:nth-child(2)"
            xml_element = soup.select_one(selector)
            if xml_element and xml_element.get('href'):
                xml_link = xml_element.get('href')
                print(f"Found XML link using specific selector: {xml_link}")
        except Exception as e:
            print(f"Specific selector failed: {e}")
        
        # Fallback: Search for XML link by text or href pattern
        if not xml_link:
            print("Trying fallback methods to find XML link...")
            
            # Method 1: Look for links containing 'xml' in href
            for link in soup.find_all('a', href=True):
                href = link.get('href', '').lower()
                if '.xml' in href and ('consolidated' in href or 'sanctions' in href):
                    xml_link = link.get('href')
                    print(f"Found XML link by href pattern: {xml_link}")
                    break
            
            # Method 2: Look for links with text containing 'xml'
            if not xml_link:
                for link in soup.find_all('a', href=True):
                    text = link.get_text(strip=True).lower()
                    if 'xml' in text and ('format' in text or 'download' in text):
                        xml_link = link.get('href')
                        print(f"Found XML link by text: {xml_link}")
                        break
        
        if not xml_link:
            print("Could not find XML download link. Available links:")
            for link in soup.find_all('a', href=True)[:10]:  # Show first 10 links
                print(f"  - {link.get_text(strip=True)[:50]}: {link.get('href')}")
            return None
        
        # Handle relative URLs
        if xml_link.startswith('/'):
            xml_link = 'https://main.un.org' + xml_link
        elif not xml_link.startswith('http'):
            xml_link = urllib.parse.urljoin(main_url, xml_link)
        
        print(f"Downloading XML from: {xml_link}")
        
        # Download the XML file
        xml_response = requests.get(xml_link, headers=headers, timeout=60)
        xml_response.raise_for_status()
        
        # Save to file
        with open(output_filename, 'wb') as f:
            f.write(xml_response.content)
        
        print(f"XML file downloaded successfully: {output_filename}")
        print(f"File size: {len(xml_response.content):,} bytes")
        
        return output_filename
        
    except requests.exceptions.RequestException as e:
        print(f"Network error downloading XML file: {e}")
        return None
    except Exception as e:
        print(f"Error downloading XML file: {e}")
        return None

def parse_un_sanctions_xml(xml_file_path=None, output_file='sanctions_cleaned', auto_download=True):
    """
    Parse UN Sanctions XML file and extract recently listed individuals/entities
    
    Args:
        xml_file_path: Path to UN Sanctions XML file (if None and auto_download=True, will download)
        output_file: Output Excel file name
        auto_download: If True, automatically download XML file if not provided
    """
    
    # Auto-download if no file path provided
    if xml_file_path is None and auto_download:
        print("No XML file provided, attempting to download...")
        xml_file_path = download_un_xml_file()
        if xml_file_path is None:
            print("Failed to download XML file")
            return
    elif xml_file_path is None:
        print("No XML file provided and auto_download is disabled")
        return
    
    # Check if file exists
    if not os.path.exists(xml_file_path):
        if auto_download:
            print(f"File {xml_file_path} not found, attempting to download...")
            xml_file_path = download_un_xml_file(xml_file_path)
            if xml_file_path is None:
                print("Failed to download XML file")
                return
        else:
            print(f"File {xml_file_path} not found")
            return
    
    try:
        # Parse XML file
        tree = ET.parse(xml_file_path)
        root = tree.getroot()
        
        # Extract dateGenerated from root element
        date_generated_str = root.get('dateGenerated')
        if date_generated_str:
            # Parse date format: 2025-07-30T23:00:05.333Z
            date_generated = datetime.fromisoformat(date_generated_str.replace('Z', '+00:00')).date()
        else:
            # Fallback to current date if not found
            date_generated = datetime.now().date()
            
        print(f"File generated date: {date_generated}")
        
        # Calculate target dates (today and yesterday from generation date)
        target_dates = [
            date_generated,
            date_generated - timedelta(days=1)
        ]
        
        print(f"Looking for listings on: {target_dates}")
        
        # Lists to store parsed data
        individuals_data = []
        entities_data = []
        
        # Process INDIVIDUALS
        individuals_section = root.find('INDIVIDUALS')
        if individuals_section is not None:
            for individual in individuals_section.findall('INDIVIDUAL'):
                listed_on = individual.find('LISTED_ON')
                if listed_on is not None and listed_on.text:
                    try:
                        listed_date = datetime.strptime(listed_on.text, '%Y-%m-%d').date()
                        
                        # Check if listed on target dates
                        if listed_date in target_dates:
                            individual_data = extract_individual_data(individual)
                            individuals_data.append(individual_data)
                            print(f"Found individual: {individual_data['Name']} listed on {listed_date}")
                    except ValueError:
                        continue
        
        # Process ENTITIES  
        entities_section = root.find('ENTITIES')
        if entities_section is not None:
            for entity in entities_section.findall('ENTITY'):
                listed_on = entity.find('LISTED_ON')
                if listed_on is not None and listed_on.text:
                    try:
                        listed_date = datetime.strptime(listed_on.text, '%Y-%m-%d').date()
                        
                        # Check if listed on target dates
                        if listed_date in target_dates:
                            entity_data = extract_entity_data(entity)
                            entities_data.append(entity_data)
                            print(f"Found entity: {entity_data['Name']} listed on {listed_date}")
                    except ValueError:
                        continue
        
        # Create CSV files with results
        if individuals_data or entities_data:
            # Generate timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            base_filename = output_file.rsplit('.', 1)[0]  # Remove extension
            
            if individuals_data:
                df_individuals = pd.DataFrame(individuals_data)
                individuals_file = f"{base_filename}_{timestamp}.csv"
                df_individuals.to_csv(individuals_file, index=False)
                print(f"Exported {len(individuals_data)} individuals to {individuals_file}")
            
            if entities_data:
                df_entities = pd.DataFrame(entities_data)
                entities_file = f"{base_filename}_entities_{timestamp}.csv"
                df_entities.to_csv(entities_file, index=False)
                print(f"Exported {len(entities_data)} entities to {entities_file}")
                    
            print(f"Results exported to CSV files")
        else:
            print("No new listings found for the target dates")
            
    except Exception as e:
        print(f"Error processing XML file: {str(e)}")

def extract_individual_data(individual):
    """Extract individual data from XML element"""
    
    # Basic info
    first_name = get_text(individual.find('FIRST_NAME'))
    second_name = get_text(individual.find('SECOND_NAME'))
    third_name = get_text(individual.find('THIRD_NAME'))
    fourth_name = get_text(individual.find('FOURTH_NAME'))
    
    # Combine names
    name_parts = [name for name in [first_name, second_name, third_name, fourth_name] if name]
    full_name = ' '.join(name_parts)
    
    # Other fields
    reference_number = get_text(individual.find('REFERENCE_NUMBER'))
    listed_on = get_text(individual.find('LISTED_ON'))
    gender = get_text(individual.find('GENDER'))
    comments = get_text(individual.find('COMMENTS1'))
    
    # Designations
    designations = []
    for designation in individual.findall('.//DESIGNATION/VALUE'):
        if designation.text:
            designations.append(designation.text.strip())
    
    # Nationalities
    nationalities = []
    for nationality in individual.findall('.//NATIONALITY/VALUE'):
        if nationality.text:
            nationalities.append(nationality.text.strip())
    
    # Aliases
    aliases = []
    for alias in individual.findall('.//INDIVIDUAL_ALIAS'):
        alias_name = get_text(alias.find('ALIAS_NAME'))
        quality = get_text(alias.find('QUALITY'))
        if alias_name:
            aliases.append(f"{alias_name} ({quality})" if quality else alias_name)
    
    # Date of birth
    dob_info = []
    for dob in individual.findall('.//INDIVIDUAL_DATE_OF_BIRTH'):
        date_val = get_text(dob.find('DATE'))
        year_val = get_text(dob.find('YEAR'))
        note_val = get_text(dob.find('NOTE'))
        
        if date_val:
            dob_info.append(date_val)
        elif year_val:
            dob_info.append(year_val)
        elif note_val:
            dob_info.append(note_val)
    
    # Place of birth
    pob_info = []
    for pob in individual.findall('.//INDIVIDUAL_PLACE_OF_BIRTH'):
        city = get_text(pob.find('CITY'))
        state = get_text(pob.find('STATE_PROVINCE'))
        country = get_text(pob.find('COUNTRY'))
        
        pob_parts = [p for p in [city, state, country] if p]
        if pob_parts:
            pob_info.append(', '.join(pob_parts))
    
    # Addresses  
    addresses = []
    for addr in individual.findall('.//INDIVIDUAL_ADDRESS'):
        street = get_text(addr.find('STREET'))
        city = get_text(addr.find('CITY'))
        state = get_text(addr.find('STATE_PROVINCE'))
        country = get_text(addr.find('COUNTRY'))
        note = get_text(addr.find('NOTE'))
        
        addr_parts = [p for p in [street, city, state, country] if p]
        if addr_parts:
            address = ', '.join(addr_parts)
            if note:
                address += f" ({note})"
            addresses.append(address)
    
    return {
        'Type': 'Individual',
        'Name': full_name,
        'Reference_Number': reference_number,
        'Listed_On': listed_on,
        'Gender': gender,
        'Designations': '; '.join(designations) if designations else '',
        'Nationalities': '; '.join(nationalities) if nationalities else '',
        'Aliases': '; '.join(aliases) if aliases else '',
        'Date_of_Birth': '; '.join(dob_info) if dob_info else '',
        'Place_of_Birth': '; '.join(pob_info) if pob_info else '',
        'Addresses': '; '.join(addresses) if addresses else '',
        'Comments': comments,
        'Source': 'UN Security Council'
    }

def extract_entity_data(entity):
    """Extract entity data from XML element"""
    
    # Basic info
    name = get_text(entity.find('FIRST_NAME'))  # For entities, name is in FIRST_NAME
    reference_number = get_text(entity.find('REFERENCE_NUMBER'))
    listed_on = get_text(entity.find('LISTED_ON'))
    comments = get_text(entity.find('COMMENTS1'))
    
    # Aliases
    aliases = []
    for alias in entity.findall('.//ENTITY_ALIAS'):
        alias_name = get_text(alias.find('ALIAS_NAME'))
        quality = get_text(alias.find('QUALITY'))
        if alias_name:
            if quality:
                aliases.append(f"{alias_name} ({quality})")
            else:
                aliases.append(alias_name)
    
    # Addresses
    addresses = []
    for addr in entity.findall('.//ENTITY_ADDRESS'):
        street = get_text(addr.find('STREET'))
        city = get_text(addr.find('CITY'))
        state = get_text(addr.find('STATE_PROVINCE'))
        country = get_text(addr.find('COUNTRY'))
        
        addr_parts = [p for p in [street, city, state, country] if p]
        if addr_parts:
            addresses.append(', '.join(addr_parts))
    
    return {
        'Type': 'Entity', 
        'Name': name,
        'Reference_Number': reference_number,
        'Listed_On': listed_on,
        'Gender': '',
        'Designations': '',
        'Nationalities': '',
        'Aliases': '; '.join(aliases) if aliases else '',
        'Date_of_Birth': '',
        'Place_of_Birth': '',
        'Addresses': '; '.join(addresses) if addresses else '',
        'Comments': comments,
        'Source': 'UN Security Council'
    }

def get_text(element):
    """Safely get text from XML element"""
    return element.text.strip() if element is not None and element.text else ''

# Usage example
if __name__ == "__main__":
    # Option 1: Auto-download and process (recommended)
    print("=== UN Sanctions Parser ===")
    parse_un_sanctions_xml()  # Will auto-download XML file
    
    # Option 2: Use existing local file
    # parse_un_sanctions_xml("my_consolidated_list.xml")
    
    # Option 3: Download only
    # download_un_xml_file("latest_consolidated_list.xml")
