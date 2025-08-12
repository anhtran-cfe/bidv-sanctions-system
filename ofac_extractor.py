import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import pandas as pd
import re
from typing import List, Dict, Optional

class OFACSanctionsExtractor:
    def __init__(self):
        self.api_url = "https://sanctionslistservice.ofac.treas.gov/changes/latest"
        self.namespace = {'ns': 'https://www.treasury.gov/ofac/DeltaFile/1.0'}
        
    def fetch_latest_sanctions_data(self) -> str:
        """
        Fetch the latest sanctions data from OFAC API
        Returns the XML content as string
        """
        try:
            print("Fetching latest sanctions data from OFAC...")
            response = requests.get(self.api_url, timeout=30)
            response.raise_for_status()
            
            print(f"Successfully fetched data. Content length: {len(response.text)} characters")
            return response.text
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data from OFAC API: {e}")
            raise
    
    def parse_xml_content(self, xml_content: str) -> ET.Element:
        """Parse XML content and return root element"""
        try:
            root = ET.fromstring(xml_content)
            # Debug: Print root tag and namespaces
            print(f"Root tag: {root.tag}")
            print(f"Root attributes: {root.attrib}")
            
            # Print first few child elements to understand structure
            print("Root children:")
            for i, child in enumerate(root):
                print(f"  {i}: {child.tag} - {child.attrib}")
                if i >= 5:  # Only show first 5
                    break
            
            return root
        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")
            raise

    def get_document_type_name(self, ref_id: str) -> str:
        """Map document type reference ID to readable name"""
        doc_type_map = {
            '1571': 'Passport',
            '1584': 'National ID',
            '1608': 'Identification Number',
            '1626': 'Vessel Registration',
            '1632': 'Residency Number',
            '91264': 'MMSI',
            '91761': 'Registration Number',
            '1575': 'Driver License',
            '1576': 'Tax ID',
            '1577': 'Social Security Number',
            '1578': 'Business Registration',
            '1579': 'Military ID'
        }
        return doc_type_map.get(ref_id, f'Doc Type {ref_id}')

    def get_sanctions_program_name(self, ref_id: str) -> str:
        """Map sanctions program reference ID to readable name"""
        program_map = {
            '91901': 'IRAN-EO13902',
            '91902': 'IRAN-EO13902',
            '1556': 'UKRAINE-EO13660',
            '1557': 'UKRAINE-EO13661', 
            '1558': 'UKRAINE-EO13662',
            '1559': 'UKRAINE-EO13685',
            '1560': 'RUSSIA-EO14024',
            '1550': 'SDN',
            '1551': 'CRIM',
            '1552': 'SYRIA',
            '1553': 'CUBA',
            '1554': 'NORTH KOREA',
            '1555': 'NICARAGUA'
        }
        return program_map.get(ref_id, f'Program {ref_id}')
    
    def extract_entity_data(self, entity: ET.Element) -> Dict:
        """Extract data from a single entity"""
        data = {
            'Name': '',
            'Aliases': '',
            'Type': '',
            'Date of Birth': '',
            'Place of Birth': '',
            'Gender': '',
            'Nationality': '',
            'COUNTRY': '',
            'ID_1': '',
            'ID_Type1': '',
            'ID_2': '',
            'ID_Type2': '',
            'Date of listing': '',
            'Watchlist': '',
            'Other info': '',
            'DOB_DJ': '',
            'DOB_YEAR': ''
        }
        
        # Skip entities that are not being added
        action = entity.get('action', '')
        if action != 'add':
            return data
        
        # Extract entity type
        entity_type_elem = entity.find('.//ns:generalInfo/ns:entityType', self.namespace)
        if entity_type_elem is not None:
            ref_id = entity_type_elem.get('refId')
            if ref_id == '600':
                data['Type'] = 'Individual'
            elif ref_id == '601':
                data['Type'] = 'Entity'
            elif ref_id == '602':
                data['Type'] = 'Vessel'
            else:
                data['Type'] = f'Type {ref_id}'
        
        # Extract primary name and aliases
        names = entity.findall('.//ns:names/ns:name', self.namespace)
        aliases_list = []
        
        for name in names:
            is_primary = name.find('ns:isPrimary', self.namespace)
            
            # Get all translations for this name
            translations = name.findall('ns:translations/ns:translation', self.namespace)
            for translation in translations:
                formatted_name = translation.find('ns:formattedFullName', self.namespace)
                if formatted_name is not None and formatted_name.text:
                    if is_primary is not None and is_primary.text == 'true':
                        # Primary name
                        if not data['Name']:  # Only take the first primary name
                            data['Name'] = formatted_name.text
                    else:
                        # Alias
                        aliases_list.append(formatted_name.text)
        
        data['Aliases'] = '; '.join(aliases_list) if aliases_list else ''
        
        # Extract features (birthdate, place of birth, gender, nationality, vessel flag)
        features = entity.findall('.//ns:features/ns:feature', self.namespace)
        nationalities = []
        vessel_flag = ''
        
        # Debug vessel flag extraction
        is_vessel = data['Type'] == 'Vessel'
        if is_vessel and data['Name']:
            print(f"DEBUG: Processing vessel {data['Name']}")
            print(f"DEBUG: Found {len(features)} features")
        
        for feature in features:
            feature_type = feature.find('ns:type', self.namespace)
            if feature_type is not None:
                feature_type_id = feature_type.get('featureTypeId')
                value_elem = feature.find('ns:value', self.namespace)
                
                # Debug vessel flag specifically
                if is_vessel and data['Name'] and feature_type_id == '3':
                    print(f"DEBUG: Found vessel flag feature for {data['Name']}")
                    print(f"DEBUG: Feature type ID: {feature_type_id}")
                    print(f"DEBUG: Value element: {value_elem}")
                    if value_elem is not None:
                        print(f"DEBUG: Value text: {value_elem.text}")
                
                if value_elem is not None and value_elem.text:
                    value = value_elem.text
                    
                    if feature_type_id == '8':  # Birthdate
                        data['Date of Birth'] = value
                        # Extract DOB_DJ and DOB_YEAR
                        try:
                            date_obj = datetime.strptime(value, '%d %b %Y')
                            data['DOB_DJ'] = date_obj.strftime('%d/%m/%Y')
                            data['DOB_YEAR'] = str(date_obj.year)
                        except:
                            # Try other date formats
                            try:
                                date_obj = datetime.strptime(value, '%Y-%m-%d')
                                data['DOB_DJ'] = date_obj.strftime('%d/%m/%Y')
                                data['DOB_YEAR'] = str(date_obj.year)
                            except:
                                # Extract just the year if possible
                                year_match = re.search(r'\b(\d{4})\b', value)
                                if year_match:
                                    data['DOB_YEAR'] = year_match.group(1)
                    elif feature_type_id == '9':  # Place of Birth
                        data['Place of Birth'] = value
                    elif feature_type_id == '224':  # Gender
                        data['Gender'] = value.lower() if value.lower() in ['male', 'female'] else value
                    elif feature_type_id == '10':  # Nationality
                        nationalities.append(value)
                    elif feature_type_id == '3':  # Vessel Flag
                        vessel_flag = value
                        if is_vessel and data['Name']:
                            print(f"DEBUG: Set vessel_flag = {vessel_flag} for {data['Name']}")
        
        data['Nationality'] = '; '.join(nationalities) if nationalities else ''
        
        # Extract addresses (for country)
        addresses = entity.findall('.//ns:addresses/ns:address', self.namespace)
        countries = []
        for address in addresses:
            country_elem = address.find('ns:country', self.namespace)
            if country_elem is not None and country_elem.text:
                countries.append(country_elem.text)
        
        # Set COUNTRY field based on entity type
        if data['Type'] == 'Vessel' and vessel_flag:
            # For vessels, use vessel flag as country
            data['COUNTRY'] = vessel_flag
            if data['Name']:
                print(f"DEBUG: Set COUNTRY = {vessel_flag} for vessel {data['Name']}")
        elif data['Nationality']:
            # For individuals/entities, prefer nationality
            data['COUNTRY'] = data['Nationality'].split(';')[0].strip()
        elif countries:
            # Fallback to address country
            data['COUNTRY'] = countries[0]
        
        # Extract identity documents
        id_docs = entity.findall('.//ns:identityDocuments/ns:identityDocument', self.namespace)
        id_count = 0
        
        for doc in id_docs:
            if id_count >= 2:  # Only extract first 2 IDs
                break
                
            doc_type_elem = doc.find('ns:type', self.namespace)
            doc_number_elem = doc.find('ns:documentNumber', self.namespace)
            
            if doc_type_elem is not None and doc_number_elem is not None and doc_number_elem.text:
                doc_type_ref = doc_type_elem.get('refId')
                doc_number = doc_number_elem.text
                
                # Map document type reference to readable format
                doc_type_name = self.get_document_type_name(doc_type_ref)
                
                if id_count == 0:
                    data['ID_1'] = doc_number
                    data['ID_Type1'] = doc_type_name
                elif id_count == 1:
                    data['ID_2'] = doc_number
                    data['ID_Type2'] = doc_type_name
                
                id_count += 1
        
        # Extract sanctions list and date
        sanctions_list = entity.find('.//ns:sanctionsLists/ns:sanctionsList', self.namespace)
        if sanctions_list is not None:
            data['Watchlist'] = 'OFAC - Specially Designated National List'
            date_published = sanctions_list.get('datePublished')
            if date_published:
                try:
                    date_obj = datetime.strptime(date_published, '%Y-%m-%d')
                    data['Date of listing'] = date_obj.strftime('%d/%m/%Y')
                except:
                    data['Date of listing'] = date_published
        
        # Extract sanctions program for other info
        sanctions_programs = entity.findall('.//ns:sanctionsPrograms/ns:sanctionsProgram', self.namespace)
        program_names = []
        for program in sanctions_programs:
            program_ref = program.get('refId')
            program_name = self.get_sanctions_program_name(program_ref)
            if program_name:
                program_names.append(program_name)
        
        data['Other info'] = '; '.join(program_names) if program_names else ''
        
        return data
    
    def explore_xml_structure(self, root: ET.Element, max_depth: int = 3, current_depth: int = 0):
        """Explore XML structure to understand the format"""
        if current_depth >= max_depth:
            return
            
        indent = "  " * current_depth
        print(f"{indent}{root.tag}")
        
        # Show attributes if any
        if root.attrib:
            print(f"{indent}  Attributes: {root.attrib}")
        
        # Show text content if meaningful
        if root.text and root.text.strip():
            text_preview = root.text.strip()[:50]
            print(f"{indent}  Text: {text_preview}...")
        
        # Show children (limit to first few)
        children = list(root)
        if children:
            print(f"{indent}  Children ({len(children)}):")
            for i, child in enumerate(children[:5]):  # Only show first 5 children
                self.explore_xml_structure(child, max_depth, current_depth + 1)
                if i == 4 and len(children) > 5:
                    print(f"{indent}    ... and {len(children) - 5} more")
                    break
    
    def process_sanctions_data(self, xml_content: str) -> pd.DataFrame:
        """
        Process the XML content and extract all entities
        Returns a pandas DataFrame with the extracted data
        """
        print("Parsing XML content...")
        root = self.parse_xml_content(xml_content)
        
        # Try different ways to find entities
        print("\nTrying to find entities...")
        
        # Method 1: Direct path
        entities = root.findall('.//entity')
        print(f"Method 1 - Found {len(entities)} entities with './/entity'")
        
        # Method 2: With namespace
        entities_ns = root.findall('.//ns:entity', self.namespace)
        print(f"Method 2 - Found {len(entities_ns)} entities with namespace")
        
        # Method 3: Check if there's an entities container
        entities_container = root.find('.//entities')
        if entities_container is not None:
            entities_direct = entities_container.findall('entity')
            print(f"Method 3 - Found {len(entities_direct)} entities in container")
            entities = entities_direct
        
        # Method 4: With namespace for entities container
        entities_container_ns = root.find('.//ns:entities', self.namespace)
        if entities_container_ns is not None:
            entities_ns_direct = entities_container_ns.findall('ns:entity', self.namespace)
            print(f"Method 4 - Found {len(entities_ns_direct)} entities with namespace in container")
            if len(entities_ns_direct) > len(entities):
                entities = entities_ns_direct
        
        # If still no entities found, explore the XML structure more
        if len(entities) == 0:
            print("\nNo entities found. Exploring XML structure...")
            self.explore_xml_structure(root)
            return pd.DataFrame()
        
        print(f"Processing {len(entities)} entities...")
        
        extracted_data = []
        
        for i, entity in enumerate(entities):
            try:
                entity_data = self.extract_entity_data(entity)
                if entity_data['Name']:  # Only add entities with names
                    extracted_data.append(entity_data)
                
                if (i + 1) % 10 == 0:
                    print(f"Processed {i + 1}/{len(entities)} entities")
                    
            except Exception as e:
                print(f"Error processing entity {i}: {e}")
                continue
        
        print(f"Successfully processed {len(extracted_data)} entities")
        
        # Create DataFrame
        if extracted_data:
            df = pd.DataFrame(extracted_data)
        else:
            # Create empty DataFrame with correct columns
            columns = ['Name', 'Aliases', 'Type', 'Date of Birth', 'Place of Birth', 'Gender', 
                      'Nationality', 'COUNTRY', 'ID_1', 'ID_Type1', 'ID_2', 'ID_Type2', 
                      'Date of listing', 'Watchlist', 'Other info', 'DOB_DJ', 'DOB_YEAR']
            df = pd.DataFrame(columns=columns)
        
        return df
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        """Save DataFrame to CSV file"""
        if filename is None:
            today = datetime.now().strftime('%Y-%m-%d')
            filename = f'sanctions_cleaned_{today}.csv'
        
        df.to_csv(filename, index=False, encoding='utf-8')
        print(f"Data saved to {filename}")
        return filename
    
    def run_extraction(self, save_file: bool = True) -> pd.DataFrame:
        """
        Main method to run the complete extraction process
        """
        try:
            # Fetch latest data
            xml_content = self.fetch_latest_sanctions_data()
            
            # Process the data
            df = self.process_sanctions_data(xml_content)
            
            # Display summary
            print("\n=== EXTRACTION SUMMARY ===")
            print(f"Total records extracted: {len(df)}")
            
            if len(df) > 0:
                print(f"Entity types:")
                print(df['Type'].value_counts())
                print(f"\nWatchlists:")
                print(df['Watchlist'].value_counts())
            else:
                print("No data extracted. Please check the XML structure above.")
            
            # Save to file
            if save_file and len(df) > 0:
                filename = self.save_to_csv(df)
                print(f"\nData saved to: {filename}")
            
            return df
            
        except Exception as e:
            print(f"Error in extraction process: {e}")
            raise

# Usage example
if __name__ == "__main__":
    # Initialize extractor
    extractor = OFACSanctionsExtractor()
    
    # Run extraction
    try:
        df = extractor.run_extraction()
        
        # Display first few records
        print("\n=== SAMPLE DATA ===")
        print(df.head())
        
        # Display columns
        print(f"\nColumns: {list(df.columns)}")
        
    except Exception as e:
        print(f"Extraction failed: {e}")

# Alternative method to process from existing XML file
def process_local_xml_file(xml_file_path: str) -> pd.DataFrame:
    """
    Process sanctions data from a local XML file
    """
    extractor = OFACSanctionsExtractor()
    
    with open(xml_file_path, 'r', encoding='utf-8') as file:
        xml_content = file.read()
    
    df = extractor.process_sanctions_data(xml_content)
    filename = extractor.save_to_csv(df)
    
    print(f"Processed local file: {xml_file_path}")
    print(f"Output saved to: {filename}")
    
    return df