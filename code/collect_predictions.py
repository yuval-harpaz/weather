import requests
import pandas as pd
import xml.etree.ElementTree as ET
import os
import sys
from io import StringIO

# Constants
XML_URL = 'https://ims.gov.il/sites/default/files/ims_data/xml_files/isr_cities.xml'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
OUTPUT_CSV = os.path.join(DATA_DIR, 'predictions.csv')
WEATHER_CODE_CSV = os.path.join(DATA_DIR, 'weather_code.csv')

def load_weather_codes():
    """Loads weather codes and their Hebrew descriptions."""
    if not os.path.exists(WEATHER_CODE_CSV):
        print(f"Warning: {WEATHER_CODE_CSV} not found. Weather descriptions will be missing.")
        return {}
    
    try:
        # Load mapping content
        df = pd.read_csv(WEATHER_CODE_CSV)
        # Create a dictionary mapping code to Hebrew description
        # Ensure column names match what we saw in the previous turn: "Code","מזג האוויר","Weather"
        # We might need to handle column name variations so we'll look for 'Code' and Hebrew text
        
        # Assuming the CSV has headers as seen in previous steps
        code_map = {}
        for index, row in df.iterrows():
            code_map[str(row['Code'])] = row['מזג האוויר']
        return code_map
    except Exception as e:
        print(f"Error loading weather codes: {e}")
        return {}

def fetch_and_parse_xml():
    """Fetches the XML from the URL and parses it."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(XML_URL, headers=headers, timeout=10)
        response.raise_for_status()
        # IMS XMLs are often ISO-8859-8 encoded
        response.encoding = 'ISO-8859-8'
        return ET.fromstring(response.text)
    except Exception as e:
        print(f"Error fetching/parsing XML: {e}")
        sys.exit(1)

def extract_data(root, weather_codes):
    """Extracts relevant data from the XML root element."""
    predictions = []
    
    # Extract IssueDateTime
    identification = root.find('Identification')
    if identification is None:
        print("Error: Could not find Identification tag in XML.")
        sys.exit(1)
        
    issue_date_time = identification.find('IssueDateTime').text
    
    # Check if we already have this IssueDateTime
    if os.path.exists(OUTPUT_CSV):
        try:
            #Read just the IssueDateTime column to check quickly
            existing_df = pd.read_csv(OUTPUT_CSV, usecols=['IssueDateTime'])
            if issue_date_time in existing_df['IssueDateTime'].values:
                print(f"Predictions for IssueDateTime {issue_date_time} already exist. Exiting.")
                sys.exit(0)
        except Exception as e:
            # If CSV exists but is empty or corrupted, we might proceed or error.
            # Choosing to proceed and just append could lead to duplicates if malformed,
            # but usually restarting is safer.
            print(f"Warning checking existing CSV: {e}")

    print(f"Processing forecast for: {issue_date_time}")

    for location in root.findall('Location'):
        meta = location.find('LocationMetaData')
        loc_name_eng = meta.find('LocationNameEng').text
        loc_name_heb = meta.find('LocationNameHeb').text
        
        loc_data = location.find('LocationData')
        for time_unit in loc_data.findall('TimeUnitData'):
            date = time_unit.find('Date').text
            
            # Initialize record
            record = {
                'IssueDateTime': issue_date_time,
                'Date': date,
                'LocationNameHeb': loc_name_heb,
                'LocationNameEng': loc_name_eng,
                'Minimum temperature': None,
                'Maximum temperature': None,
                'code': None,
                'HebrewWeatherCode': None
            }
            
            for element in time_unit.findall('Element'):
                elem_name = element.find('ElementName').text
                elem_value = element.find('ElementValue').text
                
                if elem_name == 'Minimum temperature':
                    record['Minimum temperature'] = elem_value
                elif elem_name == 'Maximum temperature':
                    record['Maximum temperature'] = elem_value
                elif elem_name == 'Weather code':
                    record['code'] = elem_value
                    record['HebrewWeatherCode'] = weather_codes.get(elem_value, '')
            
            predictions.append(record)
            
    return predictions

def main():
    weather_codes = load_weather_codes()
    root = fetch_and_parse_xml()
    data = extract_data(root, weather_codes)
    
    if not data:
        print("No data extracted.")
        return

    df = pd.DataFrame(data)
    
    # Ensure columns order
    columns = ['IssueDateTime', 'Date', 'LocationNameHeb', 'LocationNameEng', 
               'Minimum temperature', 'Maximum temperature', 'code', 'HebrewWeatherCode']
    
    # Reorder if necessary, though dict keys should maintain order in modern Python, explicit is better
    df = df[columns]

    if not os.path.exists(OUTPUT_CSV):
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
        print(f"Created {OUTPUT_CSV} with {len(df)} rows.")
    else:
        df.to_csv(OUTPUT_CSV, mode='a', header=False, index=False, encoding='utf-8-sig')
        print(f"Appended {len(df)} rows to {OUTPUT_CSV}.")

if __name__ == "__main__":
    main()
