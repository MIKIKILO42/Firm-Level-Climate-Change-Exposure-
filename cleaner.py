import pandas as pd
import re

def clean_company_name(name):
    name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def standardize_case(name):
    return name.title()

def remove_suffixes(name):
    suffixes = ['Inc', 'Corp', 'Ltd', 'LLC', 'PLC', 'Co', 'Limited']
    pattern = r'\b(?:' + '|'.join(suffixes) + r')\b'
    return re.sub(pattern, '', name, flags=re.IGNORECASE).strip()

def handle_abbreviations(name):
    abbreviations = {
        'Intl': 'International',
        'Tech': 'Technology',
        'Sys': 'Systems',
        'Mfg': 'Manufacturing',
        'Inds': 'Industries'
    }
    for abbr, full in abbreviations.items():
        name = re.sub(r'\b' + abbr + r'\b', full, name, flags=re.IGNORECASE)
    return name

def format_company_name(name):
    name = clean_company_name(name)
    name = standardize_case(name)
    name = remove_suffixes(name)
    name = handle_abbreviations(name)
    return name

# Load the list of company names
company_data = pd.read_csv('final_output.csv')

# Apply formatting to each company name
company_data['formatted_name'] = company_data['company_name'].apply(format_company_name)

# Save the formatted names to a new CSV file
company_data.to_csv('formatted_company_ids.csv', index=False)
