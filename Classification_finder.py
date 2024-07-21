


import pandas as pd
import re

# Load the Excel file
file_path = r'updated_output1_with_sectors.xlsx'  # replace with your file path
df = pd.read_excel(file_path)

# Define the keywords and their corresponding sector classifications
keywords_to_sector = {
    'TECHNOLOGIES': 'Technology',
    'TECHNOLOGY': 'Technology',
    'BIOTECHNOLOGY': 'Healthcare',
    'SOFTWARE': 'Technology',
    'SYSTEM': 'Technology',
    'HEALTH': 'Healthcare',
    'PHARMA': 'Healthcare',
    'PHARMACEUTICALS': 'Healthcare',
    'ENERGY': 'Energy',
    'ENERGIES': 'Energy',
    'INDUSTRIES': 'Industrials',
    'INDUSTRIAL': 'Industrials',
    'AIR': 'Industrials',
    'POWER': 'Industrials',
    'CAPITAL': 'Financials',
    'ASSET': 'Financials',
    'EXCHANGE': 'Financials',
    'SYSTEMS': 'Industrials',
    'PROPERTY': 'Real Estate',
    'FINANCIAL': 'Financials',
    'FINANCE': 'Financials',
    'MATERIALS': 'BASIC MATERIALS',
    'INVESTOR': 'Financials',
    'INVESTMENT': 'Financials',
    'GAMING': 'TECHNOLOGY',
    'ELECTR': 'INDUSTRIALS',
    'GOLD': 'INDUSTRIALS'
}

# Ensure there are no missing values in the company_name column
df['company_name'] = df['company_name'].astype(str).fillna('')

# Step 1: Replicate sector classifications for duplicates
sector_mapping = df[['company_name', 'LEVEL 2 SECTOR CLASSIFICATION']].dropna().drop_duplicates().set_index('company_name')['LEVEL 2 SECTOR CLASSIFICATION'].to_dict()
df['LEVEL 2 SECTOR CLASSIFICATION'] = df['company_name'].map(sector_mapping).fillna(df['LEVEL 2 SECTOR CLASSIFICATION'])

# Step 2: Assign sector based on keywords
def assign_sector(company_name, current_sector):
    if pd.notna(current_sector):
        return current_sector
    for keyword, sector in keywords_to_sector.items():
        if re.search(r'\b' + re.escape(keyword) + r'\b', company_name.upper()):
            return sector
    return current_sector

df['LEVEL 2 SECTOR CLASSIFICATION'] = df.apply(lambda row: assign_sector(row['company_name'], row['LEVEL 2 SECTOR CLASSIFICATION']), axis=1)

# Save the updated DataFrame back to Excel
output_file_path = 'updated_output2_with_sectors.xlsx'
df.to_excel(output_file_path, index=False)

print(f"Updated file saved to {output_file_path}")



####################################################

import pandas as pd

# Load the Excel file
file_path = 'updated_output_with_sector_classification.xlsx'
df = pd.read_excel(file_path)

# Extract known classifications
known_sector_classifications = df.dropna(subset=['LEVEL 2 SECTOR CLASSIFICATION']).drop_duplicates(subset=['company_name'], keep='first')[['company_name', 'LEVEL 2 SECTOR CLASSIFICATION']]

# Merge back into original DataFrame to fill missing classifications
df = df.merge(known_sector_classifications, on='company_name', how='left', suffixes=('', '_known'))

# Update LEVEL 2 SECTOR CLASSIFICATION where it is NaN
df['LEVEL 2 SECTOR CLASSIFICATION'] = df['LEVEL 2 SECTOR CLASSIFICATION'].combine_first(df['LEVEL 2 SECTOR CLASSIFICATION_known'])
df.drop(columns=['LEVEL 2 SECTOR CLASSIFICATION_known'], inplace=True)

# Save the updated DataFrame
output_file_path = 'updated_output_with_sector_classification_final.xlsx'
df.to_excel(output_file_path, index=False)

print(f"Updated file saved as {output_file_path}")
