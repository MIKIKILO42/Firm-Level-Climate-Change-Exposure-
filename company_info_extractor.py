import os
import re
import pandas as pd

def get_file_list(directory):
    """Get a list of JSON files in the specified directory."""
    return [os.path.join(directory, file) for file in os.listdir(directory) if file.lower().endswith('json')]

def extract_info_from_file(file_path):
    """Extract company ID, company name, and earnings call period from a JSON file."""
    with open(file_path, 'r') as file:
        content = file.read()
    company_id_pattern = r'"companyid":\s*(\d+)'
    company_name_pattern = r'"companyname":\s*"([^"]+)"'
    earnings_call_period_pattern = r'\bQ[1-4]\s\d{4}\b'
    
    company_id = re.findall(company_id_pattern, content)
    company_name = re.findall(company_name_pattern, content)
    earnings_call_period = re.findall(earnings_call_period_pattern, content)
    
    company_id = company_id[0] if company_id else 'N/A'
    company_name = company_name[0] if company_name else 'N/A'
    earnings_call_period = earnings_call_period[0] if earnings_call_period else 'N/A'
    
    return company_id, company_name, earnings_call_period

# Example usage
directory = r'\\lancs\homes\47\aranyeok\My Documents\Replication Files Sautner et al. (2023)\B. Figure 1 2, Table 2, and IA Table 6 7 8 9 11\pseudo_transcripts'
file_list = get_file_list(directory)

# Initialize lists to store the extracted information
company_ids = []
company_names = []
earnings_call_periods = []

# Loop through the files and extract information
for file_path in file_list:
    company_id, company_name, earnings_call_period = extract_info_from_file(file_path)
    company_ids.append(company_id)
    company_names.append(company_name)
    earnings_call_periods.append(earnings_call_period)

# Create a DataFrame from the extracted information
final_output = pd.DataFrame({
    'file': file_list,
    'company_id': company_ids,
    'company_name': company_names,
    'earnings_call_period': earnings_call_periods
})

# Save the DataFrame to a CSV file
final_output.to_csv('final_output.csv', index=False)
print(final_output)
