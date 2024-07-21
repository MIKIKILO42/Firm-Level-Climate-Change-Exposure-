# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 17:16:53 2024

@author: aranyeok
"""

import pandas as pd
import pyreadstat

# Load the .dta file
file_path = '\\lancs\homes\47\aranyeok\My Documents\Replication Files Sautner et al. (2023)\A. Main Tables and Figures\data'  # Update with the correct path to your file
df, meta = pyreadstat.read_dta(file_path)

# Display the first few rows of the dataframe
print(df.head())

# Display the metadata
print(meta.column_names)
print(meta.column_labels)

import pandas as pd
import pyreadstat

# Load the .dta file
file_path = r'\\lancs\homes\47\aranyeok\My Documents\Replication Files Sautner et al. (2023)\A. Main Tables and Figures\data\SvLVZ_OI_pseudo.dta'
df, meta = pyreadstat.read_dta(file_path)

# Display the first few rows of the dataframe
print(df.head())

# Display the metadata
print(meta.column_names)
print(meta.column_labels)

# Save the dataframe to an Excel file
output_file_path = r'\\lancs\homes\47\aranyeok\My Documents\Replication Files Sautner et al. (2023)\A. Main Tables and Figures\data\output_file2.xlsx'
df.to_excel(output_file_path, index=False)
print(f"Data saved to {output_file_path}")

import pickle

# Function to read a pickle file
def read_pickle_file(file_path):
    with open(file_path, 'rb') as file:
        data = pickle.load(file)
    return data

# Path to your pickle file
pickle_file_path = r'\\lancs\homes\47\aranyeok\My Documents\Replication Files Sautner et al. (2023)\B. Figure 1 2, Table 2, and IA Table 6 7 8 9 11\bigrams\opportunity_bigrams_4.pkl'  # Replace with your file path

# Read the pickle file
data = read_pickle_file(pickle_file_path)

# Print the contents of the pickle file
print(data)
