import os
import re
import pandas as pd
from collections import Counter
from math import ceil
import json

# Function to load unigrams from a txt file
def load_unigrams(txt_file):
    with open(txt_file, 'r') as file:
        unigrams = file.read().splitlines()
    print(f"Loaded {len(unigrams)} unigrams.")
    return unigrams

# Function to clean the text data
def clean_text(text):
    # Remove extra spaces
    cleaned_text = re.sub(r'\s+', ' ', text)
    return cleaned_text

# Function to count unigram occurrences in a text
def count_unigrams(text, unigrams):
    # Use regular expressions to find unigrams
    unigram_counts = Counter()
    for unigram in unigrams:
        unigram_counts[unigram] = len(re.findall(r'\b' + re.escape(unigram) + r'\b', text))
    return unigram_counts

# Function to calculate equal-weighted exposure
def calculate_equal_weighted_exposure(unigram_counts, total_unigrams):
    total_count = sum(unigram_counts.values())
    if total_count == 0:
        return 0
    exposure = total_count / total_unigrams
    return exposure

# Function to process a single file and calculate exposure
def process_file(transcript_file, unigrams):
    # Load and clean the transcript
    with open(transcript_file, 'r', encoding='utf-8') as file:
        transcript = file.read()
    cleaned_transcript = clean_text(transcript)
    
    # Count unigram occurrences
    unigram_counts = count_unigrams(cleaned_transcript, unigrams)
    
    # Calculate exposure
    exposure = calculate_equal_weighted_exposure(unigram_counts, len(unigrams))
    
    return exposure

# Function to process a batch of transcripts
def process_batch(directory_path, unigrams, batch_files):
    exposures = []
    for filename in batch_files:
        transcript_file = os.path.join(directory_path, filename)
        print(f"Processing file: {transcript_file}")
        exposure = process_file(transcript_file, unigrams)
        exposures.append(exposure)
        print(f"Exposure for {filename}: {exposure}")
    return exposures

# Function to save progress to a checkpoint file
def save_checkpoint(checkpoint_file, batch_number, exposures):
    checkpoint_data = {
        'batch_number': batch_number,
        'exposures': exposures
    }
    with open(checkpoint_file, 'w') as file:
        json.dump(checkpoint_data, file)
    print(f"Checkpoint saved at batch {batch_number}")

# Function to load progress from a checkpoint file
def load_checkpoint(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as file:
            checkpoint_data = json.load(file)
        print(f"Resuming from batch {checkpoint_data['batch_number']}")
        return checkpoint_data['batch_number'], checkpoint_data['exposures']
    else:
        return 0, []

# Main function to process all transcripts in batches and update the Excel file
def main(directory_path, unigram_file, input_excel, output_excel, checkpoint_file, batch_size=100):
    # Load unigrams
    unigrams = load_unigrams(unigram_file)
    
    # Read the input Excel file
    df = pd.read_excel(input_excel)
    
    # Ensure the number of transcripts matches the number of rows in the DataFrame
    txt_files = sorted([filename for filename in os.listdir(directory_path) if filename.endswith('.txt')])
    if len(txt_files) != len(df):
        raise ValueError("The number of transcript files does not match the number of rows in the Excel file.")
    
    # Start from batch 1
    start_batch = 0
    all_exposures = []

    # Split files into batches
    num_batches = ceil(len(txt_files) / batch_size)
    
    for i in range(start_batch, num_batches):
        batch_files = txt_files[i * batch_size:(i + 1) * batch_size]
        print(f"Processing batch {i + 1} of {num_batches}")
        batch_exposures = process_batch(directory_path, unigrams, batch_files)
        all_exposures.extend(batch_exposures)
        
        # Save progress after each batch
        save_checkpoint(checkpoint_file, i + 1, all_exposures)
    
    # Ensure the number of exposures matches the number of rows in the DataFrame
    if len(all_exposures) != len(df):
        raise ValueError("The number of exposures calculated does not match the number of rows in the DataFrame.")
    
    # Update the DataFrame with the calculated exposures
    df['ph_expo_ew'] = all_exposures
    
    # Save the updated DataFrame to a new Excel file
    df.to_excel(output_excel, index=False)
    print(f"Data saved to {output_excel}")

if __name__ == "__main__":
    directory_path = r'/Users/mikiokilo/Downloads/pseudo_transcripts_txt'  # Update with your JSON folder path
    unigram_file = r'/Users/mikiokilo/Downloads/physical_unigrams.txt'  # Update with your unigram txt file path
    input_excel = r'/Users/mikiokilo/Downloads/output_with_new_columns.xlsx'  # Input Excel file path
    output_excel = r'/Users/mikiokilo/Downloads/Final_Exposure_Outputs/updated_output4.xlsx'  # Output Excel file path
    checkpoint_file = r'/Users/mikiokilo/Downloads/checkpoint.json'  # Checkpoint file path

    # Remove the checkpoint file to start from scratch
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)

    main(directory_path, unigram_file, input_excel, output_excel, checkpoint_file, batch_size=100)
