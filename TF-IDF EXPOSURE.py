import os
import re
import pandas as pd
from collections import Counter
import math
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

# Function to load unigrams from a txt file
def load_unigrams(txt_file):
    with open(txt_file, 'r') as file:
        unigrams = file.read().splitlines()
    print(f"Loaded {len(unigrams)} unigrams.")
    return set(unigrams)  # Using set for faster lookups

# Function to clean the text data
def clean_text(text):
    cleaned_text = re.sub(r'\s+', ' ', text)
    return cleaned_text

# Function to count unigram occurrences in a text
def count_unigrams(text, unigrams):
    words = text.split()
    total_words = len(words)
    unigram_counts = Counter(words)
    filtered_counts = {unigram: unigram_counts[unigram] for unigram in unigrams if unigram in unigram_counts}
    return filtered_counts, total_words

# Function to calculate Term Frequency (TF)
def calculate_tf(unigram_counts, total_words):
    tf_scores = {unigram: count / total_words for unigram, count in unigram_counts.items()}
    return tf_scores

# Function to calculate Inverse Document Frequency (IDF)
def calculate_idf(transcript_files, unigrams):
    N = len(transcript_files)
    idf_scores = {}
    unigram_doc_count = Counter()

    for transcript_file in transcript_files:
        with open(transcript_file, 'r', encoding='utf-8') as file:
            transcript = file.read()
        cleaned_transcript = clean_text(transcript)
        words = set(cleaned_transcript.split())
        for unigram in unigrams:
            if unigram in words:
                unigram_doc_count[unigram] += 1

    for unigram in unigrams:
        if unigram_doc_count[unigram] > 0:
            idf_scores[unigram] = math.log(N / unigram_doc_count[unigram])
        else:
            idf_scores[unigram] = 0
    return idf_scores

# Function to calculate TF-IDF
def calculate_tfidf(tf_scores, idf_scores):
    tfidf_scores = {unigram: tf * idf_scores[unigram] for unigram, tf in tf_scores.items()}
    return tfidf_scores

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

# Function to process a single file and calculate exposure
def process_file(transcript_file, unigrams, idf_scores):
    try:
        with open(transcript_file, 'r', encoding='utf-8') as file:
            transcript = file.read()
        cleaned_transcript = clean_text(transcript)
        unigram_counts, total_words = count_unigrams(cleaned_transcript, unigrams)
        tf_scores = calculate_tf(unigram_counts, total_words)
        tfidf_scores = calculate_tfidf(tf_scores, idf_scores)
        total_tfidf = sum(tfidf_scores.values())
        normalized_tfidf = total_tfidf / len(unigrams)
        return round(normalized_tfidf, 3)
    except Exception as e:
        print(f"Error processing file {transcript_file}: {e}")
        return 0

# Function to process a batch of transcripts in parallel
def process_batch_parallel(directory_path, unigrams, idf_scores, batch_files):
    exposures = []
    with ProcessPoolExecutor() as executor:
        futures = {executor.submit(process_file, os.path.join(directory_path, filename), unigrams, idf_scores): filename for filename in batch_files}
        for future in as_completed(futures):
            filename = futures[future]
            try:
                exposure = future.result()
                exposures.append(exposure)
                print(f"Exposure for {filename}: {exposure}")
            except Exception as exc:
                print(f"Error processing file {filename}: {exc}")
    return exposures

# Main function to process all transcripts and calculate TF-IDF exposure
def main(directory_path, unigram_file, input_excel, output_excel, checkpoint_file, batch_size=20):  # Reduced batch size to 20
    start_time = time.time()

    unigrams = load_unigrams(unigram_file)
    df = pd.read_excel(input_excel)
    start_batch, all_exposures = load_checkpoint(checkpoint_file)
    
    transcript_files = [os.path.join(directory_path, filename) for filename in sorted(os.listdir(directory_path)) if filename.endswith('.txt')]
    
    if start_batch == 0:
        idf_scores = calculate_idf(transcript_files, unigrams)
        save_checkpoint('idf_scores.json', 0, idf_scores)
    else:
        with open('idf_scores.json', 'r') as file:
            idf_scores = json.load(file)

    num_batches = math.ceil(len(transcript_files) / batch_size)
    
    for i in range(start_batch, num_batches):
        batch_start = i * batch_size
        batch_end = batch_start + batch_size
        batch_files = transcript_files[batch_start:batch_end]
        print(f"Processing batch {i + 1} of {num_batches}")
        batch_exposures = process_batch_parallel(directory_path, unigrams, idf_scores, batch_files)
        all_exposures.extend(batch_exposures)
        
        save_checkpoint(checkpoint_file, i + 1, all_exposures)
    
    if len(all_exposures) != len(df):
        raise ValueError("The number of exposures calculated does not match the number of rows in the DataFrame.")
    
    df['cc_expo_tfidf'] = all_exposures
    df.to_excel(output_excel, index=False)
    print(f"Data saved to {output_excel}")

    end_time = time.time()
    print(f"Total execution time: {end_time - start_time} seconds")

if __name__ == "__main__":
    directory_path = r'/Users/mikiokilo/Downloads/pseudo_transcripts_txt'
    unigram_file = r'/Users/mikiokilo/Downloads/general_unigrams.txt'
    input_excel = r'/Users/mikiokilo/Downloads/output_with_new_columns.xlsx'
    output_excel = r'/Users/mikiokilo/Downloads/Final_Exposure_Outputs/updated_output5.xlsx'
    checkpoint_file = r'/Users/mikiokilo/Downloads/checkpoint.json'

    main(directory_path, unigram_file, input_excel, output_excel, checkpoint_file, batch_size=20)  # Reduced batch size
