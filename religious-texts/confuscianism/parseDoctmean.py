
import csv
import os

import pandas as pd


def split_by_paragraph(filename):
    with open(filename, 'r') as f:
        content = f.readlines()

    # this book has no chapters
    # just has paragraphs
    # each paragraph has a line break

    paragraphs = []
    text = ''
    for line in content:
        if line.strip():
            text += line.strip() + ' '
        else:
            paragraphs.append(text)
            text = ''

    return paragraphs
    
def split_by_chunk(verses, chunk_size, overlap_size):
    chunks = []
    chunk = []
    for i in range(len(verses)):
        verse = verses[i]
        chunk.append(verse)
        if (i + 1) % chunk_size == 0:
            chunks.append(chunk)
            chunk = []
        elif (i + 1) % chunk_size == overlap_size:
            chunk = chunk[-overlap_size:]
    if chunk:
        chunks.append(chunk)
    return chunks

# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Put the file in 'religious-texts/christianity' relative to the script directory
file_path = os.path.join(
    script_directory, 'CLEAN_doctmean_confucius.txt')

# Set the output directory to the script directory
output_directory = script_directory

religion_name = 'confucianism'
book_name = 'doctrine_of_the_mean'
# Set this to {religion_name}_{book_name}
id_flag = religion_name + '_' + book_name

paragraphs = split_by_paragraph(file_path)
chunks = split_by_chunk(paragraphs, 6, 2)

# Write to temp csv files
verses_csv_path = os.path.join(output_directory, f'{id_flag}_verses.csv')
chunks_csv_path = os.path.join(output_directory, f'{id_flag}_chunks.csv')

# use csv.writer
with open(verses_csv_path, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['paragraph', 'text'])
    for i in range(len(paragraphs)):
        writer.writerow([ i+1, paragraphs[i]])

with open(chunks_csv_path, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['chunk', 'text'])
    for i in range(len(chunks)):
        writer.writerow([ i+1, ' '.join(chunks[i])])