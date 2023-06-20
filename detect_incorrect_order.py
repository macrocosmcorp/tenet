import concurrent.futures
import csv
import os

import dotenv
import numpy as np
import openai
import pandas as pd
import pyarrow.parquet as pq
from nomic import atlas
from tenacity import retry, stop_after_attempt, wait_random_exponential

# API key stored in environment variable
dotenv.load_dotenv()

openai.api_key = os.getenv("OPENAI_API_KEY")

@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
def get_embedding(text: str, id: int, model="text-embedding-ada-002") -> tuple[int, list[float]]:
    result = openai.Embedding.create(input=[text], model=model)
    return (id, result["data"][0]["embedding"])


def read_csv_file(file_path):
    lines = []
    with open(file_path, 'r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            lines.append(row)
    return lines


def embed_file(csv_file, embed_file):
    result = read_csv_file(csv_file)[:10]

    # Add the embeddings to the dataframe
    df = pd.DataFrame(result)

    # Get the embedding for each verse
    embeddings = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        future_to_embedding = {executor.submit(
            get_embedding, verse['Text'], id): verse for id, verse in enumerate(result)}
        for future in concurrent.futures.as_completed(future_to_embedding):
            embeddings.append(future.result())
            print(len(embeddings), end='\r')

    # Sort the embeddings by ID
    embeddings.sort(key=lambda x: x[0])
    embeddings = [x[1] for x in embeddings]

    df['Embedding'] = embeddings

    # load existing embeddings from embed_file
    table = pq.read_table(embed_file)[:10]
    df2 = table.to_pandas()
    # compare the csv file with the existing embeddings
    if not df.equals(df2):
        print(embed_file.split('/')[-1])


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    texts_directory = os.path.join(script_dir, 'religious-texts')
    embeddings_directory = os.path.join(script_dir, 'embeddings')

    # Get all the files in the directory
    texts = []
    for root, directories, filenames in os.walk(texts_directory):
        for filename in filenames:
            if filename.endswith('.csv'):
                texts.append(os.path.join(root, filename))

    embeddings = []
    for root, directories, filenames in os.walk(embeddings_directory):
        for filename in filenames:
            if filename.endswith('.parquet'):
                embeddings.append(os.path.join(root, filename))
    
    # create a list of tuples of corresponding embeddings and csv files with full paths but compare only the filenames
    files = []
    for text in texts:
        for embedding in embeddings:
            if text.split('/')[-1].replace('.csv', '') == embedding.split('/')[-1].replace('.parquet', ''):
                files.append((text, embedding))

    for text, embedding in files:
        embed_file(text, embedding)