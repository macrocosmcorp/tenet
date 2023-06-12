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
def get_embedding(text: str, model="text-embedding-ada-002") -> list[float]:
    return openai.Embedding.create(input=[text], model=model)["data"][0]["embedding"]


def load_atlas(project_name, df, colorable_fields):
    # Print column names
    print(df.columns)

    # Get all columns except for Embedding and Text
    columns = df.columns
    columns = columns.drop('Embedding')
    columns = columns.drop('Text')
    print(columns)

    # Join them together to create ID
    # Only if ID doesn't already exist
    if 'ID' not in df.columns:
        df['ID'] = df[columns].agg(' '.join, axis=1)
        df.set_index('ID', inplace=True)
    print(df.head())

    embeddings = df['Embedding']
    df.drop(columns=['Embedding'], inplace=True)

    embeddings = np.array([np.array(x) for x in embeddings])
    project = atlas.map_embeddings(name=project_name, is_public=True,
                                   embeddings=embeddings, data=df, id_field='ID', colorable_fields=colorable_fields,
                                   reset_project_if_exists=True)
    print(project.maps)


def read_csv_file(file_path):
    lines = []
    with open(file_path, 'r', newline='') as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            lines.append(row)
    return lines


def embed_file(csv_file_name, csv_file_path, csv_directory):
    result = read_csv_file(csv_file_path)
    print("Length of result: ", len(result))
    print(result[0])

    # Add the embeddings to the dataframe
    df = pd.DataFrame(result)

    # Print column names
    print(df.columns)

    # Get the embedding for each verse
    embeddings = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_embedding = {executor.submit(
            get_embedding, verse['Text']): verse for verse in result}
        for future in concurrent.futures.as_completed(future_to_embedding):
            embeddings.append(future.result())
            print(len(embeddings), end='\r')

    df['Embedding'] = embeddings

    # Write to parquet
    parquet_file_name = csv_file_name.replace('.csv', '.parquet')
    parquet_file_path = os.path.join(
        script_dir, csv_directory, parquet_file_name)
    df.to_parquet(parquet_file_path)

    # Verify results from parquet
    table = pq.read_table(parquet_file_path)
    df = table.to_pandas()
    print(df.head())


if __name__ == "__main__":
    file_directory = 'religious-texts/islam'
    file_name = 'quran_chunks.parquet'

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_directory, file_name)
    print("Running for file path: ", file_path)

    table = pq.read_table(file_path)
    df = table.to_pandas()
    # Remove 'Section' suffix from section names
    df['Section'] = df['Section'].str.replace(
        'SECTION', 'S')
    df['ID'] = df['Sura'] + ' ' + df['Section'] + ' ' + df['ParagraphRange']
    df['Section'] = df['Sura'] + ' ' + df['Section']
    load_atlas(file_name.removesuffix('.parquet'), df, ['Sura', 'Section'])

    # embed_file(file_name, file_path, file_directory)
