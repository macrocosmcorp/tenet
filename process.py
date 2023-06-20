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

total_cost = 0

@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
def get_embedding(text: str, id: int, model="text-embedding-ada-002") -> tuple[int, list[float]]:
    result = openai.Embedding.create(input=[text], model=model)
    return (id, result["data"][0]["embedding"])

@retry(wait=wait_random_exponential(min=1, max=20), stop=stop_after_attempt(6))
def get_summary(text: str, id: int) -> tuple[int, str]:
    result = openai.ChatCompletion.create(
    model="gpt-3.5-turbo",
    messages=[{"role": "system", "content": "You are a Bible expert. You are great at writing children's books and stories and explaining complex information well and in a careful manner. Below is a certain chapter from the Bible. Rewrite the text and make it simpler it in a way that a child would understand, without losing information or changing the meaning of the text. Try as much as you can to keep the structure and length of each chapter. Don't start each summary with any unique phrase as each summary will be connected to each other (for example, 'Once upon a time' or 'Genesis Chapter 1:'). Return only the inline simplified text."}, {"role": "user", "content": text}],
    )["choices"][0]["message"]['content']
    # total_cost += 0.0000015 * result['usage']['prompt_tokens']
    # total_cost += 0.000002 * result['usage']['completion_tokens']
    
    return (id, result["choices"][0]["message"]['content'])


def load_atlas(project_name, df, colorable_fields, reset_project_if_exists=False):
    # Print column names
    print(df.columns)

    # Get all columns except for Embedding and Text
    columns = df.columns
    columns = columns.drop('Embedding')
    columns = columns.drop('Text')
    print(columns)

    # Join them together to create ID
    # Only if ID doesn't already exist
    # if 'ID' not in df.columns:
    #     df['ID'] = df[columns].agg(' '.join, axis=1)
    #     df.set_index('ID', inplace=True)
    print(df.head())

    embeddings = df['Embedding']
    df.drop(columns=['Embedding'], inplace=True)

    embeddings = np.array([np.array(x) for x in embeddings])
    project = atlas.map_embeddings(name=project_name, is_public=True,
                                   embeddings=embeddings, data=df, id_field='ID', colorable_fields=colorable_fields,
                                   reset_project_if_exists=reset_project_if_exists, add_datums_if_exists=( not reset_project_if_exists))
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

    # Write to parquet
    parquet_file_name = csv_file_name.replace('.csv', '.parquet')
    parquet_file_path = os.path.join(
        script_dir, csv_directory, parquet_file_name)
    df.to_parquet(parquet_file_path)

    # Verify results from parquet
    table = pq.read_table(parquet_file_path)
    df = table.to_pandas()
    print(df.head())

def summarize_file(csv_file_name, csv_file_path, csv_directory):
    result = read_csv_file(csv_file_path)[:30]
    print("Length of result: ", len(result))
    print(result[0])

    # Add the embeddings to the dataframe
    df = pd.DataFrame(result)

    # Print column names
    print(df.columns)

    # Get the embedding for each verse
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future_to_embedding = {executor.submit(
            get_summary, verse["Book"] + ' Chapter ' + verse['Chapter'] + ': ' + verse['Text'], id): verse for id, verse in enumerate(result)}
        for future in concurrent.futures.as_completed(future_to_embedding):
            results.append(future.result())
            print(len(results), end='\r')

    # Sort the results by ID
    results.sort(key=lambda x: x[0])
    results = [x[1] for x in results]

    # Save results to dataframe
    df['Summary'] = results

    # Write to csv
    csv_file_name = csv_file_name.replace('.csv', '_summarized.csv')
    csv_file_path = os.path.join(
        script_dir, csv_directory, csv_file_name)
    df.to_csv(csv_file_path)

    # Write only the summary to a text file
    text_file_name = csv_file_name.replace('.csv', '_summarized.txt')
    text_file_path = os.path.join(
        script_dir, csv_directory, text_file_name)
    with open(text_file_path, 'w') as f:
        for result in results:
            f.write(result + '\n')

    # Verify results from csv
    df = pd.read_csv(csv_file_path)
    print(df.head())
    
if __name__ == "__main__":
    file_directory = 'embeddings/hadith_embeddings'
    file_name = 'hadith.csv'

    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, file_directory, file_name)
    print("Running for file path: ", file_path)

    # table = pq.read_table(file_path)
    # df = table.to_pandas()

    # summarize_file(file_name, file_path, file_directory)

    # df['Source'] = 'Kitab'
    # df['ID'] = df['Source'] + ' ' + df.index.astype(str)
    # df['ColorableChapter'] = df['ColorableChapter'].str.replace('/Users/willdepue/project-tenet/religious-texts/', '')
    # df['ColorableBook'] = df['ColorableBook'].str.replace('/Users/willdepue/project-tenet/religious-texts/', '')
    # print(df.head())
    # parquet_file_name = file_path.replace('.parquet', '_modified.parquet')
    # df.to_parquet(parquet_file_name)

    # load_atlas('all_islam', df, ['ColorableBook', 'ColorableChapter', 'Source'], True)

    embed_file(file_name, file_path, file_directory)

    # print('Total cost:', total_cost)



def atlas(file_path):
    # # Modify for atlas upload

    table = pq.read_table(file_path)
    df = table.to_pandas()
    print(df.head())

    # # Rename Paragraph to VerseRange
    # df.rename(columns={'Section': 'Book', 'Part': 'Chapter', 'ParagraphRange': 'VerseRange'}, inplace=True)

    # # Add ID filed by splitting file_path by '_' and using first part
    # df['Source'] = 'test'
    # df['ColorableBook'] = df['Source'] + ' ' + df['Book']
    # df['ColorableChapter'] = df['Source'] + ' ' + df['Book'] + ' ' + df['Chapter']


    # # Write to parquet
    # parquet_file_name = file_path.replace('.parquet', '_modified.parquet')
    # df.to_parquet(parquet_file_name)

    # # Verify results from parquet
    # table = pq.read_table(parquet_file_name)
    # df = table.to_pandas()
    # print(df.head())
    # print(df.columns)
