import os

import pandas as pd


# Ask GPT-4 on how to write these, no reason to do by hand. Just make sure that it's right!! If this is messed up it'll screw up embedding process.
def split_by_verse(filename):
    with open(filename, 'r') as f:
        content = f.readlines()

    verses = []
    for line in content:
        if line.strip():
            # Extract the book and chapter and verse from the line
            # EX: 'Genesis 1:20' -> ['Genesis', '1', '20']

            # Split on the first space
            split = line.split(' ')

            # I
            if split[0].isnumeric():
                book = ' '.join(split[:2])
                chapter_verse_num = split[2]
                verse = ' '.join(split[3:])
            elif split[0] == 'Song':
                book = ' '.join(split[:3])
                chapter_verse_num = split[3]
                verse = ' '.join(split[4:])
            else:
                book = split[0]
                chapter_verse_num = split[1]
                verse = ' '.join(split[2:])

            chapter_num, verse_num = chapter_verse_num.split(':')

            # Remove the trailing newline
            verse = verse.strip()
            book = book.strip()

            verses.append([book, chapter_num, verse_num, verse])

    return verses


def split_by_chapter(filename):
    with open(filename, 'r') as f:
        content = f.readlines()

    book_chapter_dict = {}
    book_chapter_key = ""
    for line in content:
        if line.strip():
            # Extract the book and chapter and verse from the line
            # EX: 'Genesis 1:20' -> ['Genesis', '1', '20']

            # Split on the first space
            split = line.split(' ')

            # I
            if split[0].isnumeric():
                book = ' '.join(split[:2])
                chapter_verse_num = split[2]
                verse = ' '.join(split[3:])
            elif split[0] == 'Song':
                book = ' '.join(split[:3])
                chapter_verse_num = split[3]
                verse = ' '.join(split[4:])
            else:
                book = split[0]
                chapter_verse_num = split[1]
                verse = ' '.join(split[2:])

            chapter_num, verse_num = chapter_verse_num.split(':')

            # Remove the trailing newline
            verse = verse.strip()
            book = book.strip()

            if book_chapter_key != book + chapter_num:
                book_chapter_key = book + chapter_num
                book_chapter_dict[book_chapter_key] = []

            book_chapter_dict[book_chapter_key].append(
                [book, chapter_num, verse_num, verse])

    # Convert the dictionary to a list
    chapters = []
    for key in book_chapter_dict:
        item = book_chapter_dict[key]
        chapters.append([item[0][0], item[0][1], ' '.join(
            [verse[3] for verse in item])])

    return chapters


def split_by_chunk(filename, chunk_size, overlap_size, align_to_book=True, align_to_chapter=True):
    with open(filename, 'r') as f:
        content = f.readlines()

    # Book -> Chapter -> Verses
    book_chapter_dict_nested = {}
    book_key = ""
    chapter_key = ""
    for line in content:
        if line.strip():
            # Extract the book and chapter and verse from the line
            # EX: 'Genesis 1:20' -> ['Genesis', '1', '20']

            # Split on the first space
            split = line.split(' ')

            # I
            if split[0].isnumeric():
                book = ' '.join(split[:2])
                chapter_verse_num = split[2]
                verse = ' '.join(split[3:])
            elif split[0] == 'Song':
                book = ' '.join(split[:3])
                chapter_verse_num = split[3]
                verse = ' '.join(split[4:])
            else:
                book = split[0]
                chapter_verse_num = split[1]
                verse = ' '.join(split[2:])

            chapter_num, verse_num = chapter_verse_num.split(':')

            # Remove the trailing newline
            verse = verse.strip()
            book = book.strip()

            print(book, chapter_num, verse_num, verse)

            if book_key != book:
                book_key = book
                book_chapter_dict_nested[book_key] = {}
                chapter_key = ''
            if chapter_key != chapter_num:
                chapter_key = chapter_num
                book_chapter_dict_nested[book_key][chapter_key] = []

            book_chapter_dict_nested[book_key][chapter_key].append(
                [book, chapter_num, verse_num, verse])

    # Convert the dictionary to a chunked list.
    # Structure: Book, Chapter, VerseRange, Text
    chunks = []
    for book_key in book_chapter_dict_nested:
        book = book_chapter_dict_nested[book_key]
        for chapter_key in book:
            chapter = book[chapter_key]
            for i in range(0, len(chapter), chunk_size - overlap_size):
                chunk = chapter[i:i + chunk_size]
                chunks.append([chunk[0][0], chunk[0][1], f'{chunk[0][2]}-{chunk[-1][2]}', ' '.join(
                    [verse[3] for verse in chunk])])

    return chunks


# Get the directory of the script
script_directory = os.path.dirname(os.path.abspath(__file__))

# Put the file in 'religious-texts/christianity' relative to the script directory
file_path = os.path.join(
    script_directory, 'CLEAN_kjv.txt')

# Set the output directory to the script directory
output_directory = script_directory

religion_name = 'christianity'
book_name = 'kjv'
# Set this to {religion_name}_{book_name}
id_flag = religion_name + '_' + book_name

verses = split_by_verse(file_path)
chapters = split_by_chapter(file_path)
chunks = split_by_chunk(file_path, 6, 2)

# Write to temp csv files
verses_csv_path = os.path.join(output_directory, f'{id_flag}_verses.csv')
chapters_csv_path = os.path.join(output_directory, f'{id_flag}_chapters.csv')
chunks_csv_path = os.path.join(output_directory, f'{id_flag}_chunks.csv')

with open(verses_csv_path, 'w') as f:
    f.write('Book,Chapter,Verse,Text\n')
    for verse in verses:
        f.write(','.join(verse) + '\n')

with open(chapters_csv_path, 'w') as f:
    f.write('Book,Chapter,Text\n')
    for chapter in chapters:
        f.write(','.join(chapter) + '\n')

with open(chunks_csv_path, 'w') as f:
    f.write('Book,Chapter,VerseRange,Text\n')
    for chunk in chunks:
        f.write(','.join(chunk) + '\n')
