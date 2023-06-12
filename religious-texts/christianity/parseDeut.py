import os

import pandas as pd


# Ask GPT-4 on how to write these, no reason to do by hand. Just make sure that it's right!! If this is messed up it'll screw up embedding process.
def split_by_verse(filename):
    with open(filename, 'r') as f:
        content = f.readlines()

    current_book = ""

    cached_verse = ""
    cached_verse_num = ""
    cached_chapter_num = ""

    verses = []
    for line in content:
        if line.strip():
            if line[0].isnumeric():
                if cached_verse != "":
                    verses.append(
                        [current_book, cached_chapter_num, cached_verse_num, cached_verse])

                # Split on the first space
                chapter_verse_num, verse = line.split(' ', 1)

                chapter_num, verse_num = chapter_verse_num.split(':')

                # Remove the trailing newline
                verse = verse.strip()

                cached_verse = verse
                cached_verse_num = verse_num
                cached_chapter_num = chapter_num
            elif line.startswith('<TITLE>'):
                if cached_verse != "":
                    verses.append(
                        [current_book, cached_chapter_num, cached_verse_num, cached_verse])

                book = line.split(' ', 1)[1].strip()

                print(book)

                current_book = book
            else:
                cached_verse += ' ' + line.strip()
    if cached_verse != "":
        verses.append(
            [current_book, cached_chapter_num, cached_verse_num, cached_verse])

    return verses


def split_by_chapter(verses):
    chapters = []
    chapter = []
    current_book = ""
    current_chapter = ""
    for verse in verses:
        book, chapter_num, verse_num, text = verse

        if current_book != book:
            current_book = book
            current_chapter = chapter_num
            chapter = []

        if current_chapter != chapter_num:
            current_chapter = chapter_num
            chapters.append([book, chapter_num, ' '.join(chapter)])
            chapter = []

        chapter.append(text)

    chapters.append([book, chapter_num, ' '.join(chapter)])

    return chapters


def split_by_chunk(verses, chunk_size, overlap_size, align_to_book=True, align_to_chapter=True):

    # Book -> Chapter -> Verses
    book_chapter_dict_nested = {}
    book_key = ""
    chapter_key = ""
    for verse in verses:
        book, chapter, verse_num, text = verse

        if book_key != book:
            book_key = book
            book_chapter_dict_nested[book_key] = {}
            chapter_key = ""

        if chapter_key != chapter:
            chapter_key = chapter
            book_chapter_dict_nested[book_key][chapter_key] = []

        book_chapter_dict_nested[book_key][chapter_key].append(
            [book, chapter, verse_num, text])

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
    script_directory, 'CLEAN_deut.txt')

# Set the output directory to the script directory
output_directory = script_directory

religion_name = 'christianity'
book_name = 'deut'
# Set this to {religion_name}_{book_name}
id_flag = religion_name + '_' + book_name

verses = split_by_verse(file_path)
chapters = split_by_chapter(verses)
chunks = split_by_chunk(verses, 6, 2)

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
