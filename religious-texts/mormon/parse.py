import csv
import os
import re
from collections import deque

filepath = os.path.join('.', 'religious-texts',
                        'mormon', 'CLEAN_book_of_mormon.txt')
directory = os.path.join('.', 'religious-texts', 'mormon')

with open(filepath, 'r') as f:
    lines = [line.strip() for line in f.readlines()]


def is_new_book(line):
    return re.fullmatch(r"^[A-Z\s]+$", line) is not None


def is_book_chapter_verse(line):
    return re.fullmatch(r"^(\d\s)?[A-Za-z]+\s\d+:\d+$", line) is not None


def parse_verses():
    csv_data = []
    curr_book = curr_chapter = curr_verse = ''
    verse_arr = []
    for line in lines:
        if is_new_book(line):
            continue
        if is_book_chapter_verse(line):
            if verse_arr:
                verse_text = ' '.join(verse_arr).replace('\n', ' ')
                if verse_text:
                    csv_data.append(
                        [curr_book, curr_chapter, curr_verse, verse_text])
                verse_arr = []
            parts = line.split(' ')
            curr_book = ' '.join(parts[:-1])
            curr_chapter, curr_verse = parts[-1].split(':')
        else:
            verse_arr.append(line.lstrip('0123456789 '))
    with open(os.path.join(directory, 'mormon_verses.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Book', 'Chapter', 'Verse', 'Text'])
        writer.writerows(csv_data)


def parse_verse_chunks(chunk_size, overlap_size):
    csv_data = []
    curr_book = curr_chapter = ''
    verse_arr = []
    verse_num_arr = deque(maxlen=chunk_size)
    verses_arr = deque(maxlen=chunk_size)
    for line in lines:
        if is_new_book(line):
            continue
        if is_book_chapter_verse(line):
            if verse_arr:
                verses_arr.append(' '.join(verse_arr).replace('\n', ' '))
                verse_arr = []
            if len(verses_arr) == chunk_size:
                verses_text = ' '.join(list(verses_arr)).replace('\n', ' ')
                csv_data.append(
                    [curr_book, curr_chapter, f'{verse_num_arr[0]}-{verse_num_arr[-1]}', verses_text])
                # Remove 'overlap_size' number of elements from the front
                for _ in range(overlap_size):
                    verse_num_arr.popleft()
                    verses_arr.popleft()
            parts = line.split(' ')
            curr_book = ' '.join(parts[:-1])
            curr_chapter, verse_num = parts[-1].split(':')
            verse_num_arr.append(verse_num)
        else:
            verse_arr.append(line.lstrip('0123456789 '))
    with open(os.path.join(directory, 'mormon_chunks.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Book', 'Chapter', 'VerseRange', 'Text'])
        writer.writerows(csv_data)


def parse_chapters():
    csv_data = []
    curr_book = curr_chapter = ''
    verse_arr = []
    for line in lines:
        if is_new_book(line):
            continue
        if is_book_chapter_verse(line):
            parts = line.split(' ')
            book = ' '.join(parts[:-1])
            chapter, _ = parts[-1].split(':')
            if verse_arr and curr_chapter != chapter:
                verse_text = ' '.join(verse_arr).replace('\n', ' ')
                if verse_text:
                    csv_data.append([curr_book, curr_chapter, verse_text])
                verse_arr = []
            curr_book = book
            curr_chapter = chapter
        else:
            verse_arr.append(line.lstrip('0123456789 '))
    with open(os.path.join(directory, 'mormon_chapters.csv'), 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Book', 'Chapter', 'Text'])
        writer.writerows(csv_data)


parse_verses()
parse_verse_chunks(4, 2)
parse_chapters()
