import csv
import re

input_file = 'CLEAN_The-Complete-Hadith.txt'
output_file = 'hadith.csv'

with open(input_file, 'r') as f:
    text = f.read()

# Matches for books, and text blocks
book_match = re.compile(r"BOOK (\d+\. [^\n]+)")
text_block_match = re.compile(r"(Volume \d+, Book \d+, Number \d+:)(.*?)(?=(Volume \d+, Book \d+, Number \d+:)|$)", re.DOTALL)

books = book_match.findall(text)
text_blocks = text_block_match.findall(text)

entries = []
current_book = ''
max_words = 0

for block in text_blocks:
    header, text = block[0], block[1].strip()
    
    volume_number = re.search(r"Volume (\d+)", header).group(1)
    book_number = re.search(r"Book (\d+)", header).group(1)
    # narration_number = re.search(r"Number (\d+)", header).group(1) # make this account for things like 532c
    narration_number = header.split('Number ')[-1].split(':')[0]
        
    while books and int(book_number) >= int(books[0].split('.')[0]):
        current_book = "BOOK " + books.pop(0)
        
    narrator = re.search(r"(Narrated|Narrates|Narrate) (.*?)(:|\n)", text, re.IGNORECASE).group(2).strip()
    narration_text = text.split('\n', 1)[1].strip()
    
    if len(narration_text.split()) > max_words:
        max_words = len(narration_text.split())
    
    entries.append([volume_number, current_book, narration_number, narrator, narration_text.replace('\n', ' ')])

print('Max words:', max_words)
# Max words: 3136
# Can easily embed each single narration with ada-002 :D

# Save to CSV
with open(output_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['Volume', 'Book', 'Number', 'Narrator', 'Text']) # Write header
    writer.writerows(entries)

