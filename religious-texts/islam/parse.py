import re

import pandas as pd

# Define constants
SURANAME_REGEX = r"Sura\s[XIVLC]+\."
SECTION_REGEX = r"SECTION \d+"
PAGE_REGEX = r"\[p\. \d+\]"
PARAGRAPH_REGEX = r"\d+\.\s"
WINDOW_LENGTH = 3

filepath = './religious-texts/islam/CLEAN_quran.txt'
directory = './religious-texts/islam'

# Read the file
with open(filepath, 'r') as file:
    lines = file.read().split('\n')


def parse_paragraphs():
    # Initialize variables
    curr_sura = None
    curr_sura_name = None
    curr_section = 'Section 1.'
    curr_page = '[p. 15]'
    curr_paragraph = None
    paragraph_arr = []
    csv = []

    looking_for_sura_name = False
    for line in lines:
        line = line.strip()

        is_sura = re.search(SURANAME_REGEX, line) is not None
        is_section = re.search(SECTION_REGEX, line) is not None
        is_page = re.search(PAGE_REGEX, line) is not None
        is_paragraph = re.search(PARAGRAPH_REGEX, line) is not None
        is_not_normal_line = is_sura or is_section or is_page or is_paragraph

        if line == '' or is_not_normal_line and paragraph_arr:
            csv.append([curr_sura, curr_sura_name, curr_section,
                       curr_page, curr_paragraph, '\n'.join(paragraph_arr)])
            paragraph_arr = []

        if looking_for_sura_name:
            if line != '':
                curr_sura_name = line
                looking_for_sura_name = False
            continue

        if is_sura:
            curr_sura = line
            curr_sura_name = None
            curr_section = 'Section 1.'
            looking_for_sura_name = True
            continue

        if is_section:
            curr_section = line
            continue

        if is_page:
            curr_page = "[p. " + \
                str(int(re.search(r'\d+', line).group()) + 1) + "]"

            continue

        if is_paragraph:
            curr_paragraph = int(re.search(r'\d+', line).group())
            paragraph_arr = [line.split(f"{curr_paragraph}. ")[-1]]
            continue

        # else if normal line
        paragraph_arr.append(line)

    # Write to CSV
    df = pd.DataFrame(
        csv, columns=['Sura', 'SuraName', 'Section', 'Page', 'Paragraph', 'Text'])
    df.to_csv(f"{directory}/quran_paragraphs.csv", index=False)


def parse_paragraph_chunks():
    # Initialize variables
    curr_sura = None
    curr_sura_name = None
    curr_section = 'Section 1.'
    curr_page = '[p. 15]'
    paragraph_num_arr = []
    paragraphs_arr = []
    paragraph_arr = []
    csv = []

    looking_for_sura_name = False
    for line in lines:
        line = line.strip()

        is_sura = re.search(SURANAME_REGEX, line) is not None
        is_section = re.search(SECTION_REGEX, line) is not None
        is_page = re.search(PAGE_REGEX, line) is not None
        is_paragraph = re.search(PARAGRAPH_REGEX, line) is not None
        is_not_normal_line = is_sura or is_section or is_page or is_paragraph

        if is_not_normal_line and paragraph_arr:
            paragraphs_arr.append(' '.join(paragraph_arr))
            paragraph_arr = []

            if len(paragraphs_arr) == WINDOW_LENGTH and paragraph_num_arr:
                paragraph_range = f"{paragraph_num_arr[0]}-{paragraph_num_arr[-1]}"
                csv.append([curr_sura, curr_sura_name, curr_section,
                           curr_page, paragraph_range, ' '.join(paragraphs_arr)])
                paragraphs_arr = []
                paragraph_num_arr = []

        if looking_for_sura_name:
            if line != '':
                curr_sura_name = line
                looking_for_sura_name = False
            continue

        if is_sura:
            curr_sura = line
            curr_sura_name = None
            curr_section = 'Section 1.'
            looking_for_sura_name = True
            continue

        if is_section:
            curr_section = line
            continue

        if is_page:
            curr_page = "[p. " + \
                str(int(re.search(r'\d+', line).group()) + 1) + "]"
            continue

        if is_paragraph:
            paragraph_num = int(re.search(r'\d+', line).group())
            paragraph_num_arr.append(paragraph_num)
            paragraph_arr = [line.split(f"{paragraph_num}. ")[-1]]
            continue

        # else if normal line
        paragraph_arr.append(line)

        # Add paragraphs to the CSV if the window length has been reached
        if len(paragraphs_arr) == WINDOW_LENGTH:
            if paragraph_num_arr:
                paragraph_range = f"{paragraph_num_arr[0]}-{paragraph_num_arr[-1]}"
                csv.append([curr_sura, curr_sura_name, curr_section,
                           curr_page, paragraph_range, '\n'.join(paragraphs_arr)])
                paragraphs_arr = []
                paragraph_num_arr = []

    # Add the last paragraphs to the CSV if they have not been added yet
    if paragraphs_arr:
        paragraph_range = f"{paragraph_num_arr[0]}-{paragraph_num_arr[-1]}"
        csv.append([curr_sura, curr_sura_name, curr_section,
                   curr_page, paragraph_range, ' '.join(paragraphs_arr)])

    # Write to CSV
    df = pd.DataFrame(csv, columns=[
                      'Sura', 'SuraName', 'Section', 'EndPage', 'ParagraphRange', 'Text'])
    df.to_csv(f"{directory}/quran_paragraph_chunks.csv", index=False)


parse_paragraphs()
parse_paragraph_chunks()
