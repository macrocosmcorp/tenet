# project-tenet

### Cleaning

_Cleaning is just removing everything that shouldn't be embedded. License, random elements, etc. Chapter titles should be kept and page numbers can stay if there are no chapters. Verses should be on a new line to be parsed correctly, if it's all stuck together it won't be easy to parse._

### Splitting / Chunking

_Splitting / Chunking is dividing the text into embeddable chunks. If the content has important content in single verses (like the Bible) they should be divided per line. If the text is also/just divided into chapters of a reasonable size, we should generate chunks of chapters as a whole. If content is spread across multiple lines, with most documents, they should be divided into a reasonable chunk (4 paragraphs) each overlapping by some amount (1 paragraph overlap) so that every line is covered and not cutoff. This is an art not a science so don't overthink it_
_Everything should be converted into csv files with relevant data. It must have the chapter, verse, book, etc. metadata encoded in each row._
Some of these chunking algorithms don't have the right format: they should accept to variables: a chunk_size and a chunk_overlap parameter.

# Status

- Book Of Mormon (done)
- Quran (done)
- King James Bible (done)
