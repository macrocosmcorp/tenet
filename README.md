# project-tenet

### Splitting / Chunking

_Splitting / Chunking is dividing the text into embeddable chunks. If the content has important content in single verses (like the Bible) they should be divided per line. If the text is also/just divided into chapters of a reasonable size, we should generate chunks of chapters as a whole. If content is spread across multiple lines, with most documents, they should be divided into a reasonable chunk (4 paragraphs) each overlapping by some amount (1 paragraph overlap) so that every line is covered and not cutoff. This is an art not a science so don't overthink it_
_Everything should be converted into csv files with relevant data. It must have the chapter, verse, book, etc. metadata encoded in each row._
Some of these chunking algorithms don't have the right format: they should accept to variables: a chunk_size and a chunk_overlap parameter.

# Status

- Taoism

  - Tao Te Ching
  - Zhuangzi
  - Daozang

- Judaism

  - Nevi'im
  - Ketuvim

- Buddhism

  - Triptaka
  - Mahayana Sutras

- Hinduism

  - The Vedas
  - Bhagavad Gita (done)

- Islam

  - Quran (done)
  - Hadith
  - Tafsir

- Christianity

  - King James Bible (done)
  - Apocrypha
  - Deuterocanonical books
  - The Book of Mormon (done)

- Confucianism

  - Five Classics
    - Classic of Poetry
    - Book of Rites
    - Book of Documents
    - Book of Changes
    - Spring and Autumn Annals
  - Four Books
    - Great Learning
    - Doctrine of the Mean
    - Analects (done)
    - Mencius

- Bahai Faith

  - Kitáb-i-Aqdas
  - Kitáb-i-Íqán
  - Some Answered Questions
  - The Dawn-Breakers.

- Jainism

  - Agamas

- Modern Philosophy
  - Coming Soon
