/* anchor points: 
-BOOK TITLES;
  - example: 
THIRD BOOK OF NEPHI
  - Regex: ^[A-Z\s]+$   <--- mathes line of all caps

-Actual Verses
  -example:
Mormon 9:21
Words here just like this.
And this.
  -Regex: ^(\d\s)?[A-Za-z]+\s\d+:\d+$
*/
const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/mormon/CLEAN_book_of_mormon.txt')
const directory = path.resolve('./religious-texts/mormon')
const fileString = fs.readFileSync(filepath, 'utf-8')

const parseVerses = () => {
  const lines = fileString.split('\n')
  const csv = []
  let currBook = ''
  let currChapter = ''
  let currVerse = ''
  let verseArr = []
  for(let line of lines) {
    line = line.trim()
    const isNewBook = /^[A-Z\s]+$/.test(line)
    const isBookChapterVerse = /^(\d\s)?[A-Za-z]+\s\d+:\d+$/.test(line)
    if(isNewBook) continue // actually I think we can just parse EVERYTHING from the bookchapterverse
    if(isBookChapterVerse) {
      if(verseArr.length) {
        csv.push([currBook, currChapter, currVerse, verseArr.join('\n')])
        verseArr = []
      }
      const book = line.match(/(\d\s)?[A-Za-z]+\s/)[0].trim()
      const chapter = line.match(/(?<=[A-Za-z]+\s)\d+(?=:\d+)/)[0].trim()
      const verse = line.match(/(?<=[A-Za-z]+\s\d+:)\d+/)[0].trim()
      currBook = book
      currChapter = chapter
      currVerse = verse
    } else {
      // clean up Number + space at the beginning of the line if it's there. 
      let lineArr = line.split('')
      while(!isNaN(Number(lineArr[0]))) lineArr.shift()
      if(lineArr[0] == ' ') lineArr.shift()
      line = lineArr.join('')
      verseArr.push(line)
    }
  }

  const ws = fs.createWriteStream(`${directory}/mormon_verses.csv`);
  const csvStream = fastcsv.format({ headers: ['Book', 'Chapter', 'Verse', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}

const parseVerseChunks = () => {
  const lines = fileString.split('\n')
  const WINDOW_LENGTH = 3
  const csv = []
  let currBook = ''
  let currChapter = ''
  let verseNumArr = []
  let verseArr = []
  let versesArr = []
  for(let line of lines) {
    line = line.trim()
    const isNewBook = /^[A-Z\s]+$/.test(line)
    const isBookChapterVerse = /^(\d\s)?[A-Za-z]+\s\d+:\d+$/.test(line)
    if(isNewBook) continue // actually I think we can just parse EVERYTHING from the bookchapterverse
    if(isBookChapterVerse) {
      if(verseArr.length) {
        versesArr.push(verseArr.join('\n'))
        verseArr = []
      }
      if(versesArr.length >= WINDOW_LENGTH) {
        if(verseArr.length > WINDOW_LENGTH) throw new Error()
        if(verseNumArr.length !== WINDOW_LENGTH) {
          console.log(verseNumArr)
          console.log(versesArr)
          throw new Error()}
        if(Number(verseNumArr[0]) < Number(verseNumArr[verseNumArr.length-1])){ // hacky fix for overlapping windows
        csv.push([currBook, currChapter, `${verseNumArr[0]}-${verseNumArr[verseNumArr.length-1]}`, versesArr.join('\n')])
        }
        verseNumArr.shift()
        versesArr.shift()
      }
      const book = line.match(/(\d\s)?[A-Za-z]+\s/)[0].trim()
      const chapter = line.match(/(?<=[A-Za-z]+\s)\d+(?=:\d+)/)[0].trim()
      const verseNum = line.match(/(?<=[A-Za-z]+\s\d+:)\d+/)[0].trim()
      currBook = book
      currChapter = chapter
      verseNumArr.push(verseNum)
    } else {
      if(line) verseArr.push(line)
    }
  }

  const ws = fs.createWriteStream(`${directory}/mormon_verseChunks.csv`);
  const csvStream = fastcsv.format({ headers: ['Book', 'Chapter', 'VerseRang', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}

const parseChapters = () => {
  const lines = fileString.split('\n')
  const csv = []
  let currBook = ''
  let currChapter = ''
  let currVerse = ''
  let verseArr = []
  for(let line of lines) {
    line = line.trim()
    const isNewBook = /^[A-Z\s]+$/.test(line)
    const isBookChapterVerse = /^(\d\s)?[A-Za-z]+\s\d+:\d+$/.test(line)
    if(isNewBook) continue // actually I think we can just parse EVERYTHING from the bookchapterverse
    if(isBookChapterVerse) {
      const book = line.match(/(\d\s)?[A-Za-z]+\s/)[0].trim()
      const chapter = line.match(/(?<=[A-Za-z]+\s)\d+(?=:\d+)/)[0].trim()
      const verse = line.match(/(?<=[A-Za-z]+\s\d+:)\d+/)[0].trim()
      if(verseArr.length && currChapter !== chapter) {
        csv.push([currBook, currChapter, verseArr.join('\n')])
        verseArr = []
      }
      currBook = book
      currChapter = chapter
      currVerse = verse
    } else {
      verseArr.push(line)
    }
  }

  const ws = fs.createWriteStream(`${directory}/mormon_chapters.csv`);
  const csvStream = fastcsv.format({ headers: ['Book', 'Chapter', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}


parseVerses()
parseVerseChunks()
parseChapters()