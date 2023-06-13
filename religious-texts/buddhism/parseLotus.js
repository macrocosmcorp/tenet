const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/buddhism/CLEAN_Lotus_Sutra.txt')
const directory = path.resolve('./religious-texts/buddhism')
const fileString = fs.readFileSync(filepath, 'utf-8')

const isNumeric = (str) => /^\d+$/.test(str);

function parseVerses() {
  const lines = fileString.split('\n').filter(line => Boolean(line.trim())) // iterate over lines that have content

  const csv = []
  let currVerseArr = []
  let currVerseNum = null
  let currChapter = null
  let versePageNum = 3
  let currPageNum = 3
  for (let line of lines) {
    const isChapter = line.trim().includes('Chapter')
    const isPageNum = (line.trim().length <= 3) && isNumeric(line.trim())
    const isVerseNum = (line.trim().length <= 3) && !isNumeric(line.trim())

    if (isVerseNum && currVerseArr.length) {
      

      if(currVerseNum) csv.push([currChapter, versePageNum, currVerseNum, currVerseArr.join(' ')]) // if statemnt just filters out the introduction
      currVerseArr = []
    }

    if (isVerseNum) {
      currVerseNum = line.trim()
      versePageNum = currPageNum
      continue
    }
    if (isChapter) {
      currChapter = line.trim().split('Chapter ')[1]
      continue
    }
    if (isPageNum) {
      currPageNum = Number(line.trim()) + 1
      continue
    }

    currVerseArr.push(line)
  }

  const ws = fs.createWriteStream(`${directory}/lotus_verses.csv`);
  const csvStream = fastcsv.format({ headers: ['Chapter', 'PageStart', 'Verse', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for (let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}

function parsePages() {
  const lines = fileString.split('\n').filter(line => Boolean(line.trim())) // 

  const csv = []
  let currPageArr = []
  let currVerseNum = null
  let currChapter = null
  let currPageNum = 3
  for (let line of lines) {
    const isChapter = line.trim().includes('Chapter')
    const isPageNum = (line.trim().length <= 3) && isNumeric(line.trim())
    const isVerseNum = (line.trim().length <= 3) && !isNumeric(line.trim())
    const isNotContent = (isChapter || isPageNum || isVerseNum)

    if (isNotContent) {
      if (isVerseNum) {
        currVerseNum = line.trim()
      }
      if (isChapter) {
        currChapter = line.trim().split('Chapter ')[1]
      }
      if (isPageNum) { // all this fancy logic is to remove incomplete sentences @the beginning and end of pages.
        let c = currPageArr.join(' ').split('.')
        const firstChar = c[0].trim()[0] // firs letter of first sentence

        if (firstChar.toUpperCase() !== firstChar) c.shift() // if first sentence isn't uppercase (new sentence), shift that shit out.
        if (c[c.length - 1] !== '') {
          c.pop() //if last sentence doesn't end in a period, pop that shit.
          c.push('')
        }
        console.log(c[c.length - 1] !== '')
        console.log(c.join('.'))
        csv.push([currChapter, currPageNum, c.join('.')])
        currPageArr = []
        currPageNum = Number(line.trim()) + 1
      }
      continue
    } //else

    currPageArr.push(line)
  }

  const ws = fs.createWriteStream(`${directory}/lotus_pages.csv`);
  const csvStream = fastcsv.format({ headers: ['Chapter', 'Page', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for (let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}

parseVerses()
parsePages()