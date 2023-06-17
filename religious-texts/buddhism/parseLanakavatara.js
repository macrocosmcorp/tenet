const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/buddhism/CLEAN_Lankavatara_Sutra.txt')
const directory = path.resolve('./religious-texts/buddhism')
const fileString = fs.readFileSync(filepath, 'utf-8')

const isNumeric = (str) => /^\d+$/.test(str);


function parseParagraphs() {
  const lines = fileString.split('\n') // split file up into lines and iterate through it
  
  const csv = []
  let currParagraphArr = [] // keeps track of the current paragraph, line by line, which are joined and pushed to the csv 
  let currParagrahNum = 0
  let currChapter = null
  let lastLineWasEmpty = false // lastLineWasEmpty helps skip over the blank space in between paragraphs.
  for(let line of lines) {
    if(line.trim().includes('Chapter')) {
      currChapter = line.trim().match(/\d+/)[0] // find just the number of the chapter
      continue
    }

    if(line.trim() == '') {
      if(lastLineWasEmpty == true) continue
      lastLineWasEmpty = true

      if(currParagraphArr.length) csv.push([currChapter, currParagrahNum, currParagraphArr.join(' ')])
      currParagraphArr = []
      currParagrahNum++
      continue
    }
    lastLineWasEmpty = false

    currParagraphArr.push(line)
  }
    
  const ws = fs.createWriteStream(`${directory}/lankavatara_paragraphs.csv`);
  const csvStream = fastcsv.format({ headers: ['Chapter', 'Paragraph', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    csvStream.write(row)
  }
  csvStream.end();

}


parseParagraphs()