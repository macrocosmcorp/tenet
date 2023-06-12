const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/confuscianism/CLEAN_analects_confuscious.txt')
const directory = path.resolve('./religious-texts/confuscianism')
const fileString = fs.readFileSync(filepath, 'utf-8')

const isNumeric = (str) => /^\d+$/.test(str);


function parseParagraphs() {
  const lines = fileString.split('\n')
  
  const csv = []
  let currParagraphArr = []
  let currParagrahNum = 0
  let partNum = 0
  let sectionNum = 0
  let lastLineWasEmpty = false
  for(let line of lines) {
    if(/Part\s\d+/.test(line.trim())) {
      partNum++
      if(`Part ${partNum}` !== line.trim()) throw new Error()
      continue
    }
    if(/SECTION\s\d+/.test(line.trim())) {
      sectionNum++
      if(`SECTION ${sectionNum}` !== line.trim()) throw new Error()
      continue
    }
    if(line.trim() == '') {
      if(lastLineWasEmpty == true) continue
      lastLineWasEmpty = true

      csv.push([sectionNum, partNum, currParagrahNum, currParagraphArr.join(' ').replace(/\n/g, ' ')])
      currParagraphArr = []
      currParagrahNum++
      continue
    }
    lastLineWasEmpty = false

    currParagraphArr.push(line)
  }
    
  const ws = fs.createWriteStream(`${directory}/analects_verses.csv`);
  const csvStream = fastcsv.format({ headers: ['Section', 'Part', 'Paragraph', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    if (!row[3]) continue
    csvStream.write(row)
  }
  csvStream.end();

}
function parseParagraphChunks() {
  const lines = fileString.split('\n')
  const NUM_PARAGRAPHS = 5
  const csv = []
  let currParagraphChunkArr = []
  let currParagraphArr = []
  let currParagrahNum = 0
  let partNum = 0
  let sectionNum = 0
  let lastLineWasEmpty = false
  for(let line of lines) {
    if(/Part\s\d+/.test(line.trim())) {
      partNum++
      if(`Part ${partNum}` !== line.trim()) throw new Error()
      continue
    }
    if(/SECTION\s\d+/.test(line.trim())) {
      sectionNum++
      if(`SECTION ${sectionNum}` !== line.trim()) throw new Error()
      continue
    }
    if(line.trim() == '') {
      if(lastLineWasEmpty == true) continue
      lastLineWasEmpty = true
      if(!currParagraphArr.length) continue
      currParagraphChunkArr.push(currParagraphArr.join('\n'))
      currParagraphArr = []
      currParagrahNum++

      if(currParagraphChunkArr.length < NUM_PARAGRAPHS) continue
      // if(partNum<2)console.log(currParagraphChunkArr)

      csv.push([sectionNum, partNum, `${currParagrahNum-NUM_PARAGRAPHS+1}-${currParagrahNum}`, currParagraphChunkArr.join(' ').replace(/\n/g, ' ')])
      currParagraphChunkArr.shift()
      

      continue
    }
    lastLineWasEmpty = false

    currParagraphArr.push(line)
  }
    
  const ws = fs.createWriteStream(`${directory}/analects_chunks.csv`);
  const csvStream = fastcsv.format({ headers: ['Section', 'Part', 'ParagraphRange', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    if (!row[3]) continue
    csvStream.write(row)
  }
  csvStream.end();

}


parseParagraphs()
parseParagraphChunks()