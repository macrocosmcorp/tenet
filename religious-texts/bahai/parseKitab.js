const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/bahai/CLEAN_kitab_bahaullah.txt')
const directory = path.resolve('./religious-texts/bahai')
const fileString = fs.readFileSync(filepath, 'utf-8')


const paragraphCsv = () => {
  const paragraphs = fileString.split(/\n\d+\s/).map(p => p.trim());

  const ws = fs.createWriteStream(`${directory}/kitab_bahaullah_paragraphs.csv`);
  const csvStream = fastcsv.format({ headers: ['Paragraph', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  paragraphs.forEach((paragraph, index) => {
    if (paragraph) { // to skip empty lines if any
      csvStream.write([index + 1, paragraph.replace(/\n/g, ' ')])
    }
  });

  csvStream.end();
}

const paragraphChunksCsv = () => {
  const paragraphs = fileString.split(/\n\d+\s/)
  // .map(p => p.trim());



  const ws = fs.createWriteStream(`${directory}/kitab_bahaullah_paragraphChunks.csv`);
  const csvStream = fastcsv.format({ headers: ['ParagraphRange', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  //moving window of 3 paragraphs
  const currParagraphs = []

  paragraphs.forEach((paragraph, index) => {
    currParagraphs.push(paragraph)
    if(index < 2) return
    if (paragraph) { // to skip empty lines if any
      csvStream.write([`${index+1-2}-${index+1}`, currParagraphs.join(' ').replace(/\n/g, ' ')]);
      currParagraphs.shift()
    }
  });

  csvStream.end();
}


paragraphCsv()
paragraphChunksCsv()