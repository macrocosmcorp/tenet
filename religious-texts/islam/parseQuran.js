const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/islam/CLEAN_quran.txt')
const directory = path.resolve('./religious-texts/islam')
const fileString = fs.readFileSync(filepath, 'utf-8')


function parseVerses() {
  const lines = fileString.split('\n')
  const csv = []
  let currSura = null
  let currSuraName = null
  let currSection = 'Section 1.'
  let currPage = '[p. 15]'
  let currParagraph = null
  let paragraphArr = []
  let lookingForSuraName = false
  for(let line of lines) {
    line = line.trim()
    const isSura = /Sura\s[XIVLC]+\./.test(line)
    const isSection = /SECTION \d+/.test(line)
    const isPage = /\[p\. \d+\]/.test(line)
    const isParagraph = /\d+\.\s/.test(line)
    if(line == '') continue
    const isNotNormalLine = (isSura || isSection || isPage || isParagraph)
    if(isNotNormalLine && paragraphArr.length) {
      csv.push([currSura, currSection, currParagraph, paragraphArr.join(' ')])
      paragraphArr = []
    }

    if(lookingForSuraName) {
      // console.log('lokingforsuraname. line:', line)
      if(line !== '') {
        currSuraName = line
        lookingForSuraName = false
      } 
      continue
    }

    if(isSura) {
      currSura = line
      currSuraName = null
      currSection = 'Section 1.'
      lookingForSuraName = true
      continue
    }
    if(isSection) {
      currSection = line
      continue
    }
    if(isPage) {
      if(currPage !== line.match(/\[p\. \d+\]/)[0]) {
        throw new Error() }// want to catch if we hit [p. 15] and we haven't been on [p. 15]
      currPage = `[p. ${Number(line.match(/\d+/g)[line.match(/\d+/g).length-1]) + 1}]` // parse out num and increment it.
      continue
    }

    if(isParagraph) {
      currParagraph = Number(line.match(/\d+/)[0])
      let d = line.split(/\d+\.\s/)
      paragraphArr = [d[d.length-1]]
      continue
    }
     // else if fucking normal line
     paragraphArr.push(line)
  }

  return csv
}

const parseParagraphChunks = () => {
  const WINDOW_LENGTH = 4
  const lines = fileString.split('\n')
  const csv = []
  let currSura = null
  let currSuraName = null
  let currSection = 'Section 1.'
  let currPage = '[p. 15]'
  let paragraphNumArr = []
  let paragraphsArr = []
  let paragraphArr = []
  let lookingForSuraName = false
  for(let line of lines) {
    line = line.trim()
    const isSura = /Sura\s[XIVLC]+\./.test(line)
    const isSection = /SECTION \d+/.test(line)
    const isPage = /\[p\. \d+\]/.test(line)
    const isParagraph = /\d+\.\s/.test(line)
    if(line == '') continue
    const isNotNormalLine = (isSura || isSection || isParagraph)
    if(isNotNormalLine && paragraphArr.length ) {
      paragraphsArr.push(paragraphArr.join(' '))
      paragraphArr = []
      if(paragraphsArr.length === WINDOW_LENGTH) {
        if(paragraphsArr.length !== paragraphNumArr.length) {
          console.log(line)
          console.log(paragraphsArr)
          console.log(paragraphNumArr)
          throw new Error()}
        csv.push([currSura, currSection, `${paragraphNumArr[0]}-${paragraphNumArr[paragraphNumArr.length-1]}`, paragraphsArr.join(' ')])
        if(isPage || isParagraph) {
          if(paragraphsArr.length >= WINDOW_LENGTH) paragraphsArr.shift()
          if(paragraphNumArr.length >= WINDOW_LENGTH) paragraphNumArr.shift()
        } 
      }
    }
    if(isSura || isSection) {
      paragraphsArr = []
      paragraphArr = []
      paragraphNumArr = []
    }
    

    if(lookingForSuraName) {
      // console.log('lokingforsuraname. line:', line)
      if(line !== '') {
        currSuraName = line
        lookingForSuraName = false
      } 
      continue
    } else if(isSura) {
      currSura = line
      currSuraName = null
      currSection = 'SECTION 1. (implied)'
      lookingForSuraName = true
      continue
    } else if(isSection) {
      currSection = line
      continue
    } else if(isPage) {
      if(currPage !== line.match(/\[p\. \d+\]/)[0]) {
        throw new Error() }// want to catch if we hit [p. 15] and we haven't been on [p. 15]
      currPage = `[p. ${Number(line.match(/\d+/g)[line.match(/\d+/g).length-1]) + 1}]` // parse out num and increment it.
      continue
    } else if(isParagraph) {
      paragraphNumArr.push(Number(line.match(/\d+/)[0]))
      let d = line.split(/\d+\.\s/)
      paragraphArr = [d[d.length-1]]
      continue
    }
     // else if fucking normal line
     paragraphArr.push(line)
  }
  return csv
}

function main() {
  const verses = parseVerses()
  const chunks = parseParagraphChunks()

  let section = {}
  for (let verse of verses) {
    const id = verse[0] + verse[1]
    if (!section[id]) {
      section[id] = {
        'Sura': verse[0],
        'Section': verse[1],
        'Text': ''
      }
    }
    section[id]['Text'] += verse[3] + ' '
  }
  
  let sections = Object.values(section)
  const section_ws = fs.createWriteStream(`${directory}/quran_sections.csv`);
  const section_stream = fastcsv.format({ headers: ['Sura', 'Section', 'Text'] });
  section_stream.pipe(section_ws);
  for (const row of sections) section_stream.write(row);
  section_stream.end();

  const chunk_ws = fs.createWriteStream(`${directory}/quran_chunks.csv`);
  const chunk_stream = fastcsv.format({ headers: ['Sura', 'Section', 'ParagraphRange', 'Text'] });
  chunk_stream.pipe(chunk_ws);
  for (const row of chunks) chunk_stream.write(row);
  chunk_stream.end();

  const verse_ws = fs.createWriteStream(`${directory}/quran_verses.csv`);
  const verse_stream = fastcsv.format({ headers: ['Sura', 'Section', 'Paragraph', 'Text'] });
  verse_stream.pipe(verse_ws);
  for (const row of verses) verse_stream.write(row);
  verse_stream.end();
}

main()