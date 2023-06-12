const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv');


const filepath = path.resolve('./religious-texts/hinduism/CLEAN_JustBhagavadGita.txt')
const directory = path.resolve('./religious-texts/hinduism')
const fileString = fs.readFileSync(filepath, 'utf-8')
/*
 bhagavad gita has an edgecase where there will be: 
 """
 Chapter 3
 Someone said:
 1:1 Verse here
 """
 as far as I'm concerned, and from what I briefly looked at online, this "Someone said" part is actually a part of the next verse.
 So I am programatically going to go through and actually make it part of the next verse
*/

let whosaid = null
const newFileStringArr = []
for (let line of fileString.split('\n')) {
  if(Boolean(whosaid) && !line.trim()) continue
  if(Boolean(whosaid) && line.trim()) {
    if(!(/\d+:\d+/.test(line))){
      console.log(`${JSON.stringify(line)}`)
       throw new Error()}
    let match = line.match(/\d+:\d+./)[0]
    line = line.replace(match, `${match} ${whosaid}`)
    whosaid = null
    newFileStringArr.push(line)
    continue
  }
  if(/^.*\bsaid:$/.test(line)) {
    whosaid = line
    continue
  } else {
    newFileStringArr.push(line)
    whosaid = null
    continue
  }
}
const newFileString = newFileStringArr.join('\n')

fs.writeFileSync(`${directory}/Processed_JustBhagavadGita.txt`, newFileString)

function parseVerses() {
  const lines = newFileString.split('\n')
  
  const csv = []
  let verseArr = []
  let currChapter = null
  let currVerse = null
  for(let line of lines) {
    const isNewVerse = /\d+:\d+/.test(line.trim())
    const isNewChapter = line.trim().includes('Chapter')
    if(isNewChapter) {
      csv.push([currChapter?.replace('Chapter ', ''), currVerse?.split(':')[1], verseArr.join(' ')])
      currChapter = line.trim().split("~").join('').trim()
      continue
    }
    if(isNewVerse) {
      csv.push([currChapter?.replace('Chapter ', ''), currVerse?.split(':')[1], verseArr.join(' ')])
      currVerse = line.trim().match(/\d+:\d+./)[0].split('.').join('')
      let d = line.trim().split(currVerse) // verse shouldn't include the actual numbers and bs 
      verseArr = [d[d.length-1].split('. ').join('')]
      continue
    }

    verseArr.push(line.trim())
  }
    
  const ws = fs.createWriteStream(`${directory}/bhagavad_gita_verses.csv`);
  const csvStream = fastcsv.format({ headers: ['Chapter', 'Verse', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    if (row[2] === '') continue
    csvStream.write(row)
  }
  csvStream.end();

}
function parseVersesChunks() {
  const lines = newFileString.split('\n')
  const WINDOW_LENGTH = 3
  const csv = []
  let versesArr = []
  let verseArr = []
  let currChapter = null
  let varf = true
  let verseNumArr = [] //better to have a moving window of verse num arr than tracking a start verse and end verse
  for(let line of lines) {
    const isNewVerse = /\d+:\d+/.test(line.trim())
    const isNewChapter = line.trim().includes('Chapter')
    if(isNewChapter) {
      csv.push([currChapter?.replace('Chapter ', ''), `${verseNumArr[0]}-${verseNumArr[verseNumArr.length-1]}`, versesArr.join(' ').replace('\n', ' ')])
      verseNumArr.shift()
      versesArr.shift()
      currChapter = line.trim().split("~").join('').trim()
      continue
    }
    if(isNewVerse) {
      if(verseArr.length) versesArr.push(verseArr.join('\n')) 
      verseArr = [] 
      
      if(versesArr.length === WINDOW_LENGTH && verseNumArr.length) {
        csv.push([currChapter?.replace('Chapter ', ''), `${verseNumArr[0]}-${verseNumArr[verseNumArr.length-1]}`, versesArr.join(' ').replace('\n', ' ')])
        verseNumArr.shift()
        versesArr.shift()
      }

      currVerse = line.trim().match(/\d+:\d+./)[0].split('.').join('')
      let d = line.trim().split(currVerse)
      verseArr.push(d[d.length-1].split('. ').join(''))
      verseNumArr.push(currVerse.split(':')[1])

      continue
    }

    verseArr.push(line.trim())
  }
    
  const ws = fs.createWriteStream(`${directory}/bhagavad_gita_versesChunks.csv`);
  const csvStream = fastcsv.format({ headers: ['Chapter', 'VerseRange', 'Text'] });

  csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

  for(let row of csv) {
    if (!row[2]) continue
    row[2] = row[2].split('\n').join(' ')
    csvStream.write(row)
  }
  csvStream.end();

}

parseVerses()
parseVersesChunks()