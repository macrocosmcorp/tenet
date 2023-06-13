const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv')


const Aitareya_Aranyaka_Upanishad_filepath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Aitareya-Aranyaka_Upanishad.txt')
const directory = path.resolve('./religious-texts/hinduism/upanishads')
const Aitareya_Aranyaka_Upanishad = fs.readFileSync(Aitareya_Aranyaka_Upanishad_filepath, 'utf-8')

function parseAAUVerses(upanishadFullText, upanishadName) {
    // ignoring these because every single verse repeats them and I want to grab it from there, but I don't want to delete it from the source code.
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Khanda|Part |Aranyaka).*$/ 
    const verseRegex = /\d+,\s\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currPart = null
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currPart, currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newPart, newChapter, newSection, newVerse] = line.match(/\d+/g)
            currPart = newPart
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Part', 'Chapter/Adhyaya', 'Section/Khanda', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

// props to GPT-4
function parseAAUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    // Regex for lines to ignore
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Khanda|Part |Aranyaka).*$/ 

    // Regex to identify a verse line
    const verseRegex = /\d+,\s\d+-\d+:\d+.\s/

    // Split the full text into lines and remove ignored lines
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))

    // Initialize the array to hold the final csv data
    const csv = []

    // Initialize variables to hold the current verse details
    let verseArr = []
    let currPart = null
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null

    // Loop through all lines
    for(let line of lines) {
        // Remove leading/trailing spaces
        line = line.trim()

        // Check if the line is a verse line
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            // Extract the verse details
            const [newPart, newChapter, newSection, newVerse] = line.match(/\d+/g)

            // Check if new part, chapter or section
            if(newPart != currPart || newChapter != currChapter || newSection != currSection) {
                // If there is an existing verse chunk, add it to the csv data
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currPart, currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }

                // Update the current verse details
                currPart = newPart
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum

                // Start a new verse chunk
                verseArr = [line.split(verseRegex)[1]]
                continue
            }

            // If the line is not a new part, chapter, or section, it is a continuation of the current verse
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse

            // If the verse chunk has reached the desired size, add it to the csv data
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currPart, currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                
                // Move the verse chunk window based on the overlap setting
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }

        // If the line is not a verse line, it is a continuation of the current verse text
        verseArr[verseArr.length-1] += ' ' + line
    }

    // Write the csv data to a file
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`)
    const csvStream = fastcsv.format({ headers: ['Part', 'Chapter/Adhyaya', 'Section/Khanda', 'VerseRange', 'Text'] })

    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'))
  
    for(let row of csv) {
      csvStream.write(row)
    }

    csvStream.end()
}



parseAAUVerses(Aitareya_Aranyaka_Upanishad, 'Aitareya-Aranyaka_Upanishad')
parseAAUVerseChunks(Aitareya_Aranyaka_Upanishad, 'Aitareya-Aranyaka_Upanishad', 3, 2)
