const fs = require('fs')
const path = require('path')
const fastcsv = require('fast-csv')


const directory = path.resolve('./religious-texts/hinduism/upanishads')

const Aitareya_Aranyaka_Upanishad_filepath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Aitareya-Aranyaka_Upanishad.txt')
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




const BrihadaranyakaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Brihadaranyaka-Upanishad.txt')
const BrihadaranyakaUpanishad = fs.readFileSync(BrihadaranyakaUpanishadFilePath, 'utf-8')

function parseBUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Khanda|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'Section', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


function parseBUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Khanda|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter || newSection != currSection) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'Section', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}



parseBUVerses(BrihadaranyakaUpanishad, 'Brihadaranyaka-Upanishad')
parseBUVerseChunks(BrihadaranyakaUpanishad, 'Brihadaranyaka-Upanishad')




const KathaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Katha-Upanishad.txt')
const KathaUpanishad = fs.readFileSync(KathaUpanishadFilePath, 'utf-8')



function parseKathaUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Valli|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'Section/Valli', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseKathaUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya|Section|Valli|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter || newSection != currSection) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'Section', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


parseKathaUVerses(KathaUpanishad, 'Katha-Upanishad')
parseKathaUVerseChunks(KathaUpanishad, 'Katha-Upanishad')





const KaushitakiBrahmanaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Kaushitaki-Brahmana-Upanishad.txt')
const KaushitakiBrahmanaUpanishad = fs.readFileSync(KaushitakiBrahmanaUpanishadFilePath, 'utf-8')


function parseKBUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya).*$/ 

    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Adhyaya', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseKBUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya).*$/ 
    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Adhyaya', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}




parseKBUVerses(KaushitakiBrahmanaUpanishad, 'Kaushitaki-Brahmana-Upanishad')
parseKBUVerseChunks(KaushitakiBrahmanaUpanishad, 'Kaushitaki-Brahmana-Upanishad')



/// KU


const KhandogyaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Khandogya-Upanishad.txt')
const KhandogyaUpanishad = fs.readFileSync(KhandogyaUpanishadFilePath, 'utf-8')



function parseKUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Prapathaka|Section|Khanda|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Prapathaka', 'Section/Khanda', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseKUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Prapathaka|Section|Khanda|Brahmana|Aranyaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter || newSection != currSection) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Prapathaka', 'Section/Khanda', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


parseKUVerses(KhandogyaUpanishad, 'Khandogya-Upanishad')
parseKUVerseChunks(KhandogyaUpanishad, 'Khandogya-Upanishad')

// Maitrayana-Brahmana


const MaitrayanaBrahmanaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Maitrayana-Brahmana-Upanishad.txt')
const MaitrayanaBrahmanaUpanishad = fs.readFileSync(MaitrayanaBrahmanaUpanishadFilePath, 'utf-8')



function parseMBUSections(text, fileName) {
    const ignoreLinesRegex = /^.*(Prapathaka).*$/;
    const sectionRegex = /^Section \d+/;
    const newChapterRegex = /~ Chapter \d+ ~/
    const lines = text.split('\n').filter(line => !ignoreLinesRegex.test(line));
    const csv = [];
    let sectionArr = [];
    let currChapter = null;
    let currSection = null;

    for(let line of lines) {
        line = line.trim();
        const isSectionStart = sectionRegex.test(line);
        const isChapterStart = newChapterRegex.test(line)

        if(isSectionStart || isChapterStart) {
            sectionArr.length && csv.push([currChapter, currSection, sectionArr.join(' ')]);
            sectionArr = [];
        }
        if(isChapterStart) {
            const newChapter = line.match(/\d+/)[0];
            currChapter = newChapter;
            continue;
        }
        if(isSectionStart) {
            const newSection = line.match(/\d+/)[0];
            currSection = newSection;
            continue;
        }
        // otherwise (mid section)
        sectionArr.push(line);
    }

    // Push the last section into the csv
    sectionArr.length && csv.push([currChapter, currSection, sectionArr.join(' ')]);

    const ws = fs.createWriteStream(`${directory}/${fileName}_sections.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'Section', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


function parseMBUSectionChunks(text, fileName, numSectionsPerChunk = 3, numSectionsOverlap = 2) {
    const ignoreLinesRegex = /^.*(Prapathaka).*$/;
    const sectionRegex = /^Section \d+/;
    const newChapterRegex = /~ Chapter \d+ ~/
    const lines = text.split('\n').filter(line => !ignoreLinesRegex.test(line));
    const csv = [];
    let sectionBuffer = [];
    let currChapter = null;
    let currSection = null;

    const flushSectionsToCSV = () => {
        if (sectionBuffer.length >= numSectionsPerChunk) {
            let firstSectionInChunk = sectionBuffer[0][0];
            let lastSectionInChunk = sectionBuffer[sectionBuffer.length - 1][0];
            let textInChunk = sectionBuffer.map(s => s[1]).join(' ');
            csv.push([currChapter, `${firstSectionInChunk}-${lastSectionInChunk}`, textInChunk]);
            sectionBuffer = sectionBuffer.slice((numSectionsPerChunk-numSectionsOverlap));
        }
    };

    for(let line of lines) {
        line = line.trim();
        const isSectionStart = sectionRegex.test(line);
        const isChapterStart = newChapterRegex.test(line);

        if(isSectionStart || isChapterStart) {
            flushSectionsToCSV();
        }

        if(isChapterStart) {
            const newChapter = line.match(/\d+/)[0];
            currChapter = newChapter;
            sectionBuffer = []; // Reset at chapter start to avoid overlap
            continue;
        }

        if(isSectionStart) {
            const newSection = line.match(/\d+/)[0];
            currSection = newSection;
            sectionBuffer.push([currSection, '']); // Reset text for the new section
            continue;
        }

        // otherwise (mid section)
        sectionBuffer.length && (sectionBuffer[sectionBuffer.length - 1][1] += ' ' + line);
    }

    // Push the last section chunk into the csv
    flushSectionsToCSV();

    const ws = fs.createWriteStream(`${directory}/${fileName}_section_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter', 'SectionRange', 'Text'] });

    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));

    for(let row of csv) {
      csvStream.write(row);
    }
    csvStream.end();
}



parseMBUSections(MaitrayanaBrahmanaUpanishad, 'Maitrayana-Brahmana-Upanishad')
parseMBUSectionChunks(MaitrayanaBrahmanaUpanishad, 'Maitrayana-Brahmana-Upanishad')


/// MU


const MundakaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Mundaka-Upanishad.txt')
const MundakaUpanishad = fs.readFileSync(MundakaUpanishadFilePath, 'utf-8')



function parseMUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Mundaka|Section|Khanda).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Mundaka', 'Section/Khanda', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseMUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Mundaka|Section|Khanda).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter || newSection != currSection) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Mundaka', 'Section/Khanda', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


parseMUVerses(MundakaUpanishad, 'Mundaka-Upanishad')
parseMUVerseChunks(MundakaUpanishad, 'Mundaka-Upanishad')


// Prasna



const PrasnaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Prasna-Upanishad.txt')
const PrasnaUpanishad = fs.readFileSync(PrasnaUpanishadFilePath, 'utf-8')


function parsePUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Question).*$/ 

    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Question', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parsePUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Question).*$/ 
    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Question', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}




parsePUVerses(PrasnaUpanishad, 'Prasna-Upanishad')
parsePUVerseChunks(PrasnaUpanishad, 'Prasna-Upanishad')

// SU
const SvetasvataraUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Svetasvatara-Upanishad.txt')
const SvetasvataraUpanishad = fs.readFileSync(SvetasvataraUpanishadFilePath, 'utf-8')


function parseSUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya).*$/ 

    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Adhyaya', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseSUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Adhyaya).*$/ 
    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Adhyaya', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}




parseSUVerses(SvetasvataraUpanishad, 'Svetasvatara-Upanishad')
parseSUVerseChunks(SvetasvataraUpanishad, 'Svetasvatara-Upanishad')

// CLEAN_Upanishads:Taittiriyaka-Upanishad


const TaittiriyakaUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Taittiriyaka-Upanishad.txt')
const TaittiriyakaUpanishad = fs.readFileSync(TaittiriyakaUpanishadFilePath, 'utf-8')



function parseTaittiriyakaUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Chapter|Valli|Section|Anuvaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currSection, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currSection = newSection
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Valli', 'Section/Anuvaka', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseTaittiriyakaUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Chapter|Valli|Section|Anuvaka).*$/ 
    const verseRegex = /\d+-\d+:\d+.\s/
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currSection = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newSection, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter || newSection != currSection) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currSection = newSection
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currSection, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Chapter/Valli', 'Section/Anuvaka', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}


parseTaittiriyakaUVerses(TaittiriyakaUpanishad, 'Taittiriyaka-Upanishad')
parseTaittiriyakaUVerseChunks(TaittiriyakaUpanishad, 'Taittiriyaka-Upanishad')




// Pr



const TalavakaraUpanishadFilePath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads:Talavakara_Upanishad_or_Kena-upanishad.txt')
const TalavakaraUpanishad = fs.readFileSync(TalavakaraUpanishadFilePath, 'utf-8')


function parseTalavakaraUVerses(upanishadFullText, upanishadName) {
    const ignoreLinesRegex = /^.*(Section|Khanda).*$/ 

    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            verseArr.length && csv.push([currChapter, currVerseNum, verseArr.join(' ')])
            verseArr = [line.split(verseRegex)[1]]
            const [newChapter, newVerse] = line.match(/\d+/g)
            currChapter = newChapter
            currVerseNum = newVerse
            continue
        }
        // otherwise (mid verse)
        verseArr.push(line)
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verses.csv`);
    const csvStream = fastcsv.format({ headers: ['Section/Khanda', 'Verse', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}

function parseTalavakaraUVerseChunks(upanishadFullText, upanishadName, numVersesPerChunk = 3, numVersesOverlap = 2) {
    const ignoreLinesRegex = /^.*(Section|Khanda).*$/ 
    const verseRegex = /\d+:\d+\./
    const lines = upanishadFullText.split('\n').filter(line => !ignoreLinesRegex.test(line))
    const csv = []
    let verseArr = []
    let currChapter = null
    let currVerseNum = null
    let currVerseStart = null
    let currVerseEnd = null
    for(let line of lines) {
        line = line.trim()
        const isVerse = verseRegex.test(line)
        if(isVerse) {
            const [newChapter, newVerse] = line.match(/\d+/g)
            if(newChapter != currChapter) {
                if(verseArr.length) {
                    currVerseEnd = currVerseNum
                    csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                }
                currChapter = newChapter
                currVerseNum = newVerse
                currVerseStart = currVerseNum
                verseArr = [line.split(verseRegex)[1]]
                continue
            }
            verseArr.push(line.split(verseRegex)[1])
            currVerseNum = newVerse
            if(verseArr.length >= numVersesPerChunk) {
                currVerseEnd = currVerseNum
                csv.push([currChapter, currVerseStart+'-'+currVerseEnd, verseArr.join(' ')])
                verseArr = verseArr.slice(numVersesPerChunk - numVersesOverlap)
                currVerseStart = currVerseNum - verseArr.length + 1
            }
            continue
        }
        verseArr[verseArr.length-1] += ' ' + line
    }
    const ws = fs.createWriteStream(`${directory}/${upanishadName}_verse_chunks.csv`);
    const csvStream = fastcsv.format({ headers: ['Section/Khanda', 'VerseRange', 'Text'] });
  
    csvStream.pipe(ws).on('end', () => console.log('CSV file successfully written'));
  
    for(let row of csv) {
      csvStream.write(row)
    }
    csvStream.end();
}




parseTalavakaraUVerses(TalavakaraUpanishad, 'Talavakara-Upanishad')
parseTalavakaraUVerseChunks(TalavakaraUpanishad, 'Talavakara-Upanishad')