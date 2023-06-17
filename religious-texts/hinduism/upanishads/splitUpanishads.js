const fs = require('fs')
const path = require('path')


const filepath = path.resolve('./religious-texts/hinduism/upanishads/CLEAN_Upanishads.txt')
const directory = path.resolve('./religious-texts/hinduism/upanishads')
const fileString = fs.readFileSync(filepath, 'utf-8')
const upanishads = fileString.split('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
console.log(upanishads[2])
for(let upanishad of upanishads) {
    upanishad = upanishad.trim()


    const upanishadName = upanishad.split('\n')[0].replace(/\s/g, '_')
    
    upanishad = upanishad.split('\n').slice(1).join('\n')
    const fileName = `CLEAN_Upanishads:${upanishadName}.txt`
    console.log(upanishadName)
    fs.writeFileSync(`${directory}/${fileName}`, upanishad)
}