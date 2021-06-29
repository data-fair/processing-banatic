const path = require('path')
const fs = require('fs-extra')
const Iconv = require('iconv').Iconv
const iconv = new Iconv('iso-8859-1', 'utf-8')
const parse = require('csv-parse/lib/sync')
const parserOpts = { delimiter: '\t', quote: '' }
const stringify = require('csv-stringify/lib/sync')

const regions = require('../resources/regions.json')

exports.process = async (dir, log) => {
  const writeStream = fs.createWriteStream(path.join(dir, 'banatic.csv'), { flags: 'w' })
  let firstLine = true
  for (const region of regions) {
    const filePath = path.join(dir, `${region}.csv`)
    const content = await fs.readFile(filePath)
    const lines = parse(iconv.convert(content), parserOpts)
    const header = lines.shift()
    header.splice(40, 9)
    const skills = header.slice(40)
    const codes = {}
    lines.forEach(l => {
      const specL = l.splice(40, 9)
      const skillsL = l.slice(40)
      // console.log(skillsL)
      codes[l[4]] = codes[l[4]] || Object.assign({}, ...header.slice(0, 40).map((h, i) => ({ [h]: l[i] })), { Membres: [], Compétences: skills.filter((s, i) => skillsL[i] === '1').join(';') })
      codes[l[4]].Membres.push(specL[1])
      if (codes[l[4]].representation_substitution !== '1') codes[l[4]].representation_substitution = specL[3]
      // if (specL[6] && specL[6].length) codes[l[4]].Adhésions.push(specL[6])
    })
    Object.values(codes).forEach(g => {
      g.Membres = g.Membres.join(';')
      g['Région siège'] = g['Région siège'].split(' - ').shift()
      g['Département siège'] = g['Département siège'].split(' - ').shift()
      g['Commune siège'] = g['Commune siège'].split(' - ').pop()
    })
    if (firstLine) {
      firstLine = false
      writeStream.write(header.slice(0, 40).join(',') + ',Membres,Compétences,representation_substitution\n')
    }
    writeStream.write(stringify(Object.values(codes)))
  }
  writeStream.end()
}
