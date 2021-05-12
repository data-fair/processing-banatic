const pump = require('util').promisify(require('pump'))

const path = require('path')
const fs = require('fs-extra')

const regions = require('../resources/regions.json')

const withStreamableFile = async (filePath, fn) => {
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(filePath + '.tmp')
  await fn(fs.createWriteStream(filePath + '.tmp'))
  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(filePath + '.tmp', 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  // write in tmp file then move it for a safer operation that doesn't create partial files
  await fs.move(filePath + '.tmp', filePath, { overwrite: true })
}

exports.download = async (dir, axios, log) => {
  await fs.ensureDir(dir)
  log.info('téléchargement des fichiers régionaux')
  for (const region of regions) {
    const filePath = path.join(dir, `${region}.csv`)
    if (await fs.pathExists(filePath)) {
      log.info(`le fichier ${filePath} a déjà été téléchargé`)
    } else {
      log.info(`téléchargement du fichier ${filePath}`)
      await withStreamableFile(filePath, async (writeStream) => {
        const url = `https://www.banatic.interieur.gouv.fr/V5/fichiers-en-telechargement/telecharger.php?zone=${region}&date=01/04/2021&format=D`
        const res = await axios({ url, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })
    }
  }
}

exports.clearFiles = async (dir, log) => {
  await log.debug('suppression des anciens fichiers téléchargés')
  await fs.remove(dir)
}
