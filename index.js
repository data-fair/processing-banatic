// main execution method
exports.run = async ({ pluginConfig, processingConfig, processingId, dir, axios, log, patchConfig }) => {
  const fs = require('fs-extra')
  const path = require('path')
  const util = require('util')
  const FormData = require('form-data')
  const { download, clearFiles } = require('./lib/fetch-data')
  const { process } = require('./lib/process')
  const datasetSchema = require('./resources/schema.json')

  await clearFiles(dir, log)
  await download(dir, axios, log)
  await process(dir, log)

  const formData = new FormData()
  // formData.append('id', processingConfig.dataset.id)
  formData.append('title', processingConfig.dataset.title)
  formData.append('schema', JSON.stringify(datasetSchema))
  formData.append('extras', JSON.stringify({ processingId }))
  formData.append('file', fs.createReadStream(path.join(dir, 'banatic.csv')), { filename: 'banatic.csv' })
  formData.getLength = util.promisify(formData.getLength)

  try {
    const dataset = (await axios({
      method: 'post',
      url: 'api/v1/datasets/' + processingConfig.dataset.id,
      data: formData,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      headers: { ...formData.getHeaders(), 'content-length': await formData.getLength() }
    })).data
    await log.info(`jeu de donnée ${processingConfig.datasetMode === 'update' ? 'mis à jour' : 'créé'}, id="${dataset.id}", title="${dataset.title}"`)
    if (processingConfig.datasetMode === 'create') {
      await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
    }
  } catch (err) {
    console.log(JSON.stringify(err, null, 2))
  }
}
