const fs = require('fs-extra')
const parse = require('csv-parse/lib/sync')
const polygonClipping = require('polygon-clipping')

const loadCompos = async () => {
  const content = await fs.readFile('composition-communale-syndicats.csv')
  const lines = parse(content)
  const codes = {}
  lines.shift()
  lines.forEach(l => {
    codes[l[0]] = codes[l[0]] || { members: [], direct: false }
    codes[l[0]].members.push(l[2])
    codes[l[0]].direct = codes[l[0]].direct || (l[1] === '1')
  })
  let ok = 0; let ko = 0
  Object.values(codes).forEach(c => {
    if (c.direct)ok++
    else ko++
  })
  console.log(ok, ko, ok + ko)
  return codes
}

const replace = (dict, item) => {
  let replaced = false
  item.members_comm = JSON.parse(JSON.stringify(item.members))
  do {
    replaced = false
    let members = []
    item.members_comm.forEach((m, i) => {
      if (dict[m]) {
        members = members.concat(dict[m].members)
        // console.log(members)
        replaced = true
      } else members.push(m)
    })
    item.members_comm = members
  } while (replaced)
  // console.log()
}

const loadGeometries = async () => {
  console.log('loading geoms')
  const geoms = Object.assign({}, ...require('../2020-commune-medium-normalized.json').features.map(f => ({ [f.properties.INSEE_COM]: f.geometry })))
  console.log('loading siren / citycode file')
  const content = await fs.readFile('communes-banatic-full.csv')
  const lines = parse(content)
  lines.shift()
  const codes = {}
  lines.forEach(l => {
    if (l[2] && l[2].length && geoms[l[2]]) {
      codes[l[0]] = geoms[l[2]]
    }
  })
  require('../2020-epci-medium-normalized.json').features.forEach(f => {
    codes[f.properties.CODE_EPCI] = f.geometry
  })
  return codes
  // console.log(Object.keys(codes).length)
}
process.env.POLYGON_CLIPPING_MAX_QUEUE_SIZE = 10000000
process.env.POLYGON_CLIPPING_MAX_SWEEPLINE_SEGMENTS = 10000000

const extend = async () => {
  const geoms = await loadGeometries()
  const compos = await loadCompos()
  const content = await fs.readFile('data/banatic.csv')
  const lines = parse(content)
  const codes = {}
  lines.shift()
  lines.forEach(l => {
    codes[l[4]] = { members: l[40].split(';'), direct: l[42] === '1' }
  })
  Object.entries(codes).filter(([k, c]) => c.direct).forEach(([k, c], i) => {
    // console.log(k)
    replace(codes, c)
    if (c.direct && compos[k] && compos[k].members.length) {
      c.members_comm = compos[k].members
    }
    console.log(i, c.members_comm.length, c.members_comm.filter(m => geoms[m]).length)
    try {
      geoms[k] = { type: 'MultiPolygon', coordinates: polygonClipping.union(...c.members_comm.filter(m => geoms[m]).map(m => geoms[m].coordinates)) }
    } catch (err) {
      console.log('/!\\ error doing union for', k)
    }
  })
  const features = parse(content, { columns: true })
  let updated
  do {
    console.log('no geom', features.filter(properties => !geoms[properties['N° SIREN']]).length)
    updated = 0
    features.filter(properties => !geoms[properties['N° SIREN']]).forEach(properties => {
      if (!properties.Membres.split(';').find(m => !geoms[m])) {
        try {
          geoms[properties['N° SIREN']] = { type: 'MultiPolygon', coordinates: polygonClipping.union(...properties.Membres.split(';').map(m => geoms[m].coordinates)) }
          updated++
        } catch (err) {
          console.log('/!\\ error doing union for', properties['N° SIREN'])
        }
      }
    })
    console.log('updated', updated)
  } while (updated > 0)
  console.log('no geom', features.filter(properties => !geoms[properties['N° SIREN']]).length)
  updated = 0
  features.filter(properties => !geoms[properties['N° SIREN']]).forEach(properties => {
    const c = codes[properties['N° SIREN']]
    replace(codes, c)
    if (c.members_comm.find(m => geoms[m])) {
      try {
        geoms[properties['N° SIREN']] = { type: 'MultiPolygon', coordinates: polygonClipping.union(...c.members_comm.filter(m => geoms[m]).map(m => geoms[m].coordinates)) }
        updated++
      } catch (err) {
        console.log('/!\\ error doing union for', properties['N° SIREN'])
      }
    }
    delete c.members_comm
  })
  console.log('updated', updated)
  console.log('no geom', features.filter(properties => !geoms[properties['N° SIREN']]).length)

  fs.writeFile('banatic.geojson', JSON.stringify({
    type: 'FeatureCollection',
    features: features.map(properties => {
      properties.Communes_membres = codes[properties['N° SIREN']].members_comm ? codes[properties['N° SIREN']].members_comm.join(';') : undefined
      const ret = { type: 'Feature', properties }
      if (geoms[properties['N° SIREN']]) ret.geometry = geoms[properties['N° SIREN']]
      return ret
    })
  }))
}

extend()
