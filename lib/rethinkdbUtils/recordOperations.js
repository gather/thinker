const compareValues = require('../compareValues')
const colors = require('colors')

async function replaceRecord ({ props, table, sid, tid, sr, tr }) {
  const record = await sr.db(props.sourceDB).table(table).get(sid).run({ timeFormat: 'raw' })
  await tr.db(props.targetDB).table(table).get(tid).replace(record).run()
  return true
}

async function insertRecord ({ props, table, sid, sr, tr }) {
  const record = await sr.db(props.sourceDB).table(table).get(sid).run({ timeFormat: 'raw' })
  await tr.db(props.targetDB).table(table).insert(record).run()
  return true
}

async function deleteRecord ({ props, table, tid, tr }) {
  return tr.db(props.targetDB).table(table).get(tid).delete().run()
}

/**
 * @typedef RecordOperationsEnum
 * @type {Object}
 * @property {string} replace
 * @property {string} insert
 * @property {string} noop
 * @property {string} delete
 * @property {string} incompatibleIds
 */

/**
 * Compares 1 - 1 record from Databases and performs CRUD accordingly
 * @param {*} opts Options to perform operations
 * @param {RethinkDbHashObject} sourceRecord Record from Source Database
 * @param {RethinkDbHashObject} targetRecord Record from Target Database
 * @returns {RecordOperationsEnum}
 */
module.exports.handleRecord = async function (opts, sourceRecord, targetRecord) {
  const { props, sr, tr, table, logger } = opts
  const RecordOperationsEnum = Object.freeze({
    replace: 'replace',
    insert: 'insert',
    noop: 'noop',
    delete: 'delete',
    incompatibleIds: 'incompatibleIds'
  })
  const sid = sourceRecord === undefined ? undefined : Object.keys(sourceRecord)[0]
  const tid = targetRecord === undefined ? undefined : Object.keys(targetRecord)[0]
  const shash = sourceRecord === undefined ? undefined : Object.values(sourceRecord)[0]
  const thash = targetRecord === undefined ? undefined : Object.values(targetRecord)[0]
  const cmp = compareValues(sid, tid)
  try {
    if (cmp === 0) { // si.id === ti.id  ->  check hashes
      if (shash !== thash) {
        // --> refresh ti 050oqrro -> 050p6c74
        await replaceRecord({ props, table, sid, tid, sr, tr })
        return RecordOperationsEnum.replace
      }
      return RecordOperationsEnum.noop
    } else if (cmp < 0) { // si.id < ti.id  ->  insert si
      await insertRecord({ props, table, sid, sr, tr })
      return RecordOperationsEnum.insert
    } else if (cmp > 0) { // si.id > ti.id  ->  delete ti
      await deleteRecord({ props, table, tid, tr })
      return RecordOperationsEnum.delete
    } else {
      // ids can't be compared, did you change the UUID algorithm?
      logger.error(colors.red(`ERROR! Cannot sync, encountered uncomparable PKs`))
      return RecordOperationsEnum.incompatibleIds
    }
  } catch (e) {
    console.log(1)
    throw (e)
  }
}
