const _ = require('lodash')

async function copyIndex ({ props, sr, tr, logger }, table, targetIndexes, index) {
  if (!targetIndexes.includes(index)) {
    logger.info(`Index '${index}' does not exist on '${table}' table on target, creating...`)
    let indexObj = await sr.db(props.sourceDB).table(table).indexStatus(index).run()
    indexObj = _.first(indexObj)
    await tr.db(props.targetDB).table(table).indexCreate(indexObj.index, indexObj.function, { geo: indexObj.geo, multi: indexObj.multi }).run()
  }
  return true
}

async function copyTableIndexes ({ props, sr, tr, tablesManager, logger }, table) {
  const [sourceIndexes, targetIndexes] = await Promise.all([
    sr.db(props.sourceDB).table(table).indexList().run(),
    tr.db(props.targetDB).table(table).indexList().run()
  ])
  tablesManager[table].indexes = sourceIndexes
  await Promise.all(sourceIndexes.map(index => copyIndex({ props, sr, tr, logger }, table, targetIndexes, index)))
  await tr.db(props.targetDB).table(table).indexWait().run()
  return true
}

module.exports.createMissingIndexes = async function ({ props, sr, tr, tablesManager, logger }) {
  await Promise.all(
    Object.keys(tablesManager).map(table => copyTableIndexes({ props, sr, tr, tablesManager, logger }, table))
  )
  return true
}
