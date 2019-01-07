
async function createTheTable ({ props, sr, tr, table }) {
  const primaryKey = await sr.db(props.sourceDB).table(table).info()('primary_key').run()
  await tr.db(props.targetDB).tableCreate(table, { primaryKey }).run()
  return true
}

module.exports.createMissingTables = async function ({ props, sr, tr, tablesManager, logger }) {
  const targetDBTableList = await tr.db(props.targetDB).tableList().run()
  const createTables = Object.keys(tablesManager)
    .reduce((tablesToCreate, table) => {
      if (!targetDBTableList.includes(table)) {
        logger.info(`Table '${table}' does not exist on target, creating...`)
        tablesToCreate.push(createTheTable({ props, sr, tr, table }))
      }
      return tablesToCreate
    }, [])
  await Promise.all(createTables)
  return true
}
