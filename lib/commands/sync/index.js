const _ = require('lodash')
const colors = require('colors')
const moment = require('moment')
const PQueue = require('p-queue')

const { syncTable } = require('../../rethinkdbUtils/syncTable')
const { getLoggerObject } = require('../../loggerObject')
const { createMissingTables } = require('../../rethinkdbUtils/createMissingTables')
const { createMissingIndexes } = require('../../rethinkdbUtils/createMissingIndexes')
const { validation } = require('./validation')
const { getProperties, validateProps } = require('./props')
const { inquire } = require('./inquire')
const { getConnection } = require('./getConnection')

function getTablesToSync ({ props, sourceTableList }) {
  let tablesToSyncArray
  if (props.pickTables) {
    tablesToSyncArray = props.pickTables
  } else if (props.omitTables) {
    tablesToSyncArray = _.difference(sourceTableList, props.omitTables)
  } else {
    tablesToSyncArray = sourceTableList
  }
  return tablesToSyncArray
}

async function getOptions (argv) {
  const props = getProperties(argv)
  if (!validateProps(props)) {
    return false
  }

  const logger = getLoggerObject(isNaN(props.logLevel) ? 0 : props.logLevel)

  // Verify source database
  const sr = getConnection({
    host: props.sourceHost,
    port: props.sourcePort,
    user: props.sourceUser,
    password: props.sourcePassword,
    buffer: props.buffer,
    max: props.max
  })
  const sourceDBList = await sr.dbList().run()

  const sourceTableList = await sr.db(props.sourceDB).tableList().run()
  if (!validation({ props, sourceDBList, logger, sourceTableList })) {
    return false
  }

  if (!props.autoApprove) {
    if (!inquire(logger)) {
      return false
    }
  }

  const tablesManager = getTablesToSync({ props, sourceTableList })
    .reduce((obj, key) => {
      obj[key] = {}
      return obj
    }, {})

  const tr = getConnection({
    host: props.targetHost,
    port: props.targetPort,
    user: props.targetUser,
    password: props.targetPassword,
    buffer: props.buffer,
    max: props.max
  })

  const targetDBList = await tr.dbList().run()
  if (!targetDBList.includes(props.targetDB)) {
    logger.info('Target DB does not exist, creating...')
    await tr.dbCreate(props.targetDB).run()
  }

  return { props, sr, tr, tablesManager, logger }
}

function finalLogMessage (opts) {
  const startTime = moment()
  return () => {
    let msg = `DONE! Completed in ${startTime.fromNow(true)}`
    if (opts.props.pickTables) {
      msg = `${msg} for Tables: ${JSON.stringify(opts.props.pickTables)}`
    }
    if (opts.props.omitTables) {
      msg = `${msg} without Tables: ${JSON.stringify(opts.props.omitTables)}`
    }
    opts.logger.info(colors.green(msg))
  }
}

function cleanConnections (opts) {
  opts.sr.getPoolMaster().drain()
  opts.tr.getPoolMaster().drain()
}

module.exports.main = async function (argv) {
  return new Promise(async (resolve, reject) => {
    try {
      const opts = await getOptions(argv)
      if (!opts) {
        return false
      }
      const message = finalLogMessage(opts)

      await createMissingTables(opts)
      await createMissingIndexes(opts)

      if (!opts.props.indexesOnly) {
        const queue = new PQueue({ concurrency: 1 })
        Object.keys(opts.tablesManager)
          .forEach(table => {
            queue.add(() => syncTable(opts, table))
          })
        queue.onIdle()
          .then(() => {
            message()
            cleanConnections(opts)
            resolve(true)
          })
      }
      return true
    } catch (e) {
      reject(e)
    }
  })
}
