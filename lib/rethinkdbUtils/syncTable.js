const PQueue = require('p-queue')

const { calculateCutoffPointIndex } = require('./calculateCutoffPointIndex')
const { getChunks } = require('./chunks')
const { handleRecord } = require('./recordOperations')

/** { id: hash } we retrieve from RethinkDb Database
 * @typedef {Object.<string, string>} RethinkDbHashObject
 */

function synchronizingMessage ({ props, totalRecordsSource, totalRecordsTarget, table, companyMessage, logger }) {
  logger.info(`Synchronizing ${totalRecordsSource} records in ${table} from DB ${props.sourceDB} with ${totalRecordsTarget} on ${table} from DB ${props.targetDB} ${companyMessage || ''}...      `)
}

async function trackRecordsToProcess ({ props, sr, tr, tablesManager, table, logger }) {
  if (props.companyIds && !tablesManager[table].indexes.includes('Company')) {
    logger.info(`Skipping table ${table} because it has no secondary index Company`)
    return false
  }
  const companyMessage = props.companyIds ? `by Companies: ${props.companyIds}` : ''
  const [ totalRecordsSource, totalRecordsTarget ] = await Promise.all(props.companyIds
    ? [
      sr.db(props.sourceDB).table(table).getAll(...props.companyIds, { index: 'Company' }).count().run(),
      tr.db(props.targetDB).table(table).getAll(...props.companyIds, { index: 'Company' }).count().run()
    ]
    : [
      sr.db(props.sourceDB).table(table).count().run(), tr.db(props.targetDB).table(table).count().run()
    ])
  synchronizingMessage({ props, totalRecordsSource, totalRecordsTarget, table, companyMessage, logger })
  return totalRecordsSource
}

module.exports.syncTable = function (opts, table) {
  return new Promise(async (resolve, reject) => {
    const { props, sr, tr, tablesManager, logger } = opts
    const continueProcessing = await trackRecordsToProcess({ props, sr, tr, tablesManager, table, logger })
    if (!continueProcessing) {
      resolve(false)
    } else {
      try {
        const queue = new PQueue({ concurrency: props.buffer })
        let init = true
        let processChunks = true
        let sourceOffset = 0
        let targetOffset = 0
        while (processChunks) {
          const [ sourceChunk, targetChunk ] = await getChunks({ props, sr, tr, table }, sourceOffset, targetOffset)
          if (sourceChunk.length > 0) {
            const [ extraSourceOffset, extraTargetOffset ] = calculateCutoffPointIndex(sourceChunk, targetChunk, props.chunkSize)
            sourceOffset += extraSourceOffset
            targetOffset += extraTargetOffset
            const elementsToProcess = sourceChunk.slice(0, sourceOffset)
            elementsToProcess.forEach((element, idx) => {
              queue.add(() => handleRecord({ props, sr, tr, table, logger }, element, targetChunk[idx]))
            })
            if (init) {
              queue.onIdle().then(() => {
                resolve(true)
              })
              init = false
            }
          } else {
            processChunks = false
          }
        }
      } catch (e) {
        reject(e)
      }
      await tr.db(props.targetDB).table(table).sync().run()
      return true
    }
  })
}
