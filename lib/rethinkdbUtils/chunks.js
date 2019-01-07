
/** Gets a DB cursor to the items filtered by Secondary Index Company
 * Make sure the first element has the lastId
 * @param {*} connection Your connection object to rethinkdb
 * @param {string} database Database name
 * @param {string} table Table name
 * @param {string[]} companyIds Array of company ids
 * @param {number} chunkSize Top Limit of rows to retrieve
 * @param {number} offset Number of rows to skip, we can't use `orderBy` and `between` in ReQL
 * @param {string} lastId Row Id of the last element of the last chunk processed
 * @returns {Promise<RethinkDbHashObject[]>}
 */
async function getByCompanyChunks (connection, database, table, companyIds, chunkSize, offset, lastId) {
  const chunk = await connection.db(database).table(table)
    .getAll(...companyIds, { index: 'Company' })
    .orderBy('id')
    .skip(offset)
    .map(function (row) {
      return connection.expr([[row('id'), connection.uuid(row.toJSON())]]).coerceTo('OBJECT')
    })
    .limit(chunkSize)
    .run()
  return chunk
}

/** Gets a DB cursor to the items of a table
 * Uses between to enforce the first element is lastId
 * @param {*} connection Your connection object to rethinkdb
 * @param {string} database Database name
 * @param {string} table Table name
 * @param {number} chunkSize Top Limit of rows to retrieve
 * @param {string} lastId Row Id of the last element of the last chunk processed
 * @returns {Promise<RethinkDbHashObject[]>}
 */
async function getInOrderChunks (connection, database, table, chunkSize, lastId) {
  const chunk = await connection.db(database).table(table)
    .orderBy({ index: connection.asc('id') })
    .between(lastId, connection.maxval)
    .map(function (row) {
      return connection.expr([[row('id'), connection.uuid(row.toJSON())]]).coerceTo('OBJECT')
    })
    .limit(chunkSize)
    .run()
  return chunk
}

/** Retrieves chunks to process and returns a Tuple of Arrays from Source DB and from Target DB
 * @param {*} opts Requires cursors, table name, databases names
 * @param {*} sourceOffset How many items to skip when querying Source DB
 * @param {*} targetOffset How many items to skip when querying Target DB
 * @returns {Promise<[RethinkDbHashObject[], RethinkDbHashObject[]]>}
 */
async function getChunks (opts, sourceOffset, targetOffset) {
  const { props, sr, tr, table } = opts
  if (props.companyIds) {
    return Promise.all([
      getByCompanyChunks(sr, props.sourceDB, table, props.companyIds, props.chunkSize, sourceOffset),
      getByCompanyChunks(tr, props.targetDB, table, props.companyIds, props.chunkSize, targetOffset)
    ])
  } else {
    return Promise.all([
      getInOrderChunks(sr, props.sourceDB, table, props.chunkSize, sourceOffset),
      getInOrderChunks(tr, props.targetDB, table, props.chunkSize, targetOffset)
    ])
  }
}

module.exports = {
  getChunks,
  getByCompanyChunks,
  getInOrderChunks
}
