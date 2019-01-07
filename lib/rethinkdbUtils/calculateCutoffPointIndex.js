const _ = require('lodash')

/** Gets the offsets to the last element processed in each array
 * @param {RethinkDbHashObject[]} sourceChunk Array of { id: hash } on Source Database
 * @param {RethinkDbHashObject[]} targetChunk Array of { id: hash } on Target Database
 * @param {number} chunkSize Buffer of items to retrieve from RethinkDB
 * @param {string[]} companyIds Array of company ids
 * @returns {[number, number]} Offset to last item to be processed in [sourceDb, targetDb]
 */
module.exports.calculateCutoffPointIndex = function (sourceChunk, targetChunk, chunkSize) {
  // target is empty, process all in source
  if (targetChunk.length === 0) {
    return [ sourceChunk.length, 0 ]
  }
  // there are less items in target than in source
  // process all source items
  if (targetChunk.length < chunkSize) {
    return [ sourceChunk.length, targetChunk.length ]
  }
  // there are less items in source than in target
  // some items were deleted
  // 1. check the lastItem in targetChunk
  // 2. get newOffset - it is the array from sourceChunk from beginning to lastItem of targetChunk
  // 3. process all until that newOffset, and use that as the start of the next chunk
  const sourceChunkMap = sourceChunk.reduce((obj, rowIdAndHash, idx) => {
    const k = Object.keys(rowIdAndHash)[0]
    obj[k] = idx
    return obj
  }, {})
  let sourceLastIndex
  let targetLastIndex
  _.forEachRight(targetChunk, (rowIdAndHash, index) => {
    const k = Object.keys(rowIdAndHash)[0]
    if (k in sourceChunkMap) {
      sourceLastIndex = sourceChunkMap[rowIdAndHash]
      targetLastIndex = index
      // We found our targets, break the loop
      return false
    }
  })
  if (sourceLastIndex === undefined && targetLastIndex === undefined) {
    // All the Elements in Chunk from DB A are new
    return [ sourceChunk.length, 0 ]
  } else {
    return [ sourceLastIndex, targetLastIndex ]
  }
}
