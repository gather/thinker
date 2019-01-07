const _ = require('lodash')
const colors = require('colors')

module.exports.validation = function ({ props, sourceDBList, logger, sourceTableList }) {
  if (!sourceDBList.includes(props.sourceDB)) {
    logger.error('Source DB does not exist!')
    return false
  }

  if (props.pickTables && !_.every(props.pickTables, (table) => sourceTableList.includes(table))) {
    logger.error(colors.red('Not all the tables specified in --pickTables exist!'))
    return false
  }

  if (props.omitTables && !_.every(props.omitTables, (table) => sourceTableList.includes(table))) {
    logger.error(colors.red('Not all the tables specified in --omitTables exist!'))
    return false
  }

  let confMessage = `
    ${colors.green('Ready to synchronize!')}
    The database '${colors.yellow(props.sourceDB)}' on '${colors.yellow(props.sourceHost)}:${colors.yellow(props.sourcePort)}' will be synchronized to the '${colors.yellow(props.targetDB)}' database on '${colors.yellow(props.targetHost)}:${colors.yellow(props.targetPort)}'
    This will modify records in the '${colors.yellow(props.targetDB)}' database on '${colors.yellow(props.targetHost)}:${colors.yellow(props.targetPort)}' if it exists!
  `

  if (props.pickTables) {
    confMessage += `  ONLY the following tables will be synchronized: ${colors.yellow(props.pickTables.join(','))}\n`
  }
  if (props.omitTables) {
    confMessage += `  The following tables will NOT be synchronized: ${colors.yellow(props.omitTables.join(','))}\n`
  }
  logger.info(confMessage)

  return true
}
