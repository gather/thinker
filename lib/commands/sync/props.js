const _ = require('lodash')

let HELPTEXT = `

Thinker Sync
==============================

Sync two RethinkDB databases.

Usage:
  thinker sync [options]
  thinker sync --sh host[:port] --th host[:port] --sd dbName --td dbName
  thinker sync -h | --help

Options:
  --sh, --sourceHost=<host[:port]>          Source host, defaults to 'localhost:21015'
  --th, --targetHost=<host[:port]>          Target host, defaults to 'localhost:21015'
  --sd, --sourceDB=<dbName>                 Source database
  --td, --targetDB=<dbName>                 Target database

  --pt, --pickTables=<table1,table2>        Comma separated list of tables to sync (whitelist)
  --ot, --omitTables=<table1,table2>        Comma separated list of tables to ignore (blacklist)
                                            Note: '--pt' and '--ot' are mutually exclusive options.

  --user                                    Source and Target username
  --password                                Source and Target password

  --su                                      Source username, overrides --user
  --sp                                      Source password, overrides --password

  --tu                                      Target username, overrides --user
  --tp                                      Target password, overrides --password

  --autoApprove                             Skip interactive approval

  --logLevel                                0 (Default) = ALL
                                            1 = No Progress Log
                                            2 = Errors Only
                                            3 = No logs

  --c, --companyIds=<id1,id2>               Uses secondary ids to filter Company Secondary Index from what is synced

  --indexesOnly                             Just create indexes
  --buffer                                  <number> - Minimum number of connections available in the pool, default 20
  --max                                     <number> - Maximum number of connections available in the pool, default 50

  --chunkSize                               <number> - Buffer to use when retrieving data from RethinkDB, default 100,000
`
module.exports.getProperties = function (argv) {
  const sHost = argv.sh ? argv.sh : argv.sourceHost ? argv.sourceHost : 'localhost:28015'
  const tHost = argv.th ? argv.th : argv.targetHost ? argv.targetHost : 'localhost:28015'
  const sourceHost = _.first(sHost.split(':'))
  const targetHost = _.first(tHost.split(':'))
  const sourcePort = parseInt(_.last(sHost.split(':')), 10) || 28015
  const targetPort = parseInt(_.last(tHost.split(':')), 10) || 28015
  const sourceDB = argv.sd ? argv.sd : argv.sourceDB ? argv.sourceDB : null
  const targetDB = argv.td ? argv.td : argv.targetDB ? argv.targetDB : null
  const sourceUser = argv.su ? argv.su : argv.user ? argv.user : 'admin'
  const sourcePassword = argv.sp ? argv.sp : argv.password ? argv.password : ''
  const targetUser = argv.tu ? argv.tu : argv.user ? argv.user : 'admin'
  const targetPassword = argv.tp ? argv.tp : argv.password ? argv.password : ''
  const autoApprove = argv.autoApprove ? argv.autoApprove : null
  const logLevel = argv.logLevel ? parseInt(argv.logLevel, 10) : 0
  const indexesOnly = argv.indexesOnly ? argv.indexesOnly : null
  const buffer = argv.buffer ? parseInt(argv.buffer, 10) : 100
  const max = argv.max ? parseInt(argv.max, 10) : 750
  const chunkSize = argv.chunkSize ? parseInt(argv.chunkSize, 10) : 200000

  let pickTables = argv.pt ? argv.pt : argv.pickTables ? argv.pickTables : null
  let omitTables = argv.ot ? argv.ot : argv.omitTables ? argv.omitTables : null
  let companyIds = argv.c ? argv.c : argv.companyIds ? argv.companyIds : null
  pickTables = _.isString(pickTables) ? pickTables.split(',') : null
  omitTables = _.isString(omitTables) ? omitTables.split(',') : null
  companyIds = _.isString(companyIds) ? companyIds.split(',') : null

  return {
    help: argv.h || argv.help,
    sourceHost,
    targetHost,
    sourcePort,
    targetPort,
    sourceDB,
    targetDB,
    chunkSize,
    pickTables,
    omitTables,
    companyIds,
    sourceUser,
    sourcePassword,
    targetUser,
    targetPassword,
    autoApprove,
    logLevel,
    indexesOnly,
    buffer,
    max
  }
}

module.exports.validateProps = function (props) {
  if (props.help) {
    console.log(HELPTEXT)
    return false
  }

  if (props.pickTables && props.omitTables) {
    console.log('pickTables and omitTables are mutually exclusive options.')
    return false
  }

  if (!props.sourceDB || !props.targetDB) {
    console.log('Source and target databases are required!')
    console.log(HELPTEXT)
    return false
  }

  if (`${props.sourceHost}:${props.sourcePort}` === `${props.targetHost}:${props.targetPort}` && props.sourceDB === props.targetDB) {
    console.log('Source and target databases must be different if syncing on same server!')
    return false
  }

  return true
}
