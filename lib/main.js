const pjson = require('../package.json')
const _ = require('lodash')
const commands = require('./commands')

if (!global.VERSION) {
  global.VERSION = ''
}

let HELPTEXT = `

  Thinker ${pjson.version}
  ==============================

  A RethinkDB command line tool.

  Commands:
    thinker sync            Synchronize differences between two databases.
    thinker -h | --help     Show this screen.

`
module.exports.main = async function (argv) {
  let command = _.first(argv['_'])
  argv['_'] = argv['_'].slice(1)
  try {
    if (commands[command]) {
      await commands[command](argv)
    } else {
      console.log(HELPTEXT)
    }
    process.exit()
  } catch (err) {
    console.log('ERROR')
    console.log(err)
    process.exit(1)
  }
}
