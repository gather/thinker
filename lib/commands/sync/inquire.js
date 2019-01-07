const inquirer = require('inquirer')
const colors = require('colors')

module.exports.inquire = async function (logger) {
  let answer = await inquirer.prompt([{
    type: 'confirm',
    name: 'confirmed',
    message: 'Proceed?',
    default: false
  }])

  if (!answer.confirmed) {
    logger.error(colors.red('ABORT!'))
    return false
  }
  return true
}
