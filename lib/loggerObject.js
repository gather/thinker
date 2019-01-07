
/**
 * @typedef {Object} loggerObject
 * @function stdout
 * @param {any} msg Injects to process.stdout directly
 * @function info
 * @param {any} msg The message to put in stdout
 * @function error
 * @param {any} msg The message to put in stderr
 */

/** Global logger
 * @param {number} level Desired logger level 0 (Default) = ALL
                                              1 = No Progress Log
                                              2 = Errors Only
                                              3 = No logs
 * @returns {loggerObject}
 */
module.exports.getLoggerObject = function (level = 0) {
  return {
    stdout: (msg) => {
      if (level === 0) {
        process.stdout.write(msg)
      }
    },
    info: (msg) => {
      if (level <= 1) {
        console.log(msg)
      }
    },
    error: (msg) => {
      if (level <= 2) {
        console.error(msg)
      }
    }
  }
}
