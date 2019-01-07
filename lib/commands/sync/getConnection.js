module.exports.getConnection = function ({ host, port, user, password, buffer, max }) {
  return require('rethinkdbdash')({
    host,
    port,
    user,
    password,
    timeout: 10,
    timeoutError: 60000, // Wait 60 seconds before trying to reconnect in case of an error
    timeoutGb: 600,
    buffer,
    max,
    silent: false
  })
}
