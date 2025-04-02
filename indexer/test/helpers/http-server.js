import { once } from 'node:events'
import http from 'node:http'

/**
 * @param {http.RequestListener} handler
 */
export async function givenHttpServer(handler) {
  const server = http.createServer((req, res) => {
    // Wrap the handler() call in an async function block to ensure synchronously thrown errors
    // are converted to rejected promises.
    ;(async () => {
      await handler(req, res)
    })().catch((err) => {
      console.log('Unhandled server error:', err)
      res.statusCode = 500
      res.write(err.message || err.toString())
      res.end()
    })
  })

  server.listen(0, '127.0.0.1')
  server.unref()
  await once(server, 'listening')
  const serverPort = /** @type {import('node:net').AddressInfo} */ (server.address()).port
  const serverUrl = `http://127.0.0.1:${serverPort}/`
  return { server, serverPort, serverUrl }
}
