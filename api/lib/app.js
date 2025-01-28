import * as Sentry from '@sentry/node'
import Fastify from 'fastify'

/**
 * @param {object} args
 * @param {Repository} args.repository
 * @param {string|boolean} args.domain
 * @param {Fastify.FastifyLoggerOptions} args.logger
 */
export function createApp ({ repository, domain, logger }) {
  const app = Fastify({ logger })
  Sentry.setupFastifyErrorHandler(app)

  if (typeof domain === 'string') {
    app.addHook('onRequest', async (request, reply) => {
      if (request.headers.host.split(':')[0] !== domain) {
        reply.redirect(`https://${domain}${request.url}`, 301)
      }
    })
  }

  app.get('/sample/:providerId/:pieceCid', async (request, reply) => {
    const { providerId, pieceCid } = request.params
    const payloadCids = await repository.getPiecePayloadBlocks(providerId, pieceCid)
    const body = {}
    if (payloadCids.length) {
      body.samples = payloadCids.slice(0, 1)
      reply.header('cache-control', `public, max-age=${24 * 3600 /* 24 hours */}, immutable`)
    } else {
      body.error = 'PROVIDER_OR_PIECE_NOT_FOUND'
      reply.header('cache-control', `public, max-age=${60 /* 1min */}`)
    }
    reply.send(body)
  })

  app.get('/ingestion-status/:providerId', async (request, reply) => {
    const { providerId } = request.params
    const walkerState = await repository.getWalkerState(providerId)
    reply.header('cache-control', `public, max-age=${60 /* 1min */}`)

    if (!walkerState) {
      return reply.send({
        providerId,
        ingestionStatus: 'Unknown provider ID'
      })
    }

    return reply.send({
      providerId,
      // Discussion point:
      // We don't have providerAddress in the walker state.
      // Is it a problem if our observability API does not tell the provider address?
      ingestionStatus: walkerState.status,
      lastHeadWalkedFrom: walkerState.lastHead ?? walkerState.head,
      adsMissingPieceCID: walkerState.adsMissingPieceCID ?? 0,
      entriesNotRetrievable: walkerState.entriesNotRetrievable ?? 0,
      piecesIndexed: await repository.countPiecesIndexed(providerId)
    })
  })

  return app
}
