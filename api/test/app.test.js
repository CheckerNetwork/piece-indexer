import { RedisRepository } from '@filecoin-station/spark-piece-indexer-repository'
import { Redis } from 'ioredis'
import assert from 'node:assert'
import { after, before, beforeEach, describe, it } from 'node:test'
import { createApp } from '../lib/app.js'
import { assertResponseStatus } from './test-helpers.js'

describe('HTTP request handler', () => {
  /** @type {Redis} */
  let redis
  /** @type {RedisRepository} */
  let repository

  /** @type {import('fastify').FastifyInstance} */
  let app
  /** @type {string} */
  let baseUrl

  before(async () => {
    redis = new Redis({ db: 1 })
    repository = new RedisRepository(redis)

    app = createApp({
      repository,
      domain: false,
      logger: {
        level: process.env.DEBUG === '*' || process.env.DEBUG?.includes('test')
          ? 'debug'
          : 'error'
      }
    })
    baseUrl = await app.listen()
  })

  beforeEach(async () => {
    await redis.flushall()
  })

  after(async () => {
    await app.close()
    await redis?.disconnect()
  })

  it('returns 404 for GET /', async () => {
    const res = await fetch(new URL('/', baseUrl))
    await assertResponseStatus(res, 404)
  })

  describe('GET /samples/{providerId}/{pieceCid}', () => {
    it('returns the first payload block from the index', async () => {
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'payload-cid-1', 'payload-cid-2')
      await repository.addPiecePayloadBlocks('provider-id2', 'piece-cid', 'payload-cid-1', 'payload-cid-2')
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid2', 'payload-cid-1', 'payload-cid-2')

      const res = await fetch(new URL('/sample/provider-id/piece-cid', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        samples: ['payload-cid-1']
      })
      assert.strictEqual(
        res.headers.get('cache-control'),
        `public, max-age=${24 * 3600}, immutable`
      )
    })

    it('returns error when provider is not found', async () => {
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'payload-cid')

      const res = await fetch(new URL('/sample/unknown-provider-id/piece-cid', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        error: 'PROVIDER_OR_PIECE_NOT_FOUND'
      })
      assert.strictEqual(
        res.headers.get('cache-control'),
        `public, max-age=${60}`
      )
    })

    it('returns error when provider piece is not found', async () => {
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'payload-cid')

      const res = await fetch(new URL('/sample/provider-id/unknown-piece-cid', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        error: 'PROVIDER_OR_PIECE_NOT_FOUND'
      })
      assert.strictEqual(
        res.headers.get('cache-control'),
        `public, max-age=${60}`
      )
    })
  })

  describe('GET /ingestion-status/{providerId}', () => {
    it('returns info about a provider seen by the indexer', async () => {
      await repository.setWalkerState('provider-id', { status: 'walking', lastHead: 'last-head' })
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'bafy1')

      const res = await fetch(new URL('/ingestion-status/provider-id', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        providerId: 'provider-id',
        // TODO
        // providerAddress: "state.providerAddress",
        ingestionStatus: 'walking',
        lastHeadWalkedFrom: 'last-head',
        adsMissingPieceCID: 0,
        entriesNotRetrievable: 0,
        piecesIndexed: 1
      })

      assert.strictEqual(
        res.headers.get('cache-control'),
        `public, max-age=${60}`
      )
    })

    it('returns error for an unknown provider', async () => {
      await repository.setWalkerState('provider-id', {
        status: 'walking',
        tail: 'tail',
        lastHead: 'last-head'
      })
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'bafy1')

      const res = await fetch(new URL('/ingestion-status/unknown-provider-id', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        providerId: 'unknown-provider-id',
        ingestionStatus: 'Unknown provider ID'
      })

      assert.strictEqual(
        res.headers.get('cache-control'),
        `public, max-age=${60}`
      )
    })

    it('returns "head" as "lastHead" when the initial walk has not finished yet', async () => {
      await repository.setWalkerState('provider-id', { status: 'walking', head: 'head' })
      await repository.addPiecePayloadBlocks('provider-id', 'piece-cid', 'bafy1')

      const res = await fetch(new URL('/ingestion-status/provider-id', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        providerId: 'provider-id',
        ingestionStatus: 'walking',
        lastHeadWalkedFrom: 'head',
        adsMissingPieceCID: 0,
        entriesNotRetrievable: 0,
        piecesIndexed: 1
      })

      assert.strictEqual(
        res.headers.get('cache-control'),
          `public, max-age=${60}`
      )
    })

    it('returns the number of adsMissingPieceCID and entriesNotRetrievable', async () => {
      await repository.setWalkerState('provider-id', {
        status: 'walking',
        head: 'head',
        entriesNotRetrievable: 10,
        adsMissingPieceCID: 20
      })

      const res = await fetch(new URL('/ingestion-status/provider-id', baseUrl))
      await assertResponseStatus(res, 200)
      const body = await res.json()

      assert.deepStrictEqual(body, {
        providerId: 'provider-id',
        ingestionStatus: 'walking',
        lastHeadWalkedFrom: 'head',
        entriesNotRetrievable: 10,
        adsMissingPieceCID: 20,
        piecesIndexed: 0
      })

      assert.strictEqual(
        res.headers.get('cache-control'),
          `public, max-age=${60}`
      )
    })
  })
})
