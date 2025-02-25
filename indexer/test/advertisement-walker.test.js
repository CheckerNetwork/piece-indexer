import { RedisRepository } from '@filecoin-station/spark-piece-indexer-repository'
import { Redis } from 'ioredis'
import assert from 'node:assert'
import { after, before, beforeEach, describe, it } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import {
  fetchAdvertisedPayload,
  processNextAdvertisement,
  walkOneStep
} from '../lib/advertisement-walker.js'
import { givenHttpServer } from './helpers/http-server.js'
import { FRISBII_ADDRESS, FRISBII_AD_CID } from './helpers/test-data.js'
import { assertOkResponse } from '../lib/http-assertions.js'
import * as stream from 'node:stream'
import { pipeline } from 'node:stream/promises'

/** @import { ProviderInfo, WalkerState } from '../lib/typings.js' */

// TODO(bajtos) We may need to replace this with a mock index provider
const providerId = '12D3KooWHKeaNCnYByQUMS2n5PAZ1KZ9xKXqsb4bhpxVJ6bBJg5V'
const providerAddress = 'http://f010479.twinquasar.io:3104'
const providerAddressHttpPath = '/dns/f03303347-market.duckdns.org/https/http-path/%2Fipni-provider%2F12D3KooWJ91c6xQshrNe7QAXPFAaeRrHWq2UrgXGPf8UmMZMwyZ5'

// The advertisement chain looks this way:
//
//  adCid - advertises payloadCid and pieceCid
//    ↓
//  previousAdCid
//    ↓
//  previousPreviousAdCid
//    ↓
//  (...)
const knownAdvertisement = {
  adCid: 'baguqeerarbmakqcnzzuhki25xs357xyin4ieqxvumrp5cy7s44v7tzwwmg3q',
  previousAdCid: 'baguqeerau2rz67nvzcaotgowm2olalanx3eynr2asbjwdkaq3y5umqvdi2ea',
  previousPreviousAdCid: 'baguqeeraa5mjufqdwuwrrrqboctnn3vhdlq63rj3hce2igpzbmae7sazkfea',
  payloadCid: 'bafkreigrnnl64xuevvkhknbhrcqzbdvvmqnchp7ae2a4ulninsjoc5svoq',
  pieceCid: 'baga6ea4seaqlwzed5tgjtyhrugjziutzthx2wrympvsuqhfngwdwqzvosuchmja'
}
describe('processNextAdvertisement', () => {
  it('ignores non-HTTP(s) addresses and explains the problem in the status', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: '/ip4/127.0.0.1/tcp/80',
      lastAdvertisementCID: 'baguqeeraTEST'
    }

    const result = await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState: undefined
    })

    assert.deepStrictEqual(result, {
      finished: true,
      newState: {
        status: 'Index provider advertises over an unsupported protocol: /ip4/127.0.0.1/tcp/80'
      }
    })
  })

  it('ignores malformed http-path addresses explains the problem in the status', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: '/dns/meridian.space/http/http-path/invalid%path',
      lastAdvertisementCID: 'baguqeeraTEST'
    }

    const result = await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState: undefined
    })
    assert.deepStrictEqual(result, {
      finished: true,
      newState: {
        status: 'Index provider advertises over an unsupported protocol: /dns/meridian.space/http/http-path/invalid%path'
      }
    })
  })

  it('handles a new index provider not seen before', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }
    const walkerState = undefined
    const result = await processNextAdvertisement({ providerId, providerInfo, walkerState })
    assert.deepStrictEqual(result, {
      /** @type {WalkerState} */
      newState: {
        head: providerInfo.lastAdvertisementCID,
        tail: knownAdvertisement.previousAdCid,
        lastHead: undefined,
        status: `Walking the advertisements from ${knownAdvertisement.adCid}, next step: ${knownAdvertisement.previousAdCid}`
      },
      indexEntry: {
        payloadCid: knownAdvertisement.payloadCid,
        pieceCid: knownAdvertisement.pieceCid
      },
      finished: false
    })
  })

  it('does nothing when the last advertisement has been already processed', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }

    /** @type {WalkerState} */
    const walkerState = {
      head: undefined,
      tail: undefined,
      lastHead: knownAdvertisement.adCid,
      status: 'some-status'
    }

    const result = await processNextAdvertisement({ providerId, providerInfo, walkerState })
    assert.deepStrictEqual(result, {
      finished: true
    })
  })

  it('moves the tail by one step', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }

    /** @type {WalkerState} */
    const walkerState = {
      head: knownAdvertisement.adCid,
      tail: knownAdvertisement.previousAdCid,
      lastHead: undefined,
      status: 'some-status'
    }

    const { newState, indexEntry, finished } = await processNextAdvertisement({ providerId, providerInfo, walkerState })

    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: walkerState.head, // this does not change during the walk
      tail: knownAdvertisement.previousPreviousAdCid,
      lastHead: walkerState.lastHead, // this does not change during the walk
      status: `Walking the advertisements from ${walkerState.head}, next step: ${knownAdvertisement.previousPreviousAdCid}`
    }))

    assert(indexEntry, 'the step found an index entry')

    assert.strictEqual(finished, false, 'finished')
  })

  it('starts a new walk for a known provider', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }

    const walkerState = {
      head: undefined, // previous walk was finished
      tail: undefined, // previous walk was finished
      lastHead: knownAdvertisement.previousPreviousAdCid, // an advertisement later in the chain
      status: 'some-status'
    }

    const { newState, finished } = await processNextAdvertisement({ providerId, providerInfo, walkerState })

    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: knownAdvertisement.adCid,
      tail: knownAdvertisement.previousAdCid,
      lastHead: walkerState.lastHead, // this does not change during the walk
      status: `Walking the advertisements from ${knownAdvertisement.adCid}, next step: ${knownAdvertisement.previousAdCid}`
    }))

    assert.strictEqual(finished, false, 'finished')
  })

  it('updates lastHead after tail reaches the end of the advertisement chain', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: FRISBII_ADDRESS,
      lastAdvertisementCID: FRISBII_AD_CID
    }

    const walkerState = undefined

    const { newState, finished } = await processNextAdvertisement({ providerId, providerInfo, walkerState })

    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: undefined, // we finished the walk, there is no head
      tail: undefined, // we finished the walk, there is no next step
      lastHead: FRISBII_AD_CID, // lastHead was updated to head of the walk we finished
      adsMissingPieceCID: 1,
      status: `All advertisements from ${newState?.lastHead} to the end of the chain were processed.`
    }))

    assert.strictEqual(finished, true, 'finished')
  })

  it('handles a walk that ends but does not link to old chain', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: FRISBII_ADDRESS,
      lastAdvertisementCID: FRISBII_AD_CID
    }

    const walkerState = {
      head: undefined, // previous walk was finished
      tail: undefined, // previous walk was finished
      lastHead: knownAdvertisement.adCid, // arbitrary advertisement
      status: 'some-status'
    }

    const { newState, finished } = await processNextAdvertisement({ providerId, providerInfo, walkerState })

    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: undefined, // we finished the walk, there is no head
      tail: undefined, // we finished the walk, there is no next step
      lastHead: FRISBII_AD_CID, // lastHead was updated to head of the walk we finished
      adsMissingPieceCID: 1,
      status: `All advertisements from ${newState?.lastHead} to the end of the chain were processed.`
    }))

    assert.strictEqual(finished, true, 'finished')
  })

  it('updates lastHead after tail reaches lastHead', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }

    /** @type {WalkerState} */
    const walkerState = {
      head: knownAdvertisement.adCid,
      tail: knownAdvertisement.previousAdCid,
      lastHead: knownAdvertisement.previousPreviousAdCid,
      status: 'some-status'
    }

    const { newState, indexEntry, finished } = await processNextAdvertisement({ providerId, providerInfo, walkerState })

    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: undefined, // we finished the walk, there is no head
      tail: undefined, // we finished the walk, there is no next step
      lastHead: walkerState.head, // lastHead was updated to head of the walk we finished
      status: `All advertisements from ${newState?.lastHead} to the end of the chain were processed.`
    }))

    assert(indexEntry, 'the step found an index entry')

    assert.strictEqual(finished, true, 'finished')
  })

  it('handles Fetch errors and explains the problem in the status', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: 'http://127.0.0.1:80/',
      lastAdvertisementCID: 'baguqeeraTEST'
    }

    const result = await processNextAdvertisement({ providerId, providerInfo, walkerState: undefined })

    assert.deepStrictEqual(result, {
      failed: true,
      newState: {
        head: 'baguqeeraTEST',
        tail: 'baguqeeraTEST',
        lastHead: undefined,
        status: 'Error processing baguqeeraTEST: HTTP request to http://127.0.0.1/ipni/v1/ad/baguqeeraTEST failed: connect ECONNREFUSED 127.0.0.1:80'
      }
    })
  })

  it('handles timeout errors and explains the problem in the status', async () => {
    const { serverUrl } = await givenHttpServer(async (_req, res) => {
      await setTimeout(100)
      res.statusCode = 501
      res.end()
    })

    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: serverUrl,
      lastAdvertisementCID: 'baguqeeraTEST'
    }

    const result = await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState: undefined,
      fetchTimeout: 1
    })

    assert.deepStrictEqual(result, {
      failed: true,
      newState: {
        head: 'baguqeeraTEST',
        tail: 'baguqeeraTEST',
        lastHead: undefined,
        status: `Error processing baguqeeraTEST: HTTP request to ${serverUrl}ipni/v1/ad/baguqeeraTEST failed: operation timed out`
      }
    })
  })

  it('skips entries where the server responds with 404 cid not found and updates the counter', async () => {
    const { adCid, previousAdCid } = knownAdvertisement

    const { serverUrl } = await givenHttpServer(async (req, res) => {
      if (req.url === `/ipni/v1/ad/${adCid}`) {
        const providerRes = await fetch(providerAddress + req.url)
        await assertOkResponse(providerRes)
        assert(providerRes.body, 'provider response does not have any body')
        await pipeline(stream.Readable.fromWeb(providerRes.body), res)
      } else {
        res.statusCode = 404
        res.write('cid not found')
        res.end()
      }
    })

    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: serverUrl,
      lastAdvertisementCID: adCid
    }

    const result = await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState: undefined
    })

    assert.deepStrictEqual(result, {
      finished: false,
      indexEntry: undefined,
      newState: {
        entriesNotRetrievable: 1,
        head: adCid,
        tail: previousAdCid,
        lastHead: undefined,
        status: `Walking the advertisements from ${adCid}, next step: ${previousAdCid}`
      }
    })
  })

  it('skips entries when PieceCID is not in advertisement metadata and updates the counter', async () => {
    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress: FRISBII_ADDRESS,
      lastAdvertisementCID: FRISBII_AD_CID
    }

    const result = await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState: undefined
    })

    assert.deepStrictEqual(result, {
      finished: true,
      indexEntry: undefined,
      newState: {
        adsMissingPieceCID: 1,
        head: undefined,
        tail: undefined,
        lastHead: FRISBII_AD_CID,
        status: `All advertisements from ${FRISBII_AD_CID} to the end of the chain were processed.`
      }
    })
  })
})

/** @typedef {Awaited<ReturnType<fetchAdvertisedPayload>>} AdvertisedPayload */

describe('fetchAdvertisedPayload', () => {
  it('returns previousAdvertisementCid, pieceCid and payloadCid for Graphsync retrievals', async () => {
    const result = await fetchAdvertisedPayload(providerAddress, knownAdvertisement.adCid)
    assert.deepStrictEqual(result, /** @type {AdvertisedPayload} */({
      entry: {
        payloadCid: knownAdvertisement.payloadCid,
        pieceCid: knownAdvertisement.pieceCid
      },
      previousAdvertisementCid: knownAdvertisement.previousAdCid
    }))
  })

  it('returns MISSING_PIECE_CID error for HTTP retrievals', async () => {
    const result = await fetchAdvertisedPayload(FRISBII_ADDRESS, FRISBII_AD_CID)
    assert.deepStrictEqual(result, /** @type {AdvertisedPayload} */({
      error: 'MISSING_PIECE_CID',
      // Our Frisbii instance announced only one advertisement
      // That's unrelated to HTTP vs Graphsync retrievals
      previousAdvertisementCid: undefined
    }))
  })
})

describe('walkOneStep', () => {
  const providerId = '12D3KooTEST'

  /** @type {Redis} */
  let redis

  before(async () => {
    redis = new Redis({ db: 1 })
  })

  beforeEach(async () => {
    await redis.flushall()
  })

  after(async () => {
    await redis?.disconnect()
  })

  it('handles a new index provider not seen before', async () => {
    const repository = new RedisRepository(redis)

    const nextHead = knownAdvertisement.adCid

    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: nextHead
    }

    const result = await walkOneStep({ repository, providerId, providerInfo })
    assert.strictEqual(!!result.finished, false)
    assert.strictEqual(!!result.failed, false)

    const newState = await repository.getWalkerState(providerId)
    assert.deepStrictEqual(newState, /** @type {WalkerState} */({
      head: nextHead,
      tail: knownAdvertisement.previousAdCid,
      // lastHead: undefined,
      status: `Walking the advertisements from ${nextHead}, next step: ${knownAdvertisement.previousAdCid}`
    }))

    assert.deepStrictEqual(result.walkerState, { lastHead: undefined, ...newState })

    const pieceBlocks = await repository.getPiecePayloadBlocks(providerId, knownAdvertisement.pieceCid)
    assert.deepStrictEqual(pieceBlocks, [knownAdvertisement.payloadCid])
  })
})

describe('data schema for REST API', () => {
  /** @type {Redis} */
  let redis
  /** @type {RedisRepository} */
  let repository

  before(async () => {
    redis = new Redis({ db: 1 })

    repository = new RedisRepository(redis)

    /** @type {ProviderInfo} */
    const providerInfo = {
      providerAddress,
      lastAdvertisementCID: knownAdvertisement.adCid
    }

    await walkOneStep({ repository, providerId, providerInfo })
  })

  after(async () => {
    await redis?.disconnect()
  })

  it('supports GET /sample/{providerId}/{pieceCid}', async () => {
    // Proposed API response - see docs/design.md
    // {
    //   "samples": ["exactly one CID of a payload block advertised for PieceCID"],
    //   "pubkey": "server's public key",
    //   "signature": "signature over dag-json{providerId,pieceCid,seed,samples}"
    // }
    // pubkey & signature will be handled by the REST API server,
    // we are only interested in `samples` in this test
    const samples = await repository.getPiecePayloadBlocks(providerId, knownAdvertisement.pieceCid)
    assert.deepStrictEqual(samples, [knownAdvertisement.payloadCid])
  })

  it('supports GET /ingestion-status/{providerId}', async () => {
    // Proposed API response - see docs/design.md
    // {
    //   "providerId": "state.providerId",
    //   "providerAddress": "state.providerAddress",
    //   "ingestionStatus": "state.ingestion_status",
    //   "lastHeadWalkedFrom": "state.lastHead",
    //   "piecesIndexed": 123
    //   // ^^ number of (PieceCID, PayloadCID) records found for this provider
    // }
    const walkerState = await repository.getWalkerState(providerId)
    const response = {
      providerId,
      // Discussion point:
      // We don't have providerAddress in the walker state.
      // Is it a problem if our observability API does not tell the provider address?
      ingestionStatus: walkerState.status,
      lastHeadWalkedFrom: walkerState.lastHead ?? walkerState.head,
      adsMissingPieceCID: walkerState.adsMissingPieceCID ?? 0,
      entriesNotRetrievable: walkerState.entriesNotRetrievable ?? 0,
      piecesIndexed: await repository.countPiecesIndexed(providerId)
    }

    assert.deepStrictEqual(response, {
      providerId,
      ingestionStatus: `Walking the advertisements from ${knownAdvertisement.adCid}, next step: ${knownAdvertisement.previousAdCid}`,
      lastHeadWalkedFrom: knownAdvertisement.adCid,
      adsMissingPieceCID: 0,
      entriesNotRetrievable: 0,
      piecesIndexed: 1
    })
  })
})
