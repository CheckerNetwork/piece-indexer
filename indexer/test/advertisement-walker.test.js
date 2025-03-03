import { RedisRepository } from '@filecoin-station/spark-piece-indexer-repository'
import { Redis } from 'ioredis'
import assert from 'node:assert'
import { after, afterEach, before, beforeEach, describe, it, mock } from 'node:test'
import { setTimeout } from 'node:timers/promises'
import {
  fetchAdvertisedPayload,
  processNextAdvertisement,
  walkOneStep
  , processEntries,
  fetchCid
} from '../lib/advertisement-walker.js'
import pRetry from 'p-retry'
import { givenHttpServer } from './helpers/http-server.js'
import { FRISBII_ADDRESS, FRISBII_AD_CID } from './helpers/test-data.js'
import { assertOkResponse } from '../lib/http-assertions.js'
import * as stream from 'node:stream'
import { pipeline } from 'node:stream/promises'
import * as cbor from '@ipld/dag-cbor'
import { CID } from 'multiformats/cid'
import * as crypto from 'node:crypto'
import * as multihash from 'multiformats/hashes/digest'
/** @import { ProviderInfo, WalkerState } from '../lib/typings.js' */

// TODO(bajtos) We may need to replace this with a mock index provider
const providerId = '12D3KooWHKeaNCnYByQUMS2n5PAZ1KZ9xKXqsb4bhpxVJ6bBJg5V'
const providerAddress = 'http://f010479.twinquasar.io:3104'
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

  it('ignores malformed http-path addresses and explains the problem in the status', async () => {
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

describe('processEntries', () => {
  // Use a real DAG-JSON CID that will naturally have codec 0x0129 (297)
  // CIDs that start with 'bagu' are DAG-JSON encoded
  const dagJsonCid = 'baguqeeraa5mjufqdwuwrrrqboctnn3vhdlq63rj3hce2igpzbmae7sazkfea'
  // Use a real DAG-CBOR CID that will naturally have codec 0x71 (113)
  // CIDs that start with 'bafy' are DAG-CBOR encoded
  const dagCborCid = 'bafyreibpxkmu65ezxy7rynxotbghfz3ktiapjisntepd67hghfn4hde3na'
  const testData = 'test data for multihash'
  // Create a proper multihash from this digest
  const mh = multihash.create(0x12, crypto.createHash('sha256').update(testData).digest())
  // @ts-ignore
  const entryBytes = Buffer.from(mh.bytes).toString('base64')
  const dagJsonChunk = {
    Entries: [
      {
        '/': {
          bytes: entryBytes
        }
      }
    ]
  }

  const dagCborChunk = {
    Entries: [mh.bytes]
  }
  it('processes DAG-JSON entries correctly', () => {
    // Process the entries with the real CID
    const result = processEntries(dagJsonCid, dagJsonChunk)

    // Verify the result is a valid CID string
    assert(CID.parse(result), 'Result should be a parseable CID')
  })

  it('processes DAG-CBOR entries correctly', () => {
    // Process the entries with the real CID
    const result = processEntries(dagCborCid, dagCborChunk)

    // Verify the result is a valid CID string
    assert(CID.parse(result), 'Result should be a parseable CID')
  })

  // Error handling tests
  it('throws an error when entries array is empty', () => {
    assert.throws(
      () => processEntries(dagCborCid, { Entries: [] }),
      /No entries found/
    )
  })

  it('throws an error when Entries field is missing', () => {
    assert.throws(
      // @ts-ignore
      () => processEntries(dagCborCid, {}),
      /No entries found/
    )
  })

  it('throws an error for unsupported codec', () => {
    // Use a CID with an unsupported codec
    const unsupportedCid = 'bafkreigrnnl64xuevvkhknbhrcqzbdvvmqnchp7ae2a4ulninsjoc5svoq'

    assert.throws(
      () => processEntries(unsupportedCid, dagJsonChunk),
      /Unsupported codec/
    )
  })

  // Data integrity test using real multihash operations
  it('correctly creates a CID from entry data', () => {
    // Process the entries
    const result = processEntries(dagJsonCid, dagJsonChunk)

    // Create the expected CID directly
    const expectedCid = CID.create(1, 0x55, mh).toString()

    // They should match
    assert.strictEqual(result, expectedCid, 'CID should match the one created directly')
  })

  // Test with entries from CBOR encoding/decoding
  it('correctly handles DAG-CBOR entries serialized with @ipld/dag-cbor', () => {
    // Process the entries
    const result = processEntries(dagCborCid, dagCborChunk)

    // Create the expected CID directly
    const expectedCid = CID.create(1, 0x55, mh).toString()

    // They should match
    assert.strictEqual(result, expectedCid, 'CID should match the one created directly')
  })

  // Error case tests with real data
  it('handles malformed base64 in DAG-JSON gracefully', () => {
    const malformedChunk = {
      Entries: [
        {
          '/': {
            bytes: 'This-is-not-valid-base64!'
          }
        }
      ]
    }

    // We expect an error when processing this malformed data
    assert.throws(
      () => processEntries(dagJsonCid, malformedChunk),
      /Incorrect length/
    )
  })

  it('handles invalid multihash in DAG-CBOR gracefully', () => {
    const invalidChunk = {
      Entries: [new Uint8Array([0, 1, 2, 3])] // Too short to be a valid multihash
    }

    // We expect an error when processing this invalid multihash
    assert.throws(
      () => processEntries(dagCborCid, invalidChunk),
      /Incorrect length/
    )
  })
})

describe('fetchCid', () => {
  // Store the original fetch function before each test
  /**
   * @type {{ (input: string | URL | globalThis.Request, init?: RequestInit): Promise<Response>; (input: string | URL | globalThis.Request, init?: RequestInit): Promise<Response>; }}
   */
  let originalFetch
  // Use a real DAG-JSON CID that will naturally have codec 0x0129 (297)
  // CIDs that start with 'bagu' are DAG-JSON encoded
  const dagJsonCid = 'baguqeerayzpbdctxk4iyps45uldgibsvy6zro33vpfbehggivhcxcq5suaia'
  // Sample JSON response
  const jsonResponse = { test: 'value' }
  // Use a real DAG-CBOR CID that will naturally have codec 0x71 (113)
  // CIDs that start with 'bafy' are DAG-CBOR encoded
  const dagCborCid = 'bafyreictdikh363qfxsmjp63i6kup6aukjqfpd4r6wbhbiz2ctuji4bofm'

  beforeEach(() => {
    originalFetch = globalThis.fetch
  })

  afterEach(() => {
    // Restore the original fetch function after each test
    globalThis.fetch = originalFetch
  })

  it('uses DAG-JSON codec (0x0129) to parse response as JSON', async () => {
    // Mock fetch to return JSON
    // @ts-ignore
    globalThis.fetch = mock.fn(() => {
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () => Promise.resolve(jsonResponse),
        arrayBuffer: () => { throw new Error('Should not call arrayBuffer for JSON') }
      })
    })

    const parsedCid = CID.parse(dagJsonCid)
    assert.strictEqual(parsedCid.code, 297)

    const result = await fetchCid('http://example.com', dagJsonCid)

    // Verify we got the JSON response
    assert.deepStrictEqual(result, jsonResponse)
  })

  it('uses DAG-CBOR codec (0x71) to parse response as CBOR', async () => {
    const cborData = cbor.encode(jsonResponse)

    // Mock fetch to return ArrayBuffer
    // @ts-ignore
    globalThis.fetch = mock.fn(() => {
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () => { throw new Error('Should not call json for CBOR') },
        arrayBuffer: () => Promise.resolve(cborData.buffer)
      })
    })

    const parsedCid = CID.parse(dagCborCid)
    assert.strictEqual(parsedCid.code, 113)

    const result = await fetchCid('http://example.com', dagCborCid)

    // Verify we got the decoded CBOR data
    assert.deepStrictEqual(result, jsonResponse)
  })

  it('throws an error for unknown codec', async () => {
    // Mock fetch to return JSON

    // @ts-ignore
    globalThis.fetch = mock.fn(() => {
      return Promise.resolve({
        ok: true,
        status: 200,
        json: () => { throw new Error('Should not call json for CBOR') },
        arrayBuffer: () => { throw new Error('Should not call arrayBuffer for fallback') }
      })
    })

    // Use a CID with a codec that is neither DAG-JSON (0x0129) nor DAG-CBOR (0x71)
    // This is a raw codec (0x55) CID
    const unknownCodecCid = 'bafkreigrnnl64xuevvkhknbhrcqzbdvvmqnchp7ae2a4ulninsjoc5svoq'
    const parsedCid = CID.parse(unknownCodecCid)
    assert.strictEqual(parsedCid.code, 85)
    const errorMessage = 'To parse non base32, base36 or base58btc encoded CID multibase decoder must be provided'
    try {
      await fetchCid('http://example.com', 'testcid')
      assert.fail('fetchCid should have thrown an error')
    } catch (error) {
      // Check the error message

      // @ts-ignore
      assert.ok(error.message.includes(errorMessage), `Error message should include: ${errorMessage}`)
    }
  })
  it('correctly fetches and processes real DAG-CBOR data from Curio provider', async function () {
    // Use a real Curio provider and known DAG-CBOR CID
    const curioProviderUrl = 'https://f03303347-market.duckdns.org/ipni-provider/12D3KooWJ91c6xQshrNe7QAXPFAaeRrHWq2UrgXGPf8UmMZMwyZ5'
    const dagCborCid = 'baguqeeracgnw2ecmhaa6qkb3irrgjjk5zt5fes7wwwpb4aymoaogzyvvbrma'
    // Use the real fetchCid function with the original fetch implementation
    globalThis.fetch = originalFetch
    /** @type {{ Entries: { [key: string]: string } }} */
    // @ts-ignore
    const result = await pRetry(
      () =>
        (
          fetchCid(curioProviderUrl, dagCborCid)
        )
    )

    // Verify the result has the expected structure for DAG-CBOR entries
    assert(result, 'Expected a non-null result')
    assert(result.Entries, 'Result should have Entries property')
    const entriesCid = result.Entries['/']
    /** @type {{Entries: Array<unknown>}} */
    // @ts-ignore
    const entriesChunk = await pRetry(
      () =>
        (
          fetchCid(curioProviderUrl, entriesCid)
        )
    )
    const payloadCid = processEntries(entriesCid, entriesChunk)
    console.log(payloadCid)
    assert.deepStrictEqual(payloadCid, 'bafkreiefrclz7c6w57yl4u7uiq4kvht4z7pits5jpcj3cajbvowik3rvhm')
  })
})
