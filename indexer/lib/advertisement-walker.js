import * as cbor from '@ipld/dag-cbor'

import createDebug from 'debug'
import { varint } from 'multiformats'
import { CID } from 'multiformats/cid'
import * as multihash from 'multiformats/hashes/digest'
import assert from 'node:assert'
import timers from 'node:timers/promises'
import { assertOkResponse } from 'assert-ok-response'
import pRetry from 'p-retry'

/** @import {ProviderInfo, WalkerState} from './typings.js' */
/** @import {RedisRepository as Repository} from '@filecoin-station/spark-piece-indexer-repository' */

const debug = createDebug('spark-piece-indexer:advertisement-walker')

/**
 * @param {object} args
 * @param {Repository} args.repository
 * @param {string} args.providerId
 * @param {(providerId: string) => Promise<ProviderInfo>} args.getProviderInfo
 * @param {number} args.minStepIntervalInMs
 * @param {AbortSignal} [args.signal]
 */
export async function walkChain({
  repository,
  providerId,
  getProviderInfo,
  minStepIntervalInMs,
  signal,
}) {
  let stepInterval = minStepIntervalInMs
  let walkerState

  while (!signal?.aborted) {
    const started = Date.now()
    const providerInfo = await getProviderInfo(providerId)
    let failed = false
    /** @type {WalkerState | undefined} */
    try {
      const result = await walkOneStep({
        repository,
        providerId,
        providerInfo,
        walkerState,
      })
      walkerState = result.walkerState
      debug('Got new walker state for provider %s: %o', providerId, walkerState)
      if (result.finished) break
      failed = !!result.failed
    } catch (err) {
      failed = true
      console.error(
        'Error indexing provider %s (%s):',
        providerId,
        providerInfo.providerAddress,
        err,
      )
    }

    if (failed) {
      // exponential back-off for failing requests
      if (stepInterval < 1_000) stepInterval = 1_000
      else if (stepInterval < 60_000) stepInterval = stepInterval * 2
      else stepInterval = 60_000
    } else {
      stepInterval = minStepIntervalInMs
    }

    const delay = stepInterval - (Date.now() - started)
    if (delay > 0) {
      debug(
        'Waiting for %sms before the next walk for provider %s (%s)',
        delay,
        providerId,
        providerInfo.providerAddress,
      )
      await timers.setTimeout(delay)
    }
  }
}

/**
 * @param {object} args
 * @param {Repository} args.repository
 * @param {string} args.providerId
 * @param {ProviderInfo} args.providerInfo
 * @param {WalkerState} [args.walkerState]
 * @param {number} [args.fetchTimeout]
 */
export async function walkOneStep({
  repository,
  providerId,
  providerInfo,
  fetchTimeout,
  walkerState,
}) {
  if (!walkerState) {
    debug(
      'FETCHING walker state from the repository for provider %s (%s)',
      providerId,
      providerInfo.providerAddress,
    )
    walkerState = await repository.getWalkerState(providerId)
  } else {
    debug(
      'REUSING walker state for provider %s (%s)',
      providerId,
      providerInfo.providerAddress,
    )
  }
  const { newState, indexEntry, failed, finished } =
    await processNextAdvertisement({
      providerId,
      providerInfo,
      walkerState,
      fetchTimeout,
    })

  if (newState) {
    await repository.setWalkerState(providerId, newState)
    walkerState = newState
  }
  if (indexEntry?.pieceCid) {
    await repository.addPiecePayloadBlocks(
      providerId,
      indexEntry.pieceCid,
      indexEntry.payloadCid,
    )
  }
  return { failed, finished, walkerState }
}

/**
 * @param {object} args
 * @param {string} args.providerId
 * @param {ProviderInfo} args.providerInfo
 * @param {WalkerState | undefined} args.walkerState
 * @param {number} [args.fetchTimeout]
 */
export async function processNextAdvertisement({
  providerId,
  providerInfo,
  walkerState,
  fetchTimeout,
}) {
  if (!providerInfo.providerAddress?.match(/^https?:\/\//)) {
    debug(
      'Skipping provider %s - address is not HTTP(s): %s',
      providerId,
      providerInfo.providerAddress,
    )
    return {
      /** @type {WalkerState} */
      newState: {
        status: `Index provider advertises over an unsupported protocol: ${providerInfo.providerAddress}`,
      },
      finished: true,
    }
  }

  const nextHead = providerInfo.lastAdvertisementCID

  /** @type {WalkerState} */
  let state

  if (walkerState?.tail) {
    debug(
      'Next step for provider %s (%s): %s',
      providerId,
      providerInfo.providerAddress,
      walkerState.tail,
    )
    state = { ...walkerState }
  } else if (nextHead === walkerState?.lastHead) {
    debug(
      'No new advertisements from provider %s (%s)',
      providerId,
      providerInfo.providerAddress,
    )
    return { finished: true }
  } else {
    debug(
      'New walk for provider %s (%s): %s',
      providerId,
      providerInfo.providerAddress,
      providerInfo.lastAdvertisementCID,
    )
    state = {
      head: nextHead,
      tail: nextHead,
      lastHead: walkerState?.lastHead,
      status: 'placeholder',
    }
  }

  // TypeScript is not able to infer (yet?) that state.tail is always set by the code above
  assert(state.tail)

  try {
    const { previousAdvertisementCid, entry, error } =
      await fetchAdvertisedPayload(providerInfo.providerAddress, state.tail, {
        fetchTimeout,
      })

    if (
      !previousAdvertisementCid ||
      previousAdvertisementCid === state.lastHead
    ) {
      // We finished the walk
      state.lastHead = state.head
      state.head = undefined
      state.tail = undefined
      state.status = `All advertisements from ${state.lastHead} to the end of the chain were processed.`
    } else {
      // There are more steps in this walk
      state.tail = previousAdvertisementCid
      state.status = `Walking the advertisements from ${state.head}, next step: ${state.tail}`
    }

    if (error === 'ENTRIES_NOT_RETRIEVABLE') {
      state.entriesNotRetrievable = (state.entriesNotRetrievable ?? 0) + 1
    } else if (error === 'MISSING_PIECE_CID') {
      state.adsMissingPieceCID = (state.adsMissingPieceCID ?? 0) + 1
    }

    const indexEntry = entry?.pieceCid ? entry : undefined
    const finished = !state.tail
    return {
      newState: state,
      indexEntry,
      finished,
    }
  } catch (err) {
    const errorDescription = describeFetchError(
      err,
      providerInfo.providerAddress,
    )

    debug(
      'Cannot process provider %s (%s) advertisement %s: %s',
      providerId,
      providerInfo.providerAddress,
      state.tail,
      errorDescription ?? err,
    )
    state.status = `Error processing ${state.tail}: ${errorDescription ?? 'internal error'}`
    return {
      newState: state,
      failed: true,
    }
  }
}

/**
 * @param {unknown} err
 * @param {string} providerAddress
 * @returns {string | undefined}
 */
function describeFetchError(err, providerAddress) {
  if (!(err instanceof Error)) return undefined

  let reason
  if ('serverMessage' in err && err.serverMessage) {
    reason = err.serverMessage
    if ('statusCode' in err && err.statusCode) {
      reason = `${err.statusCode} ${reason}`
    }
  } else if ('statusCode' in err && err.statusCode) {
    reason = err.statusCode
  } else if (err.name === 'TimeoutError') {
    reason = 'operation timed out'
  } else if (
    err.name === 'TypeError' &&
    err.message === 'fetch failed' &&
    err.cause &&
    err.cause instanceof Error
  ) {
    reason = err.cause.message
  }
  if (!reason) return undefined

  const url = 'url' in err ? err.url : providerAddress
  reason = `HTTP request to ${url} failed: ${reason}`
  return reason
}

/**
 * @param {string} providerAddress
 * @param {string} advertisementCid
 * @param {object} [options]
 * @param {number} [options.fetchTimeout]
 */
export async function fetchAdvertisedPayload(
  providerAddress,
  advertisementCid,
  { fetchTimeout } = {},
) {
  const advertisement = /**
   * @type {{
   *   Addresses: string[]
   *   ContextID: { '/': { bytes: string } }
   *   Entries: { '/': string }
   *   IsRm: false
   *   Metadata: { '/': { bytes: string } }
   *   PreviousID?: { '/': string }
   *   Provider: string
   *   Signature: {
   *     '/': {
   *       bytes: string
   *     }
   *   }
   * }}
   */ (
    await pRetry(() =>
      fetchCid(providerAddress, advertisementCid, { fetchTimeout }),
    )
  )
  const previousAdvertisementCid = advertisement.PreviousID?.['/']
  debug('advertisement %s %j', advertisementCid, advertisement)

  const entriesCid = advertisement.Entries?.['/']
  if (!entriesCid || entriesCid === 'bafkreehdwdcefgh4dqkjv67uzcmw7oje') {
    // An empty advertisement with no entries
    // See https://github.com/ipni/ipni-cli/blob/512ef8294eb717027b72e572897fbd8a1ed74564/pkg/adpub/client_store.go#L46-L48
    // https://github.com/ipni/go-libipni/blob/489479457101ffe3cbe80682570b63c12ba2546d/ingest/schema/schema.go#L65-L71
    debug(
      'advertisement %s has no entries: %j',
      advertisementCid,
      advertisement.Entries,
    )
    return { previousAdvertisementCid }
  }

  let pieceCid = extractPieceInfoFromContextID(
    advertisement.ContextID,
  )?.pieceCid?.toString()
  let meta
  if (!pieceCid) {
    meta = parseMetadata(advertisement.Metadata['/'].bytes)
    pieceCid = meta.deal?.PieceCID.toString()
  }

  if (!pieceCid) {
    debug(
      'advertisement %s has no PieceCID in ContextId: %j or metadata: %j',
      advertisementCid,
      advertisement.ContextID,
      meta?.deal,
    )
    return {
      error: /** @type {const} */ ('MISSING_PIECE_CID'),
      previousAdvertisementCid,
    }
  }

  let entriesChunk
  try {
    entriesChunk = await pRetry(
      () =>
        /**
         * @type {Promise<{
         *   Entries: { '/': { bytes: string } }[]
         * }>}
         */ (fetchCid(providerAddress, entriesCid, { fetchTimeout })),
      {
        shouldRetry: ({ error }) =>
          error &&
          'statusCode' in error &&
          typeof error.statusCode === 'number' &&
          error.statusCode >= 500,
      },
    )
  } catch (err) {
    // We are not able to fetch the advertised entries. Skip this advertisement so that we can
    // continue the ingestion of other advertisements.
    const errorDescription = describeFetchError(err, providerAddress)
    const log =
      /** @type {any} */ (err)?.statusCode === 404 ? debug : console.warn
    log(
      'Cannot fetch ad %s entries %s: %s',
      advertisementCid,
      entriesCid,
      errorDescription ?? err,
    )
    return {
      error: /** @type {const} */ ('ENTRIES_NOT_RETRIEVABLE'),
      previousAdvertisementCid,
    }
  }

  let payloadCid
  try {
    payloadCid = processEntries(entriesCid, entriesChunk)
  } catch (err) {
    debug('Error processing entries: %s', err)
    return {
      error: /** @type {const} */ ('PAYLOAD_CID_NOT_EXTRACTABLE'),
      previousAdvertisementCid,
    }
  }

  return {
    previousAdvertisementCid,
    entry: { pieceCid, payloadCid },
  }
}

/**
 * @param {string} providerBaseUrl
 * @param {string} cid
 * @param {object} [options]
 * @param {number} [options.fetchTimeout]
 * @param {typeof fetch} [options.fetchFn]
 * @returns {Promise<unknown>}
 */
export async function fetchCid(
  providerBaseUrl,
  cid,
  { fetchTimeout, fetchFn } = {},
) {
  let url = new URL(providerBaseUrl)

  // Check if the URL already has a path
  if (!(url.pathname && url.pathname !== '/')) {
    // If no path, add the standard path with a placeholder
    url = new URL('/ipni/v1/ad/_cid_placeholder_', providerBaseUrl)
  } else {
    // If there's already a path, append the additional path
    url = new URL(`${url.pathname}/ipni/v1/ad/_cid_placeholder_`, url.origin)
  }
  url = new URL(cid, url)
  debug('Fetching %s', url)
  try {
    const res = await (fetchFn ?? fetch)(url, {
      signal: AbortSignal.timeout(fetchTimeout ?? 30_000),
    })
    debug('Response from %s → %s %o', url, res.status, res.headers)
    await assertOkResponse(res)

    // Determine the codec based on the CID
    const parsedCid = CID.parse(cid)
    const codec = parsedCid.code

    // List of codecs:
    // https://github.com/multiformats/multicodec/blob/f18c7ba5f4d3cc74afb51ee79978939abc0e6556/table.csv
    switch (codec) {
      case 297: // DAG-JSON
        return await res.json()

      case 113: {
        // DAG-CBOR
        const buffer = await res.arrayBuffer()
        return cbor.decode(new Uint8Array(buffer))
      }

      default:
        throw new Error(`Unknown codec ${codec} for CID ${cid}`)
    }
  } catch (err) {
    if (err && typeof err === 'object') {
      Object.assign(err, { url })
    }
    debug('Error from %s ->', url, err)
    throw err
  }
}

/** @param {string} meta */
export function parseMetadata(meta) {
  const bytes = Buffer.from(meta, 'base64')
  const [protocolCode, nextOffset] = varint.decode(bytes)

  const protocol =
    {
      0x900: 'bitswap',
      0x910: 'graphsync',
      0x0920: 'http',
    }[protocolCode] ?? '0x' + protocolCode.toString(16)

  if (protocol === 'graphsync') {
    // console.log(bytes.subarray(nextOffset).toString('hex'))
    /**
     * @type {{
     *   PieceCID: import('multiformats/cid').CID
     *   VerifiedDeal: boolean
     *   FastRetrieval: boolean
     * }}
     */
    const deal = cbor.decode(bytes.subarray(nextOffset))
    return { protocol, deal }
  } else {
    return { protocol }
  }
}

/**
 * Attempts to extract PieceCID and PieceSize from a ContextID
 *
 * @param {{ '/': { bytes: string } } | undefined | null} contextID - The
 *   ContextID object from an advertisement
 * @param {object} [options]
 * @param {function} [options.logDebugMessage] - Function to log debug messages
 * @returns {{ pieceCid: string; pieceSize: number } | null} - Object containing
 *   pieceCid and pieceSize if successful, null otherwise
 */
export function extractPieceInfoFromContextID(
  contextID,
  { logDebugMessage } = {},
) {
  logDebugMessage ??= debug
  // Check if ContextID exists and has the expected structure
  if (!contextID || !contextID['/'] || !contextID['/'].bytes) {
    logDebugMessage(
      'Advertisement %s has no properly formatted ContextID',
      contextID,
    )
    return null
  }

  try {
    // Get the bytes from the ContextID
    const contextIDBytes = contextID['/'].bytes

    const bytes = Buffer.from(contextIDBytes, 'base64')
    const decoded = cbor.decode(bytes)

    // Validate the decoded data with specific error messages
    if (!Array.isArray(decoded)) {
      logDebugMessage(
        'ContextID validation failed for %s: decoded value is not an array, got %s',
        contextID,
        typeof decoded,
      )
      return null
    }

    if (decoded.length !== 2) {
      logDebugMessage(
        'ContextID validation failed for %s: expected array with 2 items, got %d items',
        contextID,
        decoded.length,
      )
      return null
    }
    const [pieceSize, pieceCid] = decoded
    if (typeof pieceSize !== 'number') {
      logDebugMessage(
        'ContextID validation failed for %s: pieceSize is not a number, got %s',
        contextID,
        typeof decoded[0],
      )
      return null
    }
    if (!(typeof pieceCid === 'object')) {
      logDebugMessage(
        'ContextID validation failed for %s: pieceCID is not an object, got %s',
        contextID,
        typeof pieceCid,
      )
      return null
    }
    if (pieceCid === null || pieceCid === undefined) {
      logDebugMessage(
        'ContextID validation failed for %s: pieceCID is null or undefined',
        contextID,
      )
      return null
    }
    if (!(pieceCid?.constructor?.name === 'CID')) {
      logDebugMessage(
        'ContextID validation failed for %s: pieceCID is not a CID, got %s',
        contextID,
        pieceCid?.constructor?.name,
      )
      return null
    }

    return { pieceCid, pieceSize }
  } catch (err) {
    logDebugMessage(
      'Failed to decode ContextID for advertisement %s: %s',
      contextID,
      err,
    )
    return null
  }
}

/**
 * Process entries from either DAG-JSON or DAG-CBOR format
 *
 * @param {string} entriesCid - The CID of the entries
 * @param {{ Entries: unknown[] }} entriesChunk - The decoded entries
 * @returns {string} The payload CID
 */
export function processEntries(entriesCid, entriesChunk) {
  if (!entriesChunk.Entries || !entriesChunk.Entries.length) {
    throw new Error(`No entries found in the response for ${entriesCid}`)
  }
  const parsedCid = CID.parse(entriesCid)
  const codec = parsedCid.code
  let entryBytes
  switch (codec) {
    case 297: {
      // DAG-JSON
      // For DAG-JSON format, the entry is a base64 encoded string
      const entry = entriesChunk.Entries[0]
      // Check that entry is an object with a '/' property
      if (!entry || typeof entry !== 'object' || !('/' in entry)) {
        throw new Error('DAG-JSON entry must have a "/" property')
      }

      // Verify the '/' property is an object with 'bytes' property
      // In DAG-JSON, CIDs are represented as objects with a '/' property that contains 'bytes'
      if (
        !entry['/'] ||
        typeof entry['/'] !== 'object' ||
        !('bytes' in entry['/'])
      ) {
        throw new Error(
          'DAG-JSON entry\'s "/" property must be an object with a "bytes" property',
        )
      }

      const entryHash = entry['/'].bytes
      if (typeof entryHash !== 'string') {
        throw new Error(
          'DAG-JSON entry\'s ["/"]["bytes"] property must be a string',
        )
      }
      entryBytes = Buffer.from(entryHash, 'base64')
      break
    }
    case 113: {
      // DAG-CBOR
      // For DAG-CBOR format, the entry is already a Uint8Array with the multihash
      entryBytes = entriesChunk.Entries[0]
      assert(
        entryBytes instanceof Uint8Array,
        'DAG-CBOR entry must be a Uint8Array',
      )
      break
    }
    default:
      throw new Error(`Unsupported codec ${codec}`)
  }
  assert(entryBytes, 'Entry bytes must be set')
  return CID.create(1, 0x55 /* raw */, multihash.decode(entryBytes)).toString()
}
