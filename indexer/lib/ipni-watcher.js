import * as Sentry from '@sentry/node'
import createDebug from 'debug'
import timers from 'node:timers/promises'
import { assertOkResponse } from 'assert-ok-response'
import { multiaddrToHttpUrl } from './vendored/multiaddr.js'

const debug = createDebug('spark-piece-indexer:ipni-watcher')

/** @import {ProviderToInfoMap, ProviderInfo} from './typings.js' */
/** @import {IpniCidQueryResponse} from './ipni-api.js' */

/**
 * @param {object} args
 * @param {number} args.minSyncIntervalInMs
 * @param {AbortSignal} [args.signal]
 */
export async function* runIpniSync({ minSyncIntervalInMs, signal }) {
  while (!signal?.aborted) {
    const started = Date.now()
    try {
      console.log('Syncing from IPNI')
      const providers = await getProvidersWithMetadata()
      console.log(
        'Found %s providers, %s support(s) HTTP(s)',
        providers.size,
        Array.from(providers.values()).filter((p) =>
          p.providerAddress.match(/^https?:\/\//),
        ).length,
      )
      yield providers
    } catch (err) {
      console.error('Cannot sync from IPNI.', err)
      Sentry.captureException(err)
    }
    const delay = minSyncIntervalInMs - (Date.now() - started)
    if (delay > 0) {
      console.log('Waiting for %sms before the next sync from IPNI', delay)
      await timers.setTimeout(delay)
    }
  }
}

/** @returns {Promise<ProviderToInfoMap>} */
export async function getProvidersWithMetadata() {
  const res = await fetch('https://cid.contact/providers')
  assertOkResponse(res)

  const providers = /** @type {IpniCidQueryResponse} */ (await res.json())

  /** @type {[string, ProviderInfo][]} */
  const entries = providers.map((p) => {
    const providerId = p.Publisher.ID
    const lastAdvertisementCID = p.LastAdvertisement['/']

    // FIXME: handle empty Addrs[]
    let providerAddress = p.Publisher.Addrs[0]
    try {
      providerAddress = multiaddrToHttpUrl(providerAddress)
    } catch (err) {
      debug(
        'Cannot convert address to HTTP(s) URL (provider: %s): %s',
        providerId,
        err,
      )
    }

    return [providerId, { providerAddress, lastAdvertisementCID }]
  })
  return new Map(entries)
}
