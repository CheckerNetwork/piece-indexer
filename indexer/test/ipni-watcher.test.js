import createDebug from 'debug'
import assert from 'node:assert'
import { describe, it } from 'node:test'
import { getProvidersWithMetadata } from '../lib/ipni-watcher.js'
// import { FRISBII_ADDRESS, FRISBII_ID } from './helpers/test-data.js'

const debug = createDebug('test')

describe('getProvidersWithMetadata', () => {
  it('returns response including known providers', async () => {
    const providers = await getProvidersWithMetadata()
    debug(JSON.stringify(providers, null, 2))

    // FIXME: Use FRISBII_ID once cid.contact's service is restored
    const frisbiiOnFly = providers.get(
      '12D3KooWGzSJjWq2rgd8FH6jyLX6vUQiTTKnpTWwuXJvd9E1dEyH',
    )

    assert(frisbiiOnFly)
    // FIXME: Use FRISBII_ADDRESS once cid.contact's service is restored
    assert.strictEqual(
      frisbiiOnFly.providerAddress,
      'http://113.23.169.21:13105',
    )
    assert.match(frisbiiOnFly.lastAdvertisementCID, /^bagu/)
  })
})
