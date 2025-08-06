import assert from 'assert'
import { RedisRepository } from '@filecoin-station/spark-piece-indexer-repository'
import { Redis } from 'ioredis'
import { walkChain } from '../lib/advertisement-walker.js'
import { runIpniSync } from '../lib/ipni-watcher.js'

/** @import {ProviderToInfoMap} from '../lib/typings.d.ts' */

const { REDIS_URL: redisUrl = 'redis://localhost:6379' } = process.env

const redisUrlParsed = new URL(redisUrl)
const redis = new Redis({
  host: redisUrlParsed.hostname,
  port: Number(redisUrlParsed.port),
  username: redisUrlParsed.username,
  password: redisUrlParsed.password,
  lazyConnect: true, // call connect() explicitly so that we can exit on connection error
  family: 6, // required for upstash
})

await redis.connect()
const repository = new RedisRepository(redis)

/** @type {Set<string>} */
const providerIdsBeingWalked = new Set()

/** @type {ProviderToInfoMap} */
const recentProvidersInfo = new Map()

/** @param {string} providerId */
const getProviderInfo = async (providerId) => {
  const info = recentProvidersInfo.get(providerId)
  assert(info, `Unknown providerId ${providerId}`)
  return info
}

for await (const providerInfos of runIpniSync({
  minSyncIntervalInMs: 60_000,
})) {
  for (const [providerId, providerInfo] of providerInfos.entries()) {
    recentProvidersInfo.set(providerId, providerInfo)
    if (providerIdsBeingWalked.has(providerId)) continue

    providerIdsBeingWalked.add(providerId)
    walkChain({
      repository,
      providerId,
      getProviderInfo,
      minStepIntervalInMs: 100,
    }).finally(() => providerIdsBeingWalked.delete(providerId))
  }
}
