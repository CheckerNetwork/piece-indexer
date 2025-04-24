import { Redis } from 'ioredis'
import assert from 'node:assert'
import { after, before, beforeEach, describe, it } from 'node:test'
import { RedisRepository } from '../lib/redis-repository.js'

/** @import {WalkerState} from '../lib/typings.d.ts' */

describe('data schema for REST API', () => {
  /** @type {Redis} */
  let redis
  /** @type {RedisRepository} */
  let repository

  before(async () => {
    redis = new Redis({ db: 1 })
    repository = new RedisRepository(redis)
  })

  beforeEach(async () => {
    await redis.flushall()
  })

  after(async () => {
    await redis?.disconnect()
  })

  it('persists WalkerState', async () => {
    /** @type {WalkerState} */
    const state = {
      head: 'head',
      tail: 'tail',
      lastHead: 'last head',
      status: 'status',
    }

    await repository.setWalkerState('providerId', state)
    const loaded = await repository.getWalkerState('providerId')
    assert.deepStrictEqual(loaded, state)
  })
})
