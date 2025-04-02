/** @import { WalkerState } from './typings.d.ts' */

export class RedisRepository {
  #redis

  /**
   * @param {import('ioredis').Redis} redis
   */
  constructor(redis) {
    this.#redis = redis
  }

  /**
   * @param {string} providerId
   * @returns {Promise<WalkerState>}
   */
  async getWalkerState(providerId) {
    const json = await this.#redis.get(`walker-state:${providerId}`)
    return json ? JSON.parse(json) : undefined
  }

  /**
   * @param {string} providerId
   * @param {WalkerState} state
   */
  async setWalkerState(providerId, state) {
    const data = JSON.stringify(state)
    await this.#redis.set(`walker-state:${providerId}`, data)
  }

  /**
   * @param {string} providerId
   * @param {string} pieceCid
   * @param {string[]} payloadCids
   */
  async addPiecePayloadBlocks(providerId, pieceCid, ...payloadCids) {
    await this.#redis.sadd(`piece-payload:${providerId}:${pieceCid}`, ...payloadCids)
  }

  /**
   * @param {string} providerId
   * @param {string} pieceCid
   * @returns {Promise<string[]>}
   */
  async getPiecePayloadBlocks(providerId, pieceCid) {
    const payloadCids = await this.#redis.smembers(`piece-payload:${providerId}:${pieceCid}`)
    return payloadCids
  }

  /**
   * @param {string} providerId
   */
  async countPiecesIndexed(providerId) {
    const keyStream = this.#redis.scanStream({
      match: `piece-payload:${providerId}:*`,
      count: 64_000,
    })

    // We need to de-duplicate the keys returned by Redis.
    // See https://redis.io/docs/latest/commands/scan/
    // > A given element may be returned multiple times. It is up to the application to handle the
    // > case of duplicated elements, for example only using the returned elements in order to perform
    // > operations that are safe when re-applied multiple times.
    /** @type {Set<string>} */
    const uniquePieces = new Set()
    for await (const chunk of keyStream) {
      for (const key of chunk) {
        uniquePieces.add(key)
      }
    }

    return uniquePieces.size
  }
}
