import '../lib/instrument.js'

import { createApp } from '../lib/app.js'
import { RedisRepository } from '@filecoin-station/spark-piece-indexer-repository'
import { Redis } from 'ioredis'

const {
  PORT = 3000,
  HOST = '127.0.0.1',
  DOMAIN: domain = 'localhost',
  REDIS_URL: redisUrl = 'redis://localhost:6379',
  REQUEST_LOGGING: requestLogging = 'true'
} = process.env

const redisUrlParsed = new URL(redisUrl)
const redis = new Redis({
  host: redisUrlParsed.hostname,
  port: Number(redisUrlParsed.port),
  username: redisUrlParsed.username,
  password: redisUrlParsed.password,
  lazyConnect: true, // call connect() explicitly so that we can exit on connection error
  family: 6 // required for upstash
})

await redis.connect()
const repository = new RedisRepository(redis)

const app = createApp({
  repository,
  domain,
  logger: {
    level: ['1', 'true'].includes(requestLogging) ? 'info' : 'error'
  }
})
console.log('Starting the http server on host %j port %s', HOST, PORT)
const baseUrl = await app.listen({ host: HOST, port: Number(PORT) })
console.log(baseUrl)
