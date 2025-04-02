import * as Sentry from '@sentry/node'
import fs from 'node:fs/promises'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const { SENTRY_ENVIRONMENT = 'development' } = process.env

const pkg = JSON.parse(
  await fs.readFile(
    join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'package.json'),
    'utf8',
  ),
)

Sentry.init({
  dsn: 'https://01b72a603b1bd4f9f821348152486dc2@o1408530.ingest.us.sentry.io/4508002559393792',
  release: pkg.version,
  environment: SENTRY_ENVIRONMENT,
  tracesSampleRate: 0.1,
  // Ignore Fastify 4xx errors
  // Remove once https://github.com/getsentry/sentry-javascript/pull/13198 lands
  beforeSend(event, { originalException: err }) {
    const isBadRequest =
      typeof err === 'object' &&
      err !== null &&
      'statusCode' in err &&
      typeof err.statusCode === 'number' &&
      err.statusCode < 500
    return isBadRequest ? null : event
  },
})
