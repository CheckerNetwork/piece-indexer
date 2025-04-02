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
  dsn: 'https://063b6300f825d76ef0013cfbfdbde270@o1408530.ingest.us.sentry.io/4508002543206400',
  release: pkg.version,
  environment: SENTRY_ENVIRONMENT,
  tracesSampleRate: 0.1,
})
