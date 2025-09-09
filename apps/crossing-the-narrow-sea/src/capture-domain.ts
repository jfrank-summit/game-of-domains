import 'dotenv/config'
import { runScan } from './scan-runner'

const DOMAIN_RPC_URL = process.env.DOMAIN_RPC_URL as string
const rpcEndpoints = DOMAIN_RPC_URL.split(',')
const START = Number(process.env.DOMAIN_START_HEIGHT)
const END = Number(process.env.DOMAIN_END_HEIGHT)
const OUTPUT_DIR = process.env.OUTPUT_DIR || 'exports'
const DB_PATH = `${OUTPUT_DIR}/xdm.sqlite`
const RETRY_BACKOFF_MS = Number(process.env.RPC_BACKOFF_MS || 1000)
const RETRY_MAX_BACKOFF_MS = Number(process.env.RPC_MAX_BACKOFF_MS || 10000)
const BLOCK_CONCURRENCY = Math.max(1, Number(process.env.BLOCK_CONCURRENCY || 8))

const main = async () => {
  if (!rpcEndpoints || !START || !END) {
    throw new Error('DOMAIN_RPC_URL, DOMAIN_START_HEIGHT, DOMAIN_END_HEIGHT are required')
  }
  await runScan({
    rpcEndpoints,
    dbPath: DB_PATH,
    chain: 'domain',
    logPrefix: '[domain]',
    start: START,
    end: END,
    blockConcurrency: BLOCK_CONCURRENCY,
    retryBackoffMs: RETRY_BACKOFF_MS,
    retryMaxBackoffMs: RETRY_MAX_BACKOFF_MS,
    useSegments: true,
  })
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
