import 'dotenv/config'
import { getApi, getEventsAt } from './chain'
import { openDb, getLastProcessedBlockHeight, setLastProcessedBlockHeight } from './sqlite'
import { processXdmEvents } from './event-utils'

const DOMAIN_RPC_URL = process.env.DOMAIN_RPC_URL as string
const rpcEndpoints = DOMAIN_RPC_URL.split(',')
const START = Number(process.env.DOMAIN_START_HEIGHT)
const END = Number(process.env.DOMAIN_END_HEIGHT)
const OUTPUT_DIR = process.env.OUTPUT_DIR || 'exports'
const DB_PATH = `${OUTPUT_DIR}/xdm.sqlite`
const LOG_EVERY = Number(process.env.LOG_EVERY || 1000)
const RETRY_BACKOFF_MS = Number(process.env.RPC_BACKOFF_MS || 1000)
const RETRY_MAX_BACKOFF_MS = Number(process.env.RPC_MAX_BACKOFF_MS || 10000)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const main = async () => {
  if (!rpcEndpoints || !START || !END) {
    throw new Error('DOMAIN_RPC_URL, DOMAIN_START_HEIGHT, DOMAIN_END_HEIGHT are required')
  }

  const api = await getApi(rpcEndpoints)
  const db = openDb(DB_PATH)
  const resumeFrom = getLastProcessedBlockHeight(db, 'domain')
  const scanStart = Math.max(START, resumeFrom ?? START)
  const total = END - scanStart + 1
  console.log(`[domain] capture start: heights ${scanStart}..${END} (total ${Math.max(total, 0)})`)

  let backoff = RETRY_BACKOFF_MS
  for (let h = scanStart; h <= END; ) {
    try {
      const hash = await api.rpc.chain.getBlockHash(h)
      const block = await api.rpc.chain.getBlock(hash)
      // Prefer segmented events on domain when available
      const events: any[] = (await getEventsAt(api, hash, true)) as any
      const extrinsics = block.block.extrinsics
      if (h % 100 === 0) {
        console.log(`[domain] processing #${h}`)
        setLastProcessedBlockHeight(db, 'domain', h)
      }

      processXdmEvents({
        db,
        chain: 'domain',
        events,
        extrinsics,
        blockHeight: h,
        blockHash: hash.toString(),
        logPrefix: '[domain]',
      })

      const processed = h - scanStart + 1
      if (processed % LOG_EVERY === 0 || h === END) {
        const pct = Math.floor((processed * 100) / total)
        console.log(`[domain] processed ${processed}/${total} (${pct}%) at #${h}`)
      }
      h += 1
      backoff = RETRY_BACKOFF_MS
    } catch (err) {
      const msg = (err as Error)?.message || String(err)
      console.warn(`[domain] error at #${h}: ${msg}. retrying in ${backoff}ms`)
      await sleep(backoff)
      backoff = Math.min(backoff * 2, RETRY_MAX_BACKOFF_MS)
      // retry same height
    }
  }

  console.log('[domain] capture complete')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
