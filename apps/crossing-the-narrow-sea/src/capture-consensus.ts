import 'dotenv/config'
import { getApi } from './chain'
import { openDb, getLastProcessedBlockHeight, setLastProcessedBlockHeight } from './sqlite'
import { processXdmEvents } from './event-utils'

const CONSENSUS_RPC_URL = process.env.CONSENSUS_RPC_URL as string
const rpcEndpoints = CONSENSUS_RPC_URL.split(',')
const START = Number(process.env.CONSENSUS_START_HEIGHT)
const END = Number(process.env.CONSENSUS_END_HEIGHT)
const OUTPUT_DIR = process.env.OUTPUT_DIR || 'exports'
const DB_PATH = `${OUTPUT_DIR}/xdm.sqlite`
const RETRY_BACKOFF_MS = Number(process.env.RPC_BACKOFF_MS || 1000)
const RETRY_MAX_BACKOFF_MS = Number(process.env.RPC_MAX_BACKOFF_MS || 10000)
const BLOCK_CONCURRENCY = Math.max(
  1,
  Number(process.env.BLOCK_CONCURRENCY || process.env.RPC_CONCURRENCY || 8),
)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const main = async () => {
  if (!rpcEndpoints || !START || !END) {
    throw new Error('CONSENSUS_RPC_URL, CONSENSUS_START_HEIGHT, CONSENSUS_END_HEIGHT are required')
  }

  const api = await getApi(rpcEndpoints)
  const db = openDb(DB_PATH)
  const resumeFrom = getLastProcessedBlockHeight(db, 'consensus')
  const scanStart = Math.max(START, resumeFrom ?? START)
  const total = END - scanStart + 1
  console.log(
    `[consensus] capture start: heights ${scanStart}..${END} (total ${Math.max(total, 0)})`,
  )

  let nextHeight = scanStart

  const getNextHeight = (): number | null => {
    if (nextHeight > END) return null
    const h = nextHeight
    nextHeight += 1
    return h
  }

  const worker = async () => {
    while (true) {
      const h = getNextHeight()
      if (h == null) return
      let backoff = RETRY_BACKOFF_MS
      while (true) {
        try {
          const hash = await api.rpc.chain.getBlockHash(h)
          const block = await api.rpc.chain.getBlock(hash)
          const events: any[] = (await api.query.system.events.at(hash)) as any
          const extrinsics = block.block.extrinsics
          if (h % 100 === 0) {
            console.log(`[consensus] processing #${h}`)
            setLastProcessedBlockHeight(db, 'consensus', h)
          }

          processXdmEvents({
            db,
            chain: 'consensus',
            events,
            extrinsics,
            blockHeight: h,
            blockHash: hash.toString(),
            logPrefix: '[consensus]',
          })

          break
        } catch (err) {
          const msg = (err as Error)?.message || String(err)
          console.warn(`[consensus] error at #${h}: ${msg}. retrying in ${backoff}ms`)
          await sleep(backoff)
          backoff = Math.min(backoff * 2, RETRY_MAX_BACKOFF_MS)
        }
      }
    }
  }

  await Promise.all(Array.from({ length: BLOCK_CONCURRENCY }, () => worker()))
  console.log('[consensus] capture complete')
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
