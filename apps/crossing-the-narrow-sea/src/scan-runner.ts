import { getApi, getEventsAt } from './chain'
import { openDb, getLastProcessedBlockHeight, setLastProcessedBlockHeight } from './sqlite'
import { processXdmEvents } from './event-utils'

export interface RunScanOptions {
  rpcEndpoints: string[]
  dbPath: string
  chain: 'domain' | 'consensus'
  logPrefix: string
  start: number
  end: number
  blockConcurrency: number
  retryBackoffMs: number
  retryMaxBackoffMs: number
  useSegments: boolean
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

export const runScan = async (opts: RunScanOptions): Promise<void> => {
  const {
    rpcEndpoints,
    dbPath,
    chain,
    logPrefix,
    start,
    end,
    blockConcurrency,
    retryBackoffMs,
    retryMaxBackoffMs,
    useSegments,
  } = opts

  const api = await getApi(rpcEndpoints)
  const db = openDb(dbPath)

  const resumeFrom = getLastProcessedBlockHeight(db, chain)
  const scanStart = Math.max(start, resumeFrom ?? start)
  const total = end - scanStart + 1
  console.log(
    `${logPrefix} capture start: heights ${scanStart}..${end} (total ${Math.max(total, 0)})`,
  )

  // Lock-free work distribution: worker i handles scanStart + i, then + blockConcurrency, etc.
  const totalWork = Math.max(end - scanStart + 1, 0)
  const processedFlags: boolean[] = Array.from({ length: totalWork }, () => false)
  let nextToCommit = scanStart

  const advanceCommit = (): void => {
    while (nextToCommit <= end) {
      const idx = nextToCommit - scanStart
      if (!processedFlags[idx]) break
      setLastProcessedBlockHeight(db, chain, nextToCommit)
      nextToCommit += 1
    }
  }

  const worker = async (workerId: number) => {
    for (let h = scanStart + workerId; h <= end; h += blockConcurrency) {
      let backoff = retryBackoffMs
      while (true) {
        try {
          const hash = await api.rpc.chain.getBlockHash(h)
          const block = await api.rpc.chain.getBlock(hash)
          const events: any[] = (await getEventsAt(api, hash, useSegments)) as any
          const extrinsics = block.block.extrinsics
          if (h % 100 === 0) {
            console.log(`${logPrefix} processing #${h}`)
          }

          processXdmEvents({
            db,
            chain,
            events,
            extrinsics,
            blockHeight: h,
            blockHash: hash.toString(),
            logPrefix,
          })

          processedFlags[h - scanStart] = true
          advanceCommit()
          break
        } catch (err) {
          const msg = (err as Error)?.message || String(err)
          console.warn(`${logPrefix} error at #${h}: ${msg}. retrying in ${backoff}ms`)
          await sleep(backoff)
          backoff = Math.min(backoff * 2, retryMaxBackoffMs)
        }
      }
    }
  }

  await Promise.all(Array.from({ length: blockConcurrency }, (_, workerId) => worker(workerId)))

  console.log(`${logPrefix} capture complete`)
}
