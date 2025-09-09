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

const createMutex = () => {
  let tail = Promise.resolve()
  const runExclusive = async <T>(fn: () => T | Promise<T>): Promise<T> => {
    let release: () => void = () => {}
    const p = new Promise<void>((resolve) => {
      release = resolve
    })
    const prev = tail
    tail = prev.then(() => p)
    await prev
    try {
      return await fn()
    } finally {
      release()
    }
  }
  return { runExclusive }
}

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

  const heightQueue: number[] = Array.from(
    { length: Math.max(end - scanStart + 1, 0) },
    (_, i) => scanStart + i,
  )
  const getNextHeight = async (): Promise<number | null> =>
    heightQueue.length > 0 ? heightQueue.shift()! : null

  let nextToCommit = scanStart
  const completedHeights = new Set<number>()
  const commitMutex = createMutex()
  const markCompleted = async (height: number): Promise<void> =>
    commitMutex.runExclusive(() => {
      completedHeights.add(height)
      while (completedHeights.has(nextToCommit)) {
        completedHeights.delete(nextToCommit)
        setLastProcessedBlockHeight(db, chain, nextToCommit)
        nextToCommit += 1
      }
    })

  const worker = async () => {
    while (true) {
      const h = await getNextHeight()
      if (h == null) return
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

          await markCompleted(h)
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

  await Promise.all(Array.from({ length: blockConcurrency }, () => worker()))
  console.log(`${logPrefix} capture complete`)
}
