import { ApiPromise, WsProvider } from '@polkadot/api'

let apiInstance: ApiPromise | null = null
let providerInstance: WsProvider | null = null
let isReconnecting = false

const getOrCreateProvider = (rpcEndpoints: string[]): WsProvider => {
  if (providerInstance) return providerInstance

  const provider = new WsProvider(rpcEndpoints)
  providerInstance = provider

  provider.on('connected', () => {
    console.info({ endpoints: rpcEndpoints }, 'chain connected')
    console.info({ connectedEndpoint: provider.endpoint }, 'connected endpoint')
  })

  provider.on('disconnected', () => {
    console.warn('chain disconnected')
    // Do not tear down; allow WsProvider to auto-rotate/retry
    void triggerReconnect()
  })

  provider.on('error', (err) => {
    console.error({ err }, 'chain provider error')
    // Do not tear down; allow WsProvider to auto-rotate/retry
    void triggerReconnect()
  })

  return providerInstance
}

const triggerReconnect = async (): Promise<void> => {
  if (!apiInstance || isReconnecting) return
  if (apiInstance.isConnected) return
  isReconnecting = true
  try {
    console.warn('chain api disconnected; attempting reconnect')
    await apiInstance.connect()
    await apiInstance.isReadyOrError
    console.info('chain api reconnected')
  } catch (err) {
    console.warn({ err }, 'chain api reconnect attempt failed; provider will keep retrying')
  } finally {
    isReconnecting = false
  }
}

export const getApi = async (rpcEndpoints: string[]): Promise<ApiPromise> => {
  if (apiInstance) {
    if (apiInstance.isConnected) return apiInstance
    await triggerReconnect()
    await apiInstance.isReadyOrError
    return apiInstance
  }

  const provider = getOrCreateProvider(rpcEndpoints)

  if (!apiInstance) {
    apiInstance = await ApiPromise.create({ provider })

    apiInstance.on('error', (err) => {
      console.error({ err }, 'chain api error')
      // Keep provider alive; ApiPromise will recover when provider reconnects
    })

    apiInstance.on('connected', () => {
      console.info('chain api connected')
    })

    apiInstance.on('disconnected', () => {
      console.warn('chain api disconnected')
      void triggerReconnect()
    })

    await apiInstance.isReadyOrError
    console.info('chain api ready')
  }

  return apiInstance
}

export const disconnectApi = async (): Promise<void> => {
  if (apiInstance) {
    try {
      await apiInstance.disconnect()
    } catch (e) {
      console.warn({ err: e }, 'error disconnecting ApiPromise')
    } finally {
      apiInstance = null
    }
  }

  if (providerInstance) {
    try {
      await providerInstance.disconnect()
    } catch (e) {
      console.warn({ err: e }, 'error disconnecting WsProvider')
    } finally {
      providerInstance = null
    }
  }
}

export const getFinalizedHeadAndHeader = async (api: ApiPromise) => {
  const head = await api.rpc.chain.getFinalizedHead()
  const header = await api.rpc.chain.getHeader(head)
  return { head, header }
}

export const getEventsAt = async (api: ApiPromise, hash: any, useSegments: boolean) => {
  if (useSegments && (api.query as any)?.system?.eventSegments) {
    try {
      const SEGMENT_SIZE = 100
      const SEGMENT_CONCURRENCY = Math.max(1, Number(process.env.SEGMENT_CONCURRENCY || 8))
      // Prefer using eventsCount if available to determine number of segments
      let numSegments: number | null = null
      if ((api.query as any)?.system?.eventsCount?.at) {
        const countAny = await (api.query as any).system.eventsCount.at(hash)
        const count = countAny?.toNumber ? countAny.toNumber() : Number(countAny?.toString?.() || 0)
        if (Number.isFinite(count) && count > 0) {
          numSegments = Math.ceil(count / SEGMENT_SIZE)
        }
      }

      const flattened: any[] = []

      const unwrapSegment = (seg: any): any[] => {
        // Option<Vec<EventRecord>> or Vec<EventRecord>
        // @ts-ignore runtime unwrap checks
        if (seg?.isSome !== undefined && typeof seg.unwrap === 'function') {
          // @ts-ignore
          if (seg.isNone) return []
          // @ts-ignore
          const unwrapped = seg.unwrap()
          // @ts-ignore
          return unwrapped?.toArray ? unwrapped.toArray() : []
        }
        // @ts-ignore
        if (seg?.toArray) return seg.toArray()
        return []
      }

      const fetchSegment = async (index: number) =>
        unwrapSegment(await (api.query as any).system.eventSegments.at(hash, index))

      if (numSegments != null) {
        for (let start = 0; start < numSegments; start += SEGMENT_CONCURRENCY) {
          const end = Math.min(numSegments, start + SEGMENT_CONCURRENCY)
          const results = await Promise.all(
            Array.from({ length: end - start }, (_, offset) => fetchSegment(start + offset)),
          )
          for (const arr of results) {
            if (!arr.length) continue
            for (const e of arr) flattened.push(e)
          }
        }
      } else if ((api.query as any).system.eventSegments.entriesAt) {
        // entriesAt fallback (older api) – value is (Option<Vec<EventRecord>> | Vec<EventRecord>)
        const entries = await (api.query as any).system.eventSegments.entriesAt(hash)
        for (const [, value] of entries as any[]) {
          const arr = unwrapSegment(value)
          for (const e of arr) flattened.push(e)
        }
      } else {
        // Blind scan up to a cap
        const MAX_SEGMENTS = Number(process.env.EVENTS_MAX_SEGMENTS || 2048)
        for (let i = 0; i < MAX_SEGMENTS; i++) {
          const arr = await fetchSegment(i)
          if (!arr.length) break
          for (const e of arr) flattened.push(e)
        }
      }

      if (flattened.length > 0) {
        // Deduplicate events that may appear in multiple segments
        const seen = new Set<string>()
        const unique: any[] = []
        for (const rec of flattened) {
          const ev = rec?.event ?? rec
          const section = ev?.section || ''
          const method = ev?.method || ''
          const data = ev?.data?.toString?.() || ''
          const phase = rec?.phase?.toString?.() || ''
          const key = `${section}|${method}|${data}|${phase}`
          if (!seen.has(key)) {
            seen.add(key)
            unique.push(rec)
          }
        }
        return unique
      }
    } catch {
      console.warn(
        '[events] eventSegments unavailable or incompatible, falling back to system.events',
      )
    }
  }
  return api.query.system.events.at(hash)
}
