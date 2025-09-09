import Database from 'better-sqlite3'
import * as fs from 'node:fs'
import * as path from 'node:path'

export const openDb = (filePath: string) => {
  const abs = path.resolve(filePath)
  const dir = path.dirname(abs)
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true })
  }
  const db = new Database(abs)
  db.pragma('journal_mode = WAL')
  ensureTables(db)
  return db
}

export interface SourceInit {
  source_chain: 'consensus' | 'domain'
  dst_chain_id: number
  channel_id: number
  nonce: string
  from_address: string
  amount: string
  source_block_height: number
  source_block_hash: string
  source_extrinsic_index: number | null
}

export interface DestinationSuccess {
  destination_chain: 'consensus' | 'domain'
  src_chain_id: number
  channel_id: number
  nonce: string
  amount: string
  destination_block_height: number
  destination_block_hash: string
}

export interface SourceAck {
  source_chain: 'consensus' | 'domain'
  dst_chain_id: number
  channel_id: number
  nonce: string
  result: 'Ok' | 'Err'
  source_block_height: number
  source_block_hash: string
}

export const ensureTables = (db: Database.Database) => {
  db.exec(`
    CREATE TABLE IF NOT EXISTS source_inits (
      source_chain TEXT NOT NULL,
      dst_chain_id INTEGER NOT NULL,
      channel_id INTEGER NOT NULL,
      nonce TEXT NOT NULL,
      from_address TEXT NOT NULL,
      amount TEXT NOT NULL,
      source_block_height INTEGER NOT NULL,
      source_block_hash TEXT NOT NULL,
      source_extrinsic_index INTEGER,
      PRIMARY KEY (source_chain, channel_id, nonce)
    );
    CREATE TABLE IF NOT EXISTS destination_successes (
      destination_chain TEXT NOT NULL,
      src_chain_id INTEGER NOT NULL,
      channel_id INTEGER NOT NULL,
      nonce TEXT NOT NULL,
      amount TEXT NOT NULL,
      destination_block_height INTEGER NOT NULL,
      destination_block_hash TEXT NOT NULL,
      PRIMARY KEY (destination_chain, channel_id, nonce)
    );
    CREATE TABLE IF NOT EXISTS source_acks (
      source_chain TEXT NOT NULL,
      dst_chain_id INTEGER NOT NULL,
      channel_id INTEGER NOT NULL,
      nonce TEXT NOT NULL,
      result TEXT NOT NULL,
      source_block_height INTEGER NOT NULL,
      source_block_hash TEXT NOT NULL,
      PRIMARY KEY (source_chain, channel_id, nonce)
    );
    CREATE INDEX IF NOT EXISTS idx_src_init_key ON source_inits (channel_id, nonce, source_chain);
    CREATE INDEX IF NOT EXISTS idx_dst_succ_key ON destination_successes (channel_id, nonce, destination_chain);
    CREATE INDEX IF NOT EXISTS idx_src_ack_key ON source_acks (channel_id, nonce, source_chain);
    CREATE TABLE IF NOT EXISTS scan_progress (
      chain TEXT PRIMARY KEY,
      last_block_height INTEGER NOT NULL
    );
  `)
}

export const upsertSourceInit = (db: Database.Database, row: SourceInit) => {
  ensureTables(db)
  const stmt = db.prepare(`
    INSERT INTO source_inits (
      source_chain, dst_chain_id, channel_id, nonce, from_address, amount,
      source_block_height, source_block_hash, source_extrinsic_index
    ) VALUES (
      @source_chain, @dst_chain_id, @channel_id, @nonce, @from_address, @amount,
      @source_block_height, @source_block_hash, @source_extrinsic_index
    )
    ON CONFLICT(source_chain, channel_id, nonce) DO UPDATE SET
      dst_chain_id=excluded.dst_chain_id,
      from_address=excluded.from_address,
      amount=excluded.amount,
      source_block_height=excluded.source_block_height,
      source_block_hash=excluded.source_block_hash,
      source_extrinsic_index=excluded.source_extrinsic_index
  `)
  stmt.run(row as any)
}

export const upsertDestinationSuccess = (db: Database.Database, row: DestinationSuccess) => {
  ensureTables(db)
  const stmt = db.prepare(`
    INSERT INTO destination_successes (
      destination_chain, src_chain_id, channel_id, nonce, amount,
      destination_block_height, destination_block_hash
    ) VALUES (
      @destination_chain, @src_chain_id, @channel_id, @nonce, @amount,
      @destination_block_height, @destination_block_hash
    )
    ON CONFLICT(destination_chain, channel_id, nonce) DO UPDATE SET
      src_chain_id=excluded.src_chain_id,
      amount=excluded.amount,
      destination_block_height=excluded.destination_block_height,
      destination_block_hash=excluded.destination_block_hash
  `)
  stmt.run(row as any)
}

export const upsertSourceAck = (db: Database.Database, row: SourceAck) => {
  ensureTables(db)
  const stmt = db.prepare(`
    INSERT INTO source_acks (
      source_chain, dst_chain_id, channel_id, nonce, result,
      source_block_height, source_block_hash
    ) VALUES (
      @source_chain, @dst_chain_id, @channel_id, @nonce, @result,
      @source_block_height, @source_block_hash
    )
    ON CONFLICT(source_chain, channel_id, nonce) DO UPDATE SET
      dst_chain_id=excluded.dst_chain_id,
      result=excluded.result,
      source_block_height=excluded.source_block_height,
      source_block_hash=excluded.source_block_hash
  `)
  stmt.run(row as any)
}

export const getLastProcessedBlockHeight = (
  db: Database.Database,
  chain: 'consensus' | 'domain',
): number | null => {
  ensureTables(db)
  const row = db
    .prepare('SELECT last_block_height AS height FROM scan_progress WHERE chain = ?')
    .get(chain) as { height: number | null } | undefined
  return row?.height ?? null
}

export const setLastProcessedBlockHeight = (
  db: Database.Database,
  chain: 'consensus' | 'domain',
  height: number,
): void => {
  ensureTables(db)
  db.prepare(
    'INSERT INTO scan_progress(chain, last_block_height) VALUES (?, ?) ON CONFLICT(chain) DO UPDATE SET last_block_height=MAX(last_block_height, excluded.last_block_height)',
  ).run(chain, height)
}
