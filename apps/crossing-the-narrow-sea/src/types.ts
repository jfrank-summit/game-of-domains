/**
 * Shared type definitions for crossing-the-narrow-sea analysis
 */

export interface TransferRow {
  direction: 'd2c' | 'c2d'
  from: string
}

export interface WalletEntry {
  wallet: string
  count: number
  percent: string
}

export interface CountsResult {
  d2c: WalletEntry[]
  c2d: WalletEntry[]
  totals: {
    d2c: number
    d2c_percent: string
    c2d: number
    c2d_percent: string
    overall: number
  }
  block_ranges: {
    consensus_start: string
    consensus_end: string
    domain_start: string
    domain_end: string
  }
  files: { d2c_transfers: string; c2d_transfers: string }
  output_dir: string
  generated_at: string
}
