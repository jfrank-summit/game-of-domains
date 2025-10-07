import * as dotenv from 'dotenv'
import * as fs from 'node:fs'
import * as path from 'node:path'
import * as readline from 'node:readline'
import type { TransferRow, CountsResult } from './types'
import { generateHtmlLeaderboard } from './htmlLeaderboard'

// Load .env from project root first
dotenv.config({ path: path.resolve(__dirname, '../../../.env'), quiet: true })
// Then load from local directory (overrides root if exists)
dotenv.config({ path: path.resolve(__dirname, '../.env'), override: true, quiet: true })

const OUTPUT_DIR = process.env.OUTPUT_DIR || 'exports'

const ensureDir = (dirPath: string): void => {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }
}

const readNdjsonCounts = async (
  filePath: string,
  expectedDirection: 'd2c' | 'c2d',
): Promise<{ counts: Record<string, number>; total: number }> => {
  const counts: Record<string, number> = {}
  let total = 0

  if (!fs.existsSync(filePath)) {
    return { counts, total }
  }

  const stream = fs.createReadStream(filePath, { encoding: 'utf8' })
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity })

  for await (const line of rl) {
    const trimmed = line.trim()
    if (trimmed === '') continue
    try {
      const row = JSON.parse(trimmed) as Partial<TransferRow>
      if (!row || row.direction !== expectedDirection) continue
      const from = String(row.from || '')
      if (!from) continue
      counts[from] = (counts[from] ?? 0) + 1
      total += 1
    } catch {
      // skip malformed line
    }
  }

  return { counts, total }
}

const writeJsonFile = (filePath: string, data: unknown): void => {
  ensureDir(path.dirname(filePath))
  fs.writeFileSync(filePath, JSON.stringify(data, null, 2) + '\n', 'utf8')
}

const main = async () => {
  const outDirAbs = path.resolve(OUTPUT_DIR)
  const d2cPath = path.resolve(outDirAbs, 'd2c_transfers.ndjson')
  const c2dPath = path.resolve(outDirAbs, 'c2d_transfers.ndjson')

  const [{ counts: d2cCounts, total: d2cTotal }, { counts: c2dCounts, total: c2dTotal }] =
    await Promise.all([readNdjsonCounts(d2cPath, 'd2c'), readNdjsonCounts(c2dPath, 'c2d')])

  const overallTotal = d2cTotal + c2dTotal

  // Convert counts to include percentages and sort by count descending
  // Use 6 decimals to show meaningful percentages for even single transfers
  // 1 transfer out of overall total (2,718,350) = 0.000037%
  const d2cSorted = Object.entries(d2cCounts)
    .sort(([, a], [, b]) => b - a)
    .map(([wallet, count]) => ({
      wallet,
      count,
      percent: ((count / overallTotal) * 100).toFixed(6) + '%',
    }))

  const c2dSorted = Object.entries(c2dCounts)
    .sort(([, a], [, b]) => b - a)
    .map(([wallet, count]) => ({
      wallet,
      count,
      percent: ((count / overallTotal) * 100).toFixed(6) + '%',
    }))

  const result: CountsResult = {
    d2c: d2cSorted,
    c2d: c2dSorted,
    totals: {
      d2c: d2cTotal,
      d2c_percent: ((d2cTotal / overallTotal) * 100).toFixed(3) + '%',
      c2d: c2dTotal,
      c2d_percent: ((c2dTotal / overallTotal) * 100).toFixed(3) + '%',
      overall: overallTotal,
    },
    block_ranges: {
      consensus_start: process.env.CONSENSUS_START_HEIGHT || 'N/A',
      consensus_end: process.env.CONSENSUS_END_HEIGHT || 'N/A',
      domain_start: process.env.DOMAIN_START_HEIGHT || 'N/A',
      domain_end: process.env.DOMAIN_END_HEIGHT || 'N/A',
    },
    files: { d2c_transfers: d2cPath, c2d_transfers: c2dPath },
    output_dir: outDirAbs,
    generated_at: new Date().toISOString(),
  }

  const countsFile = path.resolve(outDirAbs, 'counts_per_wallet.json')
  writeJsonFile(countsFile, result)

  // Check if HTML output is requested via environment variable or command line
  const htmlOutput = process.env.HTML_OUTPUT === 'true' || process.argv.includes('--html')

  if (htmlOutput) {
    const htmlFile = path.resolve(outDirAbs, 'leaderboard.html')
    generateHtmlLeaderboard(htmlFile, result)
    console.log(
      JSON.stringify(
        {
          counts_file: countsFile,
          html_file: htmlFile,
          totals: result.totals,
          block_ranges: result.block_ranges,
          wallet_counts: { d2c: result.d2c.length, c2d: result.c2d.length },
          generated_at: result.generated_at,
        },
        null,
        2,
      ),
    )
  } else {
    console.log(JSON.stringify({ counts_file: countsFile, ...result }, null, 2))
  }
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
