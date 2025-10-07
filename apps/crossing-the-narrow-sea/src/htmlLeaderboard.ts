/**
 * HTML leaderboard generator for Game of Domains transfer statistics
 *
 * Generates an interactive HTML page with:
 * - Dark mode support with system preference detection
 * - Real-time wallet address search and filtering
 * - Sortable leaderboards for D2C and C2D transfers
 * - Visual highlighting for top performers
 */

import * as fs from 'node:fs'
import * as path from 'node:path'
import type { CountsResult } from './types'

/**
 * Ensures a directory exists, creating it recursively if necessary
 */
const ensureDir = (dirPath: string): void => {
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true })
  }
}

/**
 * Generates an HTML leaderboard file from transfer count results
 *
 * @param filePath - Absolute path where the HTML file should be written
 * @param result - Transfer count statistics and metadata
 */
export const generateHtmlLeaderboard = (filePath: string, result: CountsResult): void => {
  ensureDir(path.dirname(filePath))

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Game of Domains: Crossing the Narrow Sea</title>
  <style>
    :root {
      --bg-primary: #f5f5f5;
      --bg-card: white;
      --bg-section: #f8f9fa;
      --bg-hover: #e9ecef;
      --bg-highlight: #fff3cd;
      --bg-top3: #d4edda;
      --text-primary: #333;
      --text-secondary: #666;
      --text-muted: #999;
      --border-color: #dee2e6;
      --border-input: #ddd;
      --accent-color: #007bff;
      --shadow: 0 2px 8px rgba(0,0,0,0.1);
    }

    body.dark {
      --bg-primary: #1a1a1a;
      --bg-card: #2d2d2d;
      --bg-section: #3a3a3a;
      --bg-hover: #4a4a4a;
      --bg-highlight: #5a4a2d;
      --bg-top3: #2d4a2d;
      --text-primary: #e0e0e0;
      --text-secondary: #b0b0b0;
      --text-muted: #808080;
      --border-color: #4a4a4a;
      --border-input: #4a4a4a;
      --accent-color: #4a9eff;
      --shadow: 0 2px 8px rgba(0,0,0,0.3);
    }

    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; padding: 20px; background: var(--bg-primary); transition: background 0.3s ease; }
    .container { max-width: 1400px; margin: 0 auto; background: var(--bg-card); padding: 30px; border-radius: 8px; box-shadow: var(--shadow); position: relative; }
    .dark-mode-toggle { position: absolute; top: 30px; right: 30px; background: var(--bg-section); border: 2px solid var(--border-color); border-radius: 6px; padding: 8px 16px; cursor: pointer; font-size: 14px; color: var(--text-primary); transition: all 0.2s; }
    .dark-mode-toggle:hover { background: var(--bg-hover); }
    h1 { color: var(--text-primary); margin-bottom: 10px; }
    .subtitle { color: var(--text-secondary); margin-bottom: 30px; font-size: 14px; }
    .totals { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
    .total-card { background: var(--bg-section); padding: 20px; border-radius: 6px; border-left: 4px solid var(--accent-color); }
    .total-card h3 { font-size: 14px; color: var(--text-secondary); margin-bottom: 8px; text-transform: uppercase; }
    .total-card .value { font-size: 28px; font-weight: bold; color: var(--text-primary); }
    .total-card .percent { font-size: 16px; color: var(--accent-color); margin-top: 4px; }
    .search-box { margin-bottom: 20px; }
    .search-box input { width: 100%; max-width: 600px; padding: 12px; border: 2px solid var(--border-input); border-radius: 6px; font-size: 16px; background: var(--bg-card); color: var(--text-primary); }
    .search-box input:focus { outline: none; border-color: var(--accent-color); }
    .search-box p { color: var(--text-secondary); }
    .section { margin-bottom: 40px; }
    .section h2 { color: var(--text-primary); margin-bottom: 15px; padding-bottom: 10px; border-bottom: 2px solid var(--accent-color); }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    thead { background: var(--bg-section); position: sticky; top: 0; }
    th { padding: 12px; text-align: left; font-weight: 600; color: var(--text-primary); border-bottom: 2px solid var(--border-color); }
    td { padding: 12px; border-bottom: 1px solid var(--border-color); color: var(--text-primary); }
    tr:hover { background: var(--bg-section); }
    .wallet { font-family: 'Courier New', monospace; font-size: 13px; }
    .highlight { background: var(--bg-highlight) !important; }
    .rank { font-weight: bold; color: var(--text-secondary); }
    .top-3 { background: var(--bg-top3); }
  </style>
</head>
<body>
  <div class="container">
    <button class="dark-mode-toggle" id="darkModeToggle">🌙 Dark Mode</button>
    <h1>Game of Domains: Crossing the Narrow Sea</h1>
    <p class="subtitle">Transfer Leaderboard - Generated ${result.generated_at}</p>

    <div class="totals">
      <div class="total-card">
        <h3>Total Transfers</h3>
        <div class="value">${result.totals.overall.toLocaleString()}</div>
      </div>
      <div class="total-card">
        <h3>D2C Transfers</h3>
        <div class="value">${result.totals.d2c.toLocaleString()}</div>
        <div class="percent">${result.totals.d2c_percent}</div>
        <div style="font-size: 14px; color: var(--text-secondary); margin-top: 8px;">${result.d2c.length.toLocaleString()} wallets</div>
        <div style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">Blocks ${result.block_ranges.domain_start} → ${result.block_ranges.domain_end}</div>
      </div>
      <div class="total-card">
        <h3>C2D Transfers</h3>
        <div class="value">${result.totals.c2d.toLocaleString()}</div>
        <div class="percent">${result.totals.c2d_percent}</div>
        <div style="font-size: 14px; color: var(--text-secondary); margin-top: 8px;">${result.c2d.length.toLocaleString()} wallets</div>
        <div style="font-size: 12px; color: var(--text-muted); margin-top: 4px;">Blocks ${result.block_ranges.consensus_start} → ${result.block_ranges.consensus_end}</div>
      </div>
    </div>

    <div class="search-box">
      <input type="text" id="searchInput" placeholder="Search by wallet address..." aria-label="Search wallet addresses">
      <p style="margin-top: 8px; color: #666; font-size: 13px;">Type any part of a wallet address to filter and highlight matches</p>
    </div>

    <div class="section">
      <h2>Domain → Consensus (D2C) Rankings</h2>
      <table id="d2cTable">
        <thead>
          <tr>
            <th style="width: 60px;">Rank</th>
            <th>Wallet Address</th>
            <th style="width: 180px;">Transfer Count</th>
            <th style="width: 150px;">Share %</th>
          </tr>
        </thead>
        <tbody>
          ${result.d2c
            .map(
              (entry, idx) => `
            <tr class="${idx < 3 ? 'top-3' : ''}" data-wallet="${entry.wallet.toLowerCase()}">
              <td class="rank">#${idx + 1}</td>
              <td class="wallet">${entry.wallet}</td>
              <td>${entry.count.toLocaleString()}</td>
              <td>${entry.percent}</td>
            </tr>`,
            )
            .join('')}
        </tbody>
      </table>
    </div>

    <div class="section">
      <h2>Consensus → Domain (C2D) Rankings</h2>
      <table id="c2dTable">
        <thead>
          <tr>
            <th style="width: 60px;">Rank</th>
            <th>Wallet Address</th>
            <th style="width: 180px;">Transfer Count</th>
            <th style="width: 150px;">Share %</th>
          </tr>
        </thead>
        <tbody>
          ${result.c2d
            .map(
              (entry, idx) => `
            <tr class="${idx < 3 ? 'top-3' : ''}" data-wallet="${entry.wallet.toLowerCase()}">
              <td class="rank">#${idx + 1}</td>
              <td class="wallet">${entry.wallet}</td>
              <td>${entry.count.toLocaleString()}</td>
              <td>${entry.percent}</td>
            </tr>`,
            )
            .join('')}
        </tbody>
      </table>
    </div>
  </div>

  <script>
    // Dark mode functionality
    const darkModeToggle = document.getElementById('darkModeToggle');
    const body = document.body;

    // Check for saved preference or system preference
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)');
    const savedTheme = localStorage.getItem('theme');

    if (savedTheme === 'dark' || (!savedTheme && prefersDark.matches)) {
      body.classList.add('dark');
      darkModeToggle.textContent = '☀️ Light Mode';
    }

    darkModeToggle.addEventListener('click', () => {
      body.classList.toggle('dark');
      const isDark = body.classList.contains('dark');
      darkModeToggle.textContent = isDark ? '☀️ Light Mode' : '🌙 Dark Mode';
      localStorage.setItem('theme', isDark ? 'dark' : 'light');
    });

    // Listen for system preference changes
    prefersDark.addEventListener('change', (e) => {
      if (!localStorage.getItem('theme')) {
        body.classList.toggle('dark', e.matches);
        darkModeToggle.textContent = e.matches ? '☀️ Light Mode' : '🌙 Dark Mode';
      }
    });

    // Search functionality
    const searchInput = document.getElementById('searchInput');
    const tables = document.querySelectorAll('table');

    searchInput.addEventListener('input', (e) => {
      const search = e.target.value.toLowerCase();

      tables.forEach(table => {
        const rows = table.querySelectorAll('tbody tr');
        rows.forEach(row => {
          const wallet = row.getAttribute('data-wallet');
          if (wallet.includes(search)) {
            row.style.display = '';
            row.classList.add('highlight');
          } else {
            row.style.display = search ? 'none' : '';
            row.classList.remove('highlight');
          }
        });
      });

      // Scroll to first match if search is active
      if (search) {
        const firstMatch = document.querySelector('tr.highlight');
        if (firstMatch) {
          firstMatch.scrollIntoView({ behavior: 'smooth', block: 'center' });
        }
      }
    });
  </script>
</body>
</html>`

  fs.writeFileSync(filePath, html, 'utf8')
}
