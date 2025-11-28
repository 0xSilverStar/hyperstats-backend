/**
 * Colored console logger for CLI commands
 * Uses ANSI escape codes for terminal colors
 */

// ANSI color codes
const colors = {
  reset: '\x1b[0m',
  bright: '\x1b[1m',
  dim: '\x1b[2m',

  // Foreground colors
  black: '\x1b[30m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',

  // Background colors
  bgBlack: '\x1b[40m',
  bgRed: '\x1b[41m',
  bgGreen: '\x1b[42m',
  bgYellow: '\x1b[43m',
  bgBlue: '\x1b[44m',
  bgMagenta: '\x1b[45m',
  bgCyan: '\x1b[46m',
  bgWhite: '\x1b[47m',
};

export class ConsoleLogger {
  private prefix: string;

  constructor(prefix: string) {
    this.prefix = prefix;
  }

  private getTimestamp(): string {
    return new Date().toLocaleTimeString('en-US', { hour12: false });
  }

  private formatPrefix(): string {
    return `${colors.dim}[${this.getTimestamp()}]${colors.reset} ${colors.cyan}[${this.prefix}]${colors.reset}`;
  }

  // Standard log levels
  info(message: string): void {
    console.log(`${this.formatPrefix()} ${colors.blue}ℹ${colors.reset} ${message}`);
  }

  success(message: string): void {
    console.log(`${this.formatPrefix()} ${colors.green}✓${colors.reset} ${colors.green}${message}${colors.reset}`);
  }

  warn(message: string): void {
    console.log(`${this.formatPrefix()} ${colors.yellow}⚠${colors.reset} ${colors.yellow}${message}${colors.reset}`);
  }

  error(message: string): void {
    console.log(`${this.formatPrefix()} ${colors.red}✗${colors.reset} ${colors.red}${message}${colors.reset}`);
  }

  debug(message: string): void {
    console.log(`${this.formatPrefix()} ${colors.dim}${message}${colors.reset}`);
  }

  // Sync-specific formatted outputs
  header(message: string): void {
    const line = '═'.repeat(60);
    console.log(`\n${colors.cyan}${line}${colors.reset}`);
    console.log(`${colors.bright}${colors.cyan}  ${message}${colors.reset}`);
    console.log(`${colors.cyan}${line}${colors.reset}\n`);
  }

  subHeader(message: string): void {
    console.log(`\n${colors.yellow}━━━ ${message} ━━━${colors.reset}\n`);
  }

  // Block sync progress
  blockSync(params: { fromBlock: number; toBlock: number; totalTransactions: number; whaleCount: number; iteration?: number }): void {
    const { fromBlock, toBlock, totalTransactions, whaleCount, iteration } = params;
    const iterationStr = iteration ? `#${iteration} ` : '';

    console.log(
      `${this.formatPrefix()} ${colors.magenta}⬢${colors.reset} ` +
        `${iterationStr}Block ${colors.bright}${fromBlock.toLocaleString()}${colors.reset} → ` +
        `${colors.bright}${toBlock.toLocaleString()}${colors.reset} | ` +
        `Tx: ${colors.blue}${totalTransactions.toLocaleString()}${colors.reset} | ` +
        `Whales: ${whaleCount > 0 ? colors.green + colors.bright : colors.dim}${whaleCount}${colors.reset}`,
    );
  }

  // Summary stats
  syncSummary(params: { totalIterations: number; totalTransactions: number; totalWhales: number; duration?: number }): void {
    const { totalIterations, totalTransactions, totalWhales, duration } = params;

    console.log(`\n${colors.cyan}┌─────────────────────────────────────────┐${colors.reset}`);
    console.log(`${colors.cyan}│${colors.reset} ${colors.bright}Sync Summary${colors.reset}                            ${colors.cyan}│${colors.reset}`);
    console.log(`${colors.cyan}├─────────────────────────────────────────┤${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Iterations:    ${colors.yellow}${String(totalIterations).padStart(20)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Transactions:  ${colors.blue}${totalTransactions.toLocaleString().padStart(20)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Whales Found:  ${colors.green}${totalWhales.toLocaleString().padStart(20)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    if (duration) {
      console.log(
        `${colors.cyan}│${colors.reset}  Duration:      ${colors.magenta}${(duration / 1000).toFixed(1).padStart(18)}s${colors.reset} ${colors.cyan}│${colors.reset}`,
      );
    }
    console.log(`${colors.cyan}└─────────────────────────────────────────┘${colors.reset}\n`);
  }

  // Status display
  status(params: { currentBlock: number; highestSynced: number; completedRanges: number; percentage: number }): void {
    const { currentBlock, highestSynced, completedRanges, percentage } = params;

    // Progress bar
    const barWidth = 30;
    const filled = Math.round((percentage / 100) * barWidth);
    const empty = barWidth - filled;
    const progressBar = `${colors.green}${'█'.repeat(filled)}${colors.dim}${'░'.repeat(empty)}${colors.reset}`;

    console.log(`\n${colors.cyan}┌─────────────────────────────────────────┐${colors.reset}`);
    console.log(`${colors.cyan}│${colors.reset} ${colors.bright}Sync Status${colors.reset}                             ${colors.cyan}│${colors.reset}`);
    console.log(`${colors.cyan}├─────────────────────────────────────────┤${colors.reset}`);
    console.log(
      `${colors.cyan}│${colors.reset}  Current Block:  ${colors.yellow}${currentBlock.toLocaleString().padStart(19)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Synced Block:   ${colors.green}${highestSynced.toLocaleString().padStart(19)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Completed:      ${colors.blue}${String(completedRanges).padStart(19)}${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(
      `${colors.cyan}│${colors.reset}  Progress:       ${progressBar} ${colors.bright}${percentage.toFixed(1)}%${colors.reset} ${colors.cyan}│${colors.reset}`,
    );
    console.log(`${colors.cyan}└─────────────────────────────────────────┘${colors.reset}\n`);
  }

  // Gap info
  gap(index: number, total: number, start: number, end: number): void {
    console.log(
      `${this.formatPrefix()} ${colors.yellow}◆${colors.reset} ` +
        `Gap ${colors.bright}${index}/${total}${colors.reset}: ` +
        `${colors.dim}Block${colors.reset} ${colors.yellow}${start.toLocaleString()}${colors.reset} → ` +
        `${colors.yellow}${end.toLocaleString()}${colors.reset} ` +
        `${colors.dim}(${(end - start + 1).toLocaleString()} blocks)${colors.reset}`,
    );
  }

  // Progress indicator
  progress(current: number, total: number, label: string = ''): void {
    const percentage = total > 0 ? (current / total) * 100 : 0;
    const barWidth = 20;
    const filled = Math.round((percentage / 100) * barWidth);
    const empty = barWidth - filled;
    const progressBar = `${colors.green}${'█'.repeat(filled)}${colors.dim}${'░'.repeat(empty)}${colors.reset}`;

    process.stdout.write(`\r${this.formatPrefix()} ${progressBar} ${colors.bright}${percentage.toFixed(1)}%${colors.reset} ${label}`);
  }

  // New line after progress
  progressDone(): void {
    console.log('');
  }
}

// Export singleton instances for common use cases
export const syncLogger = new ConsoleLogger('Sync');
export const tradingLogger = new ConsoleLogger('Trading');
export const pairsLogger = new ConsoleLogger('Pairs');
