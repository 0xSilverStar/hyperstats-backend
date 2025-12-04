# Trader Ranking Strategy

## Overview

HyperStats uses a multi-factor scoring system to rank traders based on their trading performance. Rankings are recalculated every 30 minutes to reflect the latest trading activity.

## Scoring Formula

Each trader's score (0-100) is calculated using three weighted factors:

| Factor | Weight | Description |
|--------|--------|-------------|
| Total PnL | 40% | Cumulative profit/loss across all trades |
| Win Rate | 35% | Percentage of profitable trades |
| Recent PnL (7d) | 25% | Profit/loss in the last 7 days |

**Note**: Volume and portfolio value are intentionally excluded from the scoring formula. High volume does not indicate good trading (more trades can mean more mistakes), and balance size is not a measure of trading skill.

## Normalization

Values are normalized to a 0-100 scale using the following ranges:

- **Total PnL**: -$1,000,000 to +$10,000,000
- **Recent PnL**: -$100,000 to +$1,000,000

Win rate is naturally a 0-1 value, multiplied by 100 for scoring.

## Grade System

Traders are assigned grades based on their percentile rank:

| Grade | Percentile | Description |
|-------|------------|-------------|
| S+ | Top 1% | Elite traders |
| S | Top 5% | Exceptional performers |
| A | Top 15% | Strong performers |
| B | Top 35% | Above average |
| C | Top 60% | Average |
| D | Top 85% | Below average |
| F | Bottom 15% | Underperforming |

## Data Sources

Rankings are calculated from:
- **Fill records**: PnL, trade count, win/loss ratio

## Update Frequency

- Rankings update every 30 minutes (at :00 and :30 of each hour)
- Initial calculation runs 30 seconds after server startup
