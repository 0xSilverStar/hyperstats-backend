/**
 * Dayjs wrapper with UTC and ISO week plugins pre-configured
 * Use this instead of importing dayjs directly to ensure consistent date handling
 */
import dayjs from 'dayjs';
import utc from 'dayjs/plugin/utc';
import isoWeek from 'dayjs/plugin/isoWeek';
import weekYear from 'dayjs/plugin/weekYear';
import weekOfYear from 'dayjs/plugin/weekOfYear';

// Initialize plugins
dayjs.extend(utc);
dayjs.extend(isoWeek);
dayjs.extend(weekYear);
dayjs.extend(weekOfYear);

// Re-export configured dayjs instance
export { dayjs };

/**
 * Create a UTC dayjs instance from timestamp
 */
export const utcFromTimestamp = (timestamp: number) => dayjs.utc(timestamp);

/**
 * Create a UTC dayjs instance from date string
 */
export const utcFromString = (dateStr: string) => dayjs.utc(dateStr);

/**
 * Get ISO week key (YYYY-WXX format) from timestamp
 */
export const getISOWeekKey = (timestamp: number): string => {
  const d = dayjs.utc(timestamp);
  const isoYear = d.isoWeekYear();
  const isoWeekNum = d.isoWeek();
  return `${isoYear}-W${isoWeekNum.toString().padStart(2, '0')}`;
};

/**
 * Get daily key (YYYY-MM-DD format) from timestamp
 */
export const getDailyKey = (timestamp: number): string => {
  return dayjs.utc(timestamp).format('YYYY-MM-DD');
};

/**
 * Get hourly key (YYYY-MM-DD HH:00 format) from timestamp
 */
export const getHourlyKey = (timestamp: number): string => {
  return dayjs.utc(timestamp).format('YYYY-MM-DD HH:00');
};

/**
 * Format timestamp as short date (e.g., "Nov 24")
 */
export const formatShortDate = (timestamp: number): string => {
  return dayjs.utc(timestamp).format('MMM D');
};

/**
 * Parse ISO week key (YYYY-WXX) and return the Monday of that week as timestamp
 */
export const parseISOWeekKey = (weekKey: string): number => {
  const match = weekKey.match(/^(\d{4})-W(\d{2})$/);
  if (!match) {
    throw new Error(`Invalid ISO week key: ${weekKey}`);
  }
  const year = parseInt(match[1], 10);
  const week = parseInt(match[2], 10);

  // Create a date in the target ISO week year, week 1
  // Then add (week - 1) weeks to get to the target week
  // ISO week 1 contains January 4th
  const jan4 = dayjs.utc(`${year}-01-04`);
  const week1Monday = jan4.isoWeekday(1); // Monday of week 1
  const targetMonday = week1Monday.add(week - 1, 'week');

  return targetMonday.valueOf();
};

/**
 * Format ISO week key (YYYY-WXX) as short date (e.g., "Nov 24")
 */
export const formatWeekKeyAsDate = (weekKey: string): string => {
  const timestamp = parseISOWeekKey(weekKey);
  return formatShortDate(timestamp);
};

export default dayjs;
