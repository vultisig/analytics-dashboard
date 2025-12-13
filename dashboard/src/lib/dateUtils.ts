/**
 * Date utility functions and types.
 */

import { SHORT_VALUES } from './urlParams';

/**
 * Type for date range values
 */
export type DateRangeType =
  | typeof SHORT_VALUES.RANGE_1D
  | typeof SHORT_VALUES.RANGE_7D
  | typeof SHORT_VALUES.RANGE_30D
  | typeof SHORT_VALUES.RANGE_90D
  | typeof SHORT_VALUES.RANGE_YTD
  | typeof SHORT_VALUES.RANGE_1Y
  | typeof SHORT_VALUES.RANGE_ALL
  | typeof SHORT_VALUES.RANGE_CUSTOM;

/**
 * Get the start date for a given date range.
 * @param range - The date range type
 * @returns Start date or null for 'all' range
 */
export function getStartDateForRange(range: DateRangeType): Date | null {
  const now = new Date();

  switch (range) {
    case SHORT_VALUES.RANGE_1D:
      return new Date(now.getTime() - 24 * 60 * 60 * 1000);
    case SHORT_VALUES.RANGE_7D:
      return new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    case SHORT_VALUES.RANGE_30D:
      return new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    case SHORT_VALUES.RANGE_90D:
      return new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
    case SHORT_VALUES.RANGE_YTD:
      return new Date(now.getFullYear(), 0, 1);
    case SHORT_VALUES.RANGE_1Y:
      return new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    case SHORT_VALUES.RANGE_ALL:
      return null;
    case SHORT_VALUES.RANGE_CUSTOM:
      return null; // Custom range uses explicit start/end dates
    default:
      return null;
  }
}

/**
 * Format a date as YYYY-MM-DD string.
 * @param date - Date to format
 * @returns Formatted date string
 */
export function formatDateISO(date: Date): string {
  return date.toISOString().split('T')[0];
}

/**
 * Parse a date string to Date object.
 * @param dateStr - Date string in YYYY-MM-DD format
 * @returns Date object
 */
export function parseDate(dateStr: string): Date {
  return new Date(dateStr + 'T00:00:00Z');
}

/**
 * Calculate the number of days between two dates.
 * @param startDate - Start date
 * @param endDate - End date
 * @returns Number of days
 */
export function daysBetween(startDate: Date, endDate: Date): number {
  const msPerDay = 24 * 60 * 60 * 1000;
  return Math.round((endDate.getTime() - startDate.getTime()) / msPerDay);
}

/**
 * Check if a date is within a range.
 * @param date - Date to check
 * @param startDate - Range start date
 * @param endDate - Range end date
 * @returns True if date is within range (inclusive)
 */
export function isDateInRange(date: Date, startDate: Date, endDate: Date): boolean {
  return date >= startDate && date <= endDate;
}

/**
 * Get display label for a date range.
 * @param range - The date range type
 * @returns Human-readable label
 */
export function getDateRangeLabel(range: DateRangeType): string {
  switch (range) {
    case SHORT_VALUES.RANGE_1D:
      return 'Last 24 Hours';
    case SHORT_VALUES.RANGE_7D:
      return 'Last 7 Days';
    case SHORT_VALUES.RANGE_30D:
      return 'Last 30 Days';
    case SHORT_VALUES.RANGE_90D:
      return 'Last 90 Days';
    case SHORT_VALUES.RANGE_YTD:
      return 'Year to Date';
    case SHORT_VALUES.RANGE_1Y:
      return 'Last Year';
    case SHORT_VALUES.RANGE_ALL:
      return 'All Time';
    case SHORT_VALUES.RANGE_CUSTOM:
      return 'Custom Range';
    default:
      return 'Unknown';
  }
}
