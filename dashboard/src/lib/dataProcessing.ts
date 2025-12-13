/**
 * Data processing utilities for chart data transformation.
 */

import type { DateRangeType } from './dateUtils';
import { SHORT_VALUES } from './urlParams';

interface RawDataItem {
  date: string;
  source: string;
  [key: string]: string | number;
}

interface ChartDataItem {
  date: string;
  [key: string]: string | number;
}

/**
 * Transform raw API data (with date, source, value) into chart format (date, provider1, provider2, ...).
 * @param data - Raw data array with date, source, and a value field
 * @param valueKey - The key of the value field (e.g., 'volume', 'revenue', 'count', 'users')
 * @returns Transformed data for charts
 */
export function transformToChartData(
  data: RawDataItem[],
  valueKey: string
): ChartDataItem[] {
  if (!data || data.length === 0) return [];

  // Group by date
  const byDate: Record<string, Record<string, number>> = {};

  data.forEach(item => {
    const date = item.date;
    const source = item.source;
    const value = Number(item[valueKey]) || 0;

    if (!byDate[date]) {
      byDate[date] = {};
    }

    byDate[date][source] = (byDate[date][source] || 0) + value;
  });

  // Convert to array format
  const result: ChartDataItem[] = Object.entries(byDate)
    .map(([date, values]) => ({
      date,
      ...values,
    }))
    .sort((a, b) => new Date(a.date).getTime() - new Date(b.date).getTime());

  return result;
}

/**
 * Filter data by date range.
 * @param data - Chart data array
 * @param range - Date range type
 * @param startDate - Optional start date for custom range
 * @param endDate - Optional end date for custom range
 * @returns Filtered data
 */
export function filterByDateRange(
  data: ChartDataItem[],
  range: DateRangeType,
  startDate?: string | null,
  endDate?: string | null
): ChartDataItem[] {
  if (!data || data.length === 0) return [];

  const now = new Date();
  let filterStartDate: Date | null = null;
  let filterEndDate: Date = now;

  switch (range) {
    case SHORT_VALUES.RANGE_1D:
      filterStartDate = new Date(now.getTime() - 24 * 60 * 60 * 1000);
      break;
    case SHORT_VALUES.RANGE_7D:
      filterStartDate = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
      break;
    case SHORT_VALUES.RANGE_30D:
      filterStartDate = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
      break;
    case SHORT_VALUES.RANGE_90D:
      filterStartDate = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
      break;
    case SHORT_VALUES.RANGE_YTD:
      filterStartDate = new Date(now.getFullYear(), 0, 1);
      break;
    case SHORT_VALUES.RANGE_1Y:
      filterStartDate = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
      break;
    case SHORT_VALUES.RANGE_CUSTOM:
      if (startDate) filterStartDate = new Date(startDate);
      if (endDate) filterEndDate = new Date(endDate);
      break;
    case SHORT_VALUES.RANGE_ALL:
    default:
      // No filter for 'all'
      return data;
  }

  return data.filter(item => {
    const itemDate = new Date(item.date);
    if (filterStartDate && itemDate < filterStartDate) return false;
    if (itemDate > filterEndDate) return false;
    return true;
  });
}

type GranularityType = 'h' | 'd' | 'w' | 'm';

/**
 * Aggregate data by granularity (hour, day, week, month).
 * @param data - Chart data array
 * @param granularity - Granularity level
 * @param providers - List of providers to include
 * @returns Aggregated data
 */
export function aggregateByGranularity(
  data: ChartDataItem[],
  granularity: GranularityType,
  providers: string[]
): ChartDataItem[] {
  if (!data || data.length === 0) return [];

  // Group by the appropriate time bucket
  // Store both the formatted date string and the original timestamp for sorting
  const grouped: Record<string, { values: Record<string, number>; timestamp: number }> = {};

  data.forEach(item => {
    const date = new Date(item.date);
    let bucketKey: string;

    switch (granularity) {
      case 'h':
        // Format: "Dec 10 14" (Month Day Hour)
        bucketKey = date.toLocaleString('en-US', {
          month: 'short',
          day: 'numeric',
          hour: '2-digit',
          hour12: false,
        }).replace(',', '');
        break;
      case 'w':
        // Get week start (Sunday)
        const weekStart = new Date(date);
        weekStart.setDate(date.getDate() - date.getDay());
        bucketKey = weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        break;
      case 'm':
        // Format: "2024 Jan"
        bucketKey = date.toLocaleString('en-US', { year: 'numeric', month: 'short' });
        break;
      case 'd':
      default:
        // Format: "Dec 10"
        bucketKey = date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
        break;
    }

    if (!grouped[bucketKey]) {
      grouped[bucketKey] = {
        values: {},
        timestamp: date.getTime(),
      };
      providers.forEach(p => {
        grouped[bucketKey].values[p] = 0;
      });
    }

    // Update timestamp to the earliest date in the bucket for consistent sorting
    if (date.getTime() < grouped[bucketKey].timestamp) {
      grouped[bucketKey].timestamp = date.getTime();
    }

    // Sum values for each provider
    providers.forEach(provider => {
      if (item[provider] !== undefined) {
        grouped[bucketKey].values[provider] += Number(item[provider]) || 0;
      }
    });
  });

  // Convert to array and sort by timestamp
  const result: ChartDataItem[] = Object.entries(grouped)
    .map(([date, { values, timestamp }]) => ({
      date,
      ...values,
      timestamp, // Store timestamp for sorting
    }))
    .sort((a, b) => a.timestamp - b.timestamp)
    .map(({ timestamp, ...item }) => item); // Remove timestamp after sorting

  return result;
}

/**
 * Calculate cumulative totals for chart data.
 * @param data - Chart data array
 * @param keys - Keys to accumulate
 * @returns Data with cumulative values
 */
export function toCumulativeData(
  data: ChartDataItem[],
  keys: string[]
): ChartDataItem[] {
  const cumulative: ChartDataItem[] = [];
  const runningTotals: Record<string, number> = {};

  keys.forEach(key => {
    runningTotals[key] = 0;
  });

  data.forEach(item => {
    const cumulativeItem: ChartDataItem = { date: item.date };
    keys.forEach(key => {
      runningTotals[key] += Number(item[key]) || 0;
      cumulativeItem[key] = runningTotals[key];
    });
    cumulative.push(cumulativeItem);
  });

  return cumulative;
}
