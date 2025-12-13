/**
 * URL Parameter utilities for short parameter names.
 * Uses short parameter names to keep URLs clean and concise.
 */

// Short parameter names (used in URLs)
export const SHORT_PARAMS = {
  TAB: 't',
  RANGE: 'r',
  GRANULARITY: 'g',
  START_DATE: 'sd',
  END_DATE: 'ed',
} as const;

// Short values for parameters
export const SHORT_VALUES = {
  // Tabs
  TAB_OVERVIEW: 'o',
  TAB_VOLUME: 'v',
  TAB_REVENUE: 'r',
  TAB_USERS: 'u',
  TAB_COUNT: 'c',
  TAB_REFERRALS: 'ref',
  TAB_HOLDERS: 'h',

  // Date ranges
  RANGE_1D: '1d',
  RANGE_7D: '7d',
  RANGE_30D: '30d',
  RANGE_90D: '90d',
  RANGE_YTD: 'ytd',
  RANGE_1Y: '1y',
  RANGE_ALL: 'all',
  RANGE_CUSTOM: 'custom',

  // Granularity
  GRAN_HOUR: 'h',
  GRAN_DAY: 'd',
  GRAN_WEEK: 'w',
  GRAN_MONTH: 'm',
} as const;

// Long to short mappings for backward compatibility
const LONG_TO_SHORT_PARAMS: Record<string, string> = {
  tab: SHORT_PARAMS.TAB,
  range: SHORT_PARAMS.RANGE,
  granularity: SHORT_PARAMS.GRANULARITY,
  startDate: SHORT_PARAMS.START_DATE,
  endDate: SHORT_PARAMS.END_DATE,
};

const LONG_TO_SHORT_VALUES: Record<string, string> = {
  // Tabs
  overview: SHORT_VALUES.TAB_OVERVIEW,
  volume: SHORT_VALUES.TAB_VOLUME,
  revenue: SHORT_VALUES.TAB_REVENUE,
  users: SHORT_VALUES.TAB_USERS,
  count: SHORT_VALUES.TAB_COUNT,
  referrals: SHORT_VALUES.TAB_REFERRALS,
  holders: SHORT_VALUES.TAB_HOLDERS,

  // Granularity
  hour: SHORT_VALUES.GRAN_HOUR,
  day: SHORT_VALUES.GRAN_DAY,
  week: SHORT_VALUES.GRAN_WEEK,
  month: SHORT_VALUES.GRAN_MONTH,
};

/**
 * Get a parameter value from URLSearchParams, supporting both short and long formats.
 * @param searchParams - URLSearchParams instance
 * @param shortKey - The short parameter key to look for
 * @returns The parameter value or null if not found
 */
export function getParam(searchParams: URLSearchParams, shortKey: string): string | null {
  // First try the short key
  let value = searchParams.get(shortKey);
  if (value) return value;

  // Find the long key equivalent and try that
  const longKey = Object.entries(LONG_TO_SHORT_PARAMS).find(([, short]) => short === shortKey)?.[0];
  if (longKey) {
    value = searchParams.get(longKey);
    if (value) {
      // Convert long value to short if needed
      return LONG_TO_SHORT_VALUES[value] || value;
    }
  }

  return null;
}

/**
 * Convert URLSearchParams to a plain object.
 * @param searchParams - URLSearchParams instance
 * @returns Plain object with all parameters
 */
export function paramsToObject(searchParams: URLSearchParams): Record<string, string> {
  const obj: Record<string, string> = {};
  searchParams.forEach((value, key) => {
    obj[key] = value;
  });
  return obj;
}

/**
 * Build a query string from parameters object.
 * @param params - Object with parameter key-value pairs
 * @returns URLSearchParams query string
 */
export function buildParams(params: Record<string, string | undefined | null>): string {
  const searchParams = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== '') {
      searchParams.set(key, value);
    }
  }

  return searchParams.toString();
}

/**
 * Type for granularity values
 */
export type Granularity =
  | typeof SHORT_VALUES.GRAN_HOUR
  | typeof SHORT_VALUES.GRAN_DAY
  | typeof SHORT_VALUES.GRAN_WEEK
  | typeof SHORT_VALUES.GRAN_MONTH;

/**
 * Type for range values
 */
export type RangeType =
  | typeof SHORT_VALUES.RANGE_1D
  | typeof SHORT_VALUES.RANGE_7D
  | typeof SHORT_VALUES.RANGE_30D
  | typeof SHORT_VALUES.RANGE_90D
  | typeof SHORT_VALUES.RANGE_YTD
  | typeof SHORT_VALUES.RANGE_1Y
  | typeof SHORT_VALUES.RANGE_ALL
  | typeof SHORT_VALUES.RANGE_CUSTOM;
