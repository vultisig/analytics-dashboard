/**
 * API utility module for making requests to the backend API.
 *
 * In Docker: NEXT_PUBLIC_API_URL = http://backend:8080
 * For local dev: NEXT_PUBLIC_API_URL = http://localhost:8080
 *
 * If NEXT_PUBLIC_API_URL is not set, falls back to relative URLs
 * which will use Next.js API routes (for backward compatibility during migration).
 */

/**
 * Get the base API URL from environment variable.
 * Returns empty string if not set (falls back to relative URLs).
 */
export function getApiBaseUrl(): string {
  return process.env.NEXT_PUBLIC_API_URL || '';
}

/**
 * Build a full API URL for a given endpoint.
 * @param endpoint - The API endpoint (e.g., '/api/revenue')
 * @returns Full URL to the API endpoint
 */
export function buildApiUrl(endpoint: string): string {
  const baseUrl = getApiBaseUrl();
  // Ensure endpoint starts with /api
  const normalizedEndpoint = endpoint.startsWith('/api') ? endpoint : `/api${endpoint}`;
  return `${baseUrl}${normalizedEndpoint}`;
}

/**
 * Custom error class for API errors
 */
export class ApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public statusText: string
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

/**
 * Fetch data from the API with consistent error handling.
 * @param endpoint - The API endpoint (e.g., '/api/revenue')
 * @param options - Optional fetch options
 * @returns Parsed JSON response
 * @throws ApiError if the request fails
 */
export async function fetchApi<T>(
  endpoint: string,
  options?: RequestInit
): Promise<T> {
  const url = buildApiUrl(endpoint);

  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    let errorMessage = `API request failed: ${response.status} ${response.statusText}`;

    // Try to parse error message from response body
    try {
      const errorData = await response.json();
      if (errorData.error) {
        errorMessage = errorData.error;
      } else if (errorData.message) {
        errorMessage = errorData.message;
      }
    } catch {
      // Ignore JSON parse errors for error response
    }

    throw new ApiError(errorMessage, response.status, response.statusText);
  }

  return response.json();
}

/**
 * Build query string from parameters object.
 * Handles null/undefined values by excluding them.
 * @param params - Object with query parameters
 * @returns URLSearchParams instance
 */
export function buildQueryParams(params: Record<string, string | null | undefined>): URLSearchParams {
  const searchParams = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== null && value !== undefined && value !== '') {
      searchParams.set(key, value);
    }
  }

  return searchParams;
}

// ============================================
// API Endpoint Helper Functions
// ============================================

interface CommonQueryParams {
  range: string;
  granularity: string;
  startDate?: string | null;
  endDate?: string | null;
}

/**
 * Fetch revenue data
 */
export async function fetchRevenue(params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/revenue?${queryParams.toString()}`);
}

/**
 * Fetch revenue data for a specific provider
 */
export async function fetchRevenueByProvider(provider: string, params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/revenue/provider/${provider}?${queryParams.toString()}`);
}

/**
 * Fetch swap volume data
 */
export async function fetchSwapVolume(params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/swap-volume?${queryParams.toString()}`);
}

/**
 * Fetch swap volume data for a specific provider
 */
export async function fetchSwapVolumeByProvider(provider: string, params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/swap-volume/provider/${provider}?${queryParams.toString()}`);
}

/**
 * Fetch swap count data
 */
export async function fetchSwapCount(params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/swap-count?${queryParams.toString()}`);
}

/**
 * Fetch swap count data for a specific provider
 */
export async function fetchSwapCountByProvider(provider: string, params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/swap-count/provider/${provider}?${queryParams.toString()}`);
}

/**
 * Fetch users data
 */
export async function fetchUsers(params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/users?${queryParams.toString()}`);
}

/**
 * Fetch users data for a specific provider
 */
export async function fetchUsersByProvider(provider: string, params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/users/provider/${provider}?${queryParams.toString()}`);
}

/**
 * Fetch holders data
 */
export async function fetchHolders() {
  return fetchApi('/api/holders');
}

/**
 * Lookup a specific holder by address
 */
export async function fetchHolderLookup(address: string) {
  return fetchApi(`/api/holders/lookup?address=${encodeURIComponent(address)}`);
}

/**
 * Fetch referrals data
 */
export async function fetchReferrals(params: CommonQueryParams) {
  const queryParams = buildQueryParams({
    r: params.range,
    g: params.granularity,
    sd: params.startDate,
    ed: params.endDate,
  });
  return fetchApi(`/api/referrals?${queryParams.toString()}`);
}

/**
 * Fetch system status data
 */
export async function fetchSystemStatus() {
  return fetchApi('/api/system-status');
}
