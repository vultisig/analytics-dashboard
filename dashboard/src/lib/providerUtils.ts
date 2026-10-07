/**
 * Provider-related utility functions.
 */

// Provider display names
const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  thorchain: 'THORChain',
  mayachain: 'MAYAChain',
  lifi: 'LI.FI',
  '1inch': '1inch',
  kyberswap: 'KyberSwap',
  swapkit: 'SwapKit',
};

// Providers fed by the shared Arkham ingestor (rows live in
// dex_aggregator_revenue, not the `swaps` table). Mirrors the backend
// `config.ARKHAM_PROVIDERS` tuple.
export const ARKHAM_PROVIDERS: readonly string[] = ['1inch', 'kyberswap'];

export function isArkhamProvider(name: string): boolean {
  return ARKHAM_PROVIDERS.includes(name.toLowerCase());
}

// Preferred provider order for sorting
const PROVIDER_ORDER: string[] = ['thorchain', 'mayachain', 'lifi', '1inch', 'kyberswap'];

/**
 * Format a provider name for display.
 * @param name - The raw provider name
 * @returns Formatted display name
 */
export function formatProviderName(name: string): string {
  if (!name) return 'Unknown';

  const lowerName = name.toLowerCase();

  // Check for known provider names
  if (PROVIDER_DISPLAY_NAMES[lowerName]) {
    return PROVIDER_DISPLAY_NAMES[lowerName];
  }

  // Capitalize first letter of each word for unknown names
  return name
    .split(/[\s_-]+/)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
    .join(' ');
}

/**
 * Sort providers in preferred order.
 * Known providers come first in their preferred order, then unknown providers alphabetically.
 * @param providers - Array of provider names
 * @returns Sorted array of provider names
 */
export function sortProviders(providers: string[]): string[] {
  return [...providers].sort((a, b) => {
    const aLower = a.toLowerCase();
    const bLower = b.toLowerCase();

    const aIndex = PROVIDER_ORDER.indexOf(aLower);
    const bIndex = PROVIDER_ORDER.indexOf(bLower);

    // Both are known providers - sort by preferred order
    if (aIndex !== -1 && bIndex !== -1) {
      return aIndex - bIndex;
    }

    // Only a is known - a comes first
    if (aIndex !== -1) return -1;

    // Only b is known - b comes first
    if (bIndex !== -1) return 1;

    // Both unknown - sort alphabetically
    return aLower.localeCompare(bLower);
  });
}

/**
 * Check if a provider is a known provider.
 * @param name - The provider name
 * @returns True if the provider is known
 */
export function isKnownProvider(name: string): boolean {
  return PROVIDER_ORDER.includes(name.toLowerCase());
}

/**
 * Get all known providers.
 * @returns Array of known provider names
 */
export function getKnownProviders(): string[] {
  return [...PROVIDER_ORDER];
}

/** True for the swapkit provider name, in any letter case. */
export function isSwapkitProvider(name: string): boolean {
  return name.toLowerCase() === 'swapkit';
}

// Feature flag for the SwapKit earned charts. Off unless set to '1' at build time.
export const SWAPKIT_EARNED_ENABLED = process.env.NEXT_PUBLIC_SWAPKIT_EARNED === '1';

// Explains how "earned" differs from "received" revenue.
export const SWAPKIT_EARNED_TOOLTIP =
  'Earned: revenue and volume as reported by SwapKit, dated by the day of the swap. ' +
  'Received: fee-wallet receipts, dated when the payout arrives. ' +
  'The two measures differ in timing and can differ in total.';

/**
 * Label for a provider returned by an API: the known display name, else the raw name.
 */
export function providerLabel(name: string): string {
  return PROVIDER_DISPLAY_NAMES[name.toLowerCase()] ?? name;
}
