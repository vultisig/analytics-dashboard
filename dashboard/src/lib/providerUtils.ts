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

// Dex-revenue providers have chain attribution, not platform.
export const ARKHAM_PROVIDERS: readonly string[] = ['1inch', 'kyberswap'];
export const SWAPKIT_PROTOCOL = 'swapkit';
export const DEX_REVENUE_PROVIDERS: readonly string[] = [...ARKHAM_PROVIDERS, SWAPKIT_PROTOCOL];

export function isDexRevenueProvider(name: string): boolean {
  return DEX_REVENUE_PROVIDERS.includes(name.toLowerCase());
}

export const SWAPKIT_COVERAGE_TOOLTIP =
  'Volume: Chainflip, measured per app (the split shows on the platform charts, not on this card). SwapKit EVM swaps are counted but their volume is unknown (fee rates vary). Near volume needs partner credentials and has no platform. Not covered: Flashnet, Jupiter, Garden, Harbor, Mayan. Revenue: fee-wallet receipts, dated at payout, not per provider. THOR/Maya via SwapKit is already counted separately.';

// Preferred provider order for sorting
const PROVIDER_ORDER: string[] = ['thorchain', 'mayachain', 'lifi', '1inch', 'kyberswap', SWAPKIT_PROTOCOL];

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
