/**
 * Provider-related utility functions.
 */

// Provider display names
const PROVIDER_DISPLAY_NAMES: Record<string, string> = {
  thorchain: 'THORChain',
  mayachain: 'MAYAChain',
  lifi: 'LI.FI',
  '1inch': '1inch',
};

// Preferred provider order for sorting
const PROVIDER_ORDER: string[] = ['thorchain', 'mayachain', 'lifi', '1inch'];

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
