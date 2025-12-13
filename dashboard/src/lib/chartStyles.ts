/**
 * Chart styling constants and color configurations.
 */

// Provider colors (ordered array for stacked charts)
export const providerColors = [
  '#00CED1', // Cyan - THORChain
  '#8B5CF6', // Purple - MAYAChain
  '#10B981', // Emerald - LI.FI
  '#F59E0B', // Amber - 1inch
  '#EF4444', // Red
  '#3B82F6', // Blue
  '#EC4899', // Pink
  '#14B8A6', // Teal
];

// Provider color map (by name)
export const providerColorMap: Record<string, string> = {
  thorchain: '#00CED1',
  mayachain: '#8B5CF6',
  lifi: '#10B981',
  '1inch': '#F59E0B',
};

// Chain/Platform color map
export const chainColorMap: Record<string, string> = {
  // Platforms
  android: '#3DDC84',
  ios: '#007AFF',
  web: '#FF6B35',
  other: '#64748B',

  // Common chains
  ethereum: '#627EEA',
  eth: '#627EEA',
  bitcoin: '#F7931A',
  btc: '#F7931A',
  avalanche: '#E84142',
  avax: '#E84142',
  polygon: '#8247E5',
  matic: '#8247E5',
  bsc: '#F3BA2F',
  bnb: '#F3BA2F',
  arbitrum: '#28A0F0',
  arb: '#28A0F0',
  optimism: '#FF0420',
  op: '#FF0420',
  base: '#0052FF',
  solana: '#00FFA3',
  sol: '#00FFA3',
  cosmos: '#2E3148',
  atom: '#2E3148',
  osmosis: '#750BBB',
  osmo: '#750BBB',
  thorchain: '#00CED1',
  rune: '#00CED1',
  mayachain: '#8B5CF6',
  maya: '#8B5CF6',
  cacao: '#8B5CF6',
  doge: '#C2A633',
  dogecoin: '#C2A633',
  ltc: '#345D9D',
  litecoin: '#345D9D',
  dash: '#008CE7',
  bch: '#8DC351',
  bitcoincash: '#8DC351',
};

// Fallback chain colors for unknown chains
export const fallbackChainColors = [
  '#6366F1', // Indigo
  '#8B5CF6', // Purple
  '#EC4899', // Pink
  '#EF4444', // Red
  '#F97316', // Orange
  '#EAB308', // Yellow
  '#22C55E', // Green
  '#14B8A6', // Teal
  '#06B6D4', // Cyan
  '#3B82F6', // Blue
];

// Glass tooltip style for charts
export const glassTooltipStyle = {
  backgroundColor: 'rgba(15, 23, 42, 0.9)',
  backdropFilter: 'blur(12px)',
  border: '1px solid rgba(255, 255, 255, 0.1)',
  borderRadius: '12px',
  boxShadow: '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06)',
  padding: '12px',
};

/**
 * Get color for a provider or chain
 */
export function getProviderColor(name: string, index: number = 0): string {
  const lowerName = name.toLowerCase();

  // Check provider map first
  if (providerColorMap[lowerName]) {
    return providerColorMap[lowerName];
  }

  // Check chain map
  if (chainColorMap[lowerName]) {
    return chainColorMap[lowerName];
  }

  // Fallback to indexed color
  return fallbackChainColors[index % fallbackChainColors.length];
}
