/**
 * Volume Extractor Module
 * Extracts swap volumes from blockchain transaction data.
 */

const { createPublicClient, http, parseAbiItem, decodeFunctionData } = require('viem');
const { mainnet, arbitrum, optimism, base, polygon, bsc, avalanche } = require('viem/chains');
const https = require('https');

// RPC Configuration - Use Alchemy for better coverage, fallback to public RPCs
const ALCHEMY_KEY = process.env.ALCHEMY_API_KEY;

const RPC_CONFIG = {
    ethereum: ALCHEMY_KEY
        ? [`https://eth-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, 'https://rpc.flashbots.net']
        : ['https://rpc.flashbots.net', 'https://eth.llamarpc.com'],
    arbitrum: ALCHEMY_KEY
        ? [`https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, 'https://arb1.arbitrum.io/rpc']
        : ['https://arb1.arbitrum.io/rpc', 'https://arbitrum.llamarpc.com'],
    optimism: ALCHEMY_KEY
        ? [`https://opt-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, 'https://mainnet.optimism.io']
        : ['https://mainnet.optimism.io', 'https://optimism.llamarpc.com'],
    base: ALCHEMY_KEY
        ? [`https://base-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, 'https://mainnet.base.org']
        : ['https://mainnet.base.org', 'https://base.llamarpc.com'],
    polygon: ALCHEMY_KEY
        ? [`https://polygon-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`, 'https://polygon-rpc.com']
        : ['https://polygon-rpc.com', 'https://polygon.llamarpc.com'],
    bsc: ['https://bsc-dataseed.binance.org', 'https://binance.llamarpc.com'],
    avalanche: ['https://api.avax.network/ext/bc/C/rpc', 'https://avalanche.llamarpc.com']
};

// Chain Mapping
const CHAIN_MAP = {
    'Ethereum': mainnet,
    'Arbitrum': arbitrum,
    'Optimism': optimism,
    'Base': base,
    'Polygon': polygon,
    'BSC': bsc,
    'Avalanche': avalanche
};

// Block Explorer API Configuration
// Note: All chains use the same Etherscan API key
// V2 API only available for Ethereum mainnet, others use V1
const EXPLORER_CONFIG = {
    'Ethereum': {
        url: 'https://api.etherscan.io/v2/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        chainId: 1,
        version: 'v2'
    },
    'Arbitrum': {
        url: 'https://api.arbiscan.io/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    },
    'Optimism': {
        url: 'https://api-optimistic.etherscan.io/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    },
    'Base': {
        url: 'https://api.basescan.org/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    },
    'Polygon': {
        url: 'https://api.polygonscan.com/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    },
    'BSC': {
        url: 'https://api.bscscan.com/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    },
    'Avalanche': {
        url: 'https://api.snowtrace.io/api',
        apiKey: process.env.ETHERSCAN_API_KEY,
        version: 'v1'
    }
};

// ABIs for common router functions
const ROUTER_ABIS = [
    // THORChain Router: depositWithExpiry(address vault, address asset, uint256 amount, string memo, uint256 expiration)
    'function depositWithExpiry(address vault, address asset, uint256 amount, string memo, uint256 expiration)',
    // 1inch: swap(address,(address,address,address,address,uint256,uint256,uint256),bytes)
    'function swap(address executor, (address srcToken, address dstToken, address srcReceiver, address dstReceiver, uint256 amount, uint256 minReturnAmount, uint256 flags) desc, bytes permit, bytes data)',
    // Uniswap V3: exactInput((bytes,address,uint256,uint256,uint256))
    'function exactInput((bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum) params)',
    // Uniswap V2 / Common: swapExactTokensForTokens
    'function swapExactTokensForTokens(uint256 amountIn, uint256 amountOutMin, address[] path, address to, uint256 deadline)'
];

class VolumeExtractor {
    constructor() {
        this.clients = {};
    }

    getClient(chainName) {
        if (this.clients[chainName]) return this.clients[chainName];

        const chain = CHAIN_MAP[chainName];
        const rpcs = RPC_CONFIG[chainName.toLowerCase()];

        if (!chain || !rpcs) {
            console.error(`Unsupported chain: ${chainName}`);
            return null;
        }

        // Create client with first RPC (simple for now, could add rotation)
        this.clients[chainName] = createPublicClient({
            chain,
            transport: http(rpcs[0])
        });

        return this.clients[chainName];
    }

    async getDecimals(client, tokenAddress) {
        if (tokenAddress === 'NATIVE' || tokenAddress === '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') return 18;
        try {
            const decimals = await client.readContract({
                address: tokenAddress,
                abi: [{
                    name: 'decimals',
                    type: 'function',
                    stateMutability: 'view',
                    inputs: [],
                    outputs: [{ type: 'uint8' }]
                }],
                functionName: 'decimals'
            });
            return decimals;
        } catch (e) {
            return 18; // Default
        }
    }

    async getTokenSymbol(client, tokenAddress) {
        if (tokenAddress === 'NATIVE' || tokenAddress === '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee') return 'NATIVE';
        try {
            const symbol = await client.readContract({
                address: tokenAddress,
                abi: [{
                    name: 'symbol',
                    type: 'function',
                    stateMutability: 'view',
                    inputs: [],
                    outputs: [{ type: 'string' }]
                }],
                functionName: 'symbol'
            });
            return symbol;
        } catch (e) {
            return null; // Unknown symbol
        }
    }

    async getInternalTransactionsAlchemy(txHash, chainName) {
        /**
         * Fetch internal transactions using Alchemy's alchemy_getAssetTransfers API.
         * This is more reliable than debug_traceTransaction and available on free tier.
         */
        if (!ALCHEMY_KEY) return null;

        // Map chain names to Alchemy network identifiers
        const alchemyNetworks = {
            'Ethereum': 'eth-mainnet',
            'Arbitrum': 'arb-mainnet',
            'Optimism': 'opt-mainnet',
            'Base': 'base-mainnet',
            'Polygon': 'polygon-mainnet'
        };

        const network = alchemyNetworks[chainName];
        if (!network) return null; // Chain not supported by Alchemy

        try {
            // First get transaction to find block number
            const client = this.getClient(chainName);
            const tx = await client.getTransaction({ hash: txHash });
            const blockNumber = tx.blockNumber;

            // Use Alchemy's alchemy_getAssetTransfers API
            const requestData = JSON.stringify({
                jsonrpc: '2.0',
                method: 'alchemy_getAssetTransfers',
                params: [{
                    fromBlock: `0x${blockNumber.toString(16)}`,
                    toBlock: `0x${blockNumber.toString(16)}`,
                    category: ['internal', 'external'],
                    withMetadata: true,
                    excludeZeroValue: true
                }],
                id: 1
            });

            const response = await new Promise((resolve, reject) => {
                const options = {
                    hostname: `${network}.g.alchemy.com`,
                    path: `/v2/${ALCHEMY_KEY}`,
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                        'Content-Length': requestData.length
                    }
                };

                https.request(options, (res) => {
                    let data = '';
                    res.on('data', (chunk) => data += chunk);
                    res.on('end', () => resolve(data));
                }).on('error', reject).end(requestData);
            });

            const json = JSON.parse(response);

            if (!json.result || !json.result.transfers) {
                return [];
            }

            // Filter transfers for this specific transaction and category 'internal'
            const internalTxs = json.result.transfers
                .filter(t => t.hash && t.hash.toLowerCase() === txHash.toLowerCase() && t.category === 'internal')
                .filter(t => t.value && parseFloat(t.value) > 0)
                .map(t => ({
                    from: t.from.toLowerCase(),
                    to: t.to ? t.to.toLowerCase() : null,
                    value: BigInt(Math.floor(parseFloat(t.value) * 1e18)), // Convert to wei
                    type: 'call'
                }))
                .filter(t => t.to !== null); // Remove transfers with no destination

            return internalTxs;

        } catch (err) {
            console.error(`Alchemy internal txs fetch failed for ${txHash}:`, err.message);
            return null; // Return null to indicate Alchemy failed, try fallback
        }
    }

    async getInternalTransactionsExplorer(txHash, chainName) {
        /**
         * Fallback: Fetch internal transactions using block explorer API.
         * Used when Alchemy is unavailable or unsupported.
         */
        const explorerConfig = EXPLORER_CONFIG[chainName];

        if (!explorerConfig || !explorerConfig.apiKey) {
            return [];
        }

        try {
            // Build URL based on API version
            let url;
            if (explorerConfig.version === 'v2') {
                url = `${explorerConfig.url}?chainid=${explorerConfig.chainId}&module=account&action=txlistinternal&txhash=${txHash}&apikey=${explorerConfig.apiKey}`;
            } else {
                // V1 API (no chainid parameter)
                url = `${explorerConfig.url}?module=account&action=txlistinternal&txhash=${txHash}&apikey=${explorerConfig.apiKey}`;
            }

            const response = await new Promise((resolve, reject) => {
                https.get(url, (res) => {
                    let data = '';
                    res.on('data', (chunk) => data += chunk);
                    res.on('end', () => resolve(data));
                }).on('error', reject);
            });

            const json = JSON.parse(response);

            if (json.status !== '1' || !Array.isArray(json.result)) {
                return [];
            }

            // Parse internal transactions from explorer API
            const internalTxs = json.result
                .filter(tx => tx.value && tx.value !== '0')
                .map(tx => ({
                    from: tx.from.toLowerCase(),
                    to: tx.to.toLowerCase(),
                    value: BigInt(tx.value),
                    type: tx.type || 'call'
                }));

            return internalTxs;

        } catch (err) {
            console.error(`Explorer internal txs fetch failed for ${txHash}:`, err.message);
            return [];
        }
    }

    async getInternalTransactions(txHash, chainName) {
        /**
         * Get internal transactions with Alchemy primary, block explorer fallback.
         */
        // Try Alchemy first (more reliable, better coverage)
        const alchemyResult = await this.getInternalTransactionsAlchemy(txHash, chainName);
        if (alchemyResult !== null) {
            return alchemyResult;
        }

        // Fallback to block explorer API
        return await this.getInternalTransactionsExplorer(txHash, chainName);
    }

    async getSwapFromLogs(txHash, chainName) {
        /**
         * Extract swap details from transaction logs by analyzing Transfer events AND internal transactions.
         * This works for any DEX transaction including 1inch.
         *
         * Strategy:
         * 1. Get transaction receipt with logs
         * 2. Get internal transactions (trace data) for native ETH transfers
         * 3. Find all Transfer events (ERC20) and internal ETH transfers
         * 4. Identify initiator (tx.from)
         * 5. Find token transfers OUT from initiator (token_in)
         * 6. Find token transfers TO initiator (token_out)
         *    - Check ERC20 Transfer events
         *    - Check internal ETH transfers
         */
        const client = this.getClient(chainName);
        if (!client) return null;

        try {
            // Get transaction, receipt, and internal transactions in parallel
            const [tx, receipt, internalTxs] = await Promise.all([
                client.getTransaction({ hash: txHash }),
                client.getTransactionReceipt({ hash: txHash }),
                this.getInternalTransactions(txHash, chainName)
            ]);

            const initiator = tx.from.toLowerCase();
            const FEE_ADDRESS = '0xa4a4f610e89488eb4ecc6c63069f241a54485269'; // Vultisig integrator

            // ERC20 Transfer event signature
            const TRANSFER_SIGNATURE = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef';

            let tokenIn = null;
            const transfers = []; // Store all transfers (ERC20 + native ETH) for analysis

            // 1. Analyze logs for ERC20 Transfer events
            for (const log of receipt.logs) {
                if (log.topics[0] === TRANSFER_SIGNATURE && log.topics.length >= 3) {
                    // Decode Transfer(address indexed from, address indexed to, uint256 value)
                    const from = '0x' + log.topics[1].slice(26).toLowerCase();
                    const to = '0x' + log.topics[2].slice(26).toLowerCase();
                    const value = BigInt(log.data);
                    const tokenAddress = log.address.toLowerCase();

                    // Token OUT from initiator (what they're selling)
                    if (from === initiator && !tokenIn) {
                        tokenIn = {
                            address: tokenAddress,
                            amount: value.toString()
                        };
                    }

                    // Store all ERC20 transfers for later analysis
                    transfers.push({
                        from,
                        to,
                        tokenAddress,
                        amount: value,
                        isNative: false
                    });
                }
            }

            // 2. Add internal native ETH transfers
            for (const internalTx of internalTxs) {
                // Native ETH sent FROM initiator (token_in candidate)
                if (internalTx.from === initiator && internalTx.value > 0n && !tokenIn) {
                    tokenIn = {
                        address: 'NATIVE',
                        amount: internalTx.value.toString()
                    };
                }

                // Store all internal ETH transfers for analysis
                transfers.push({
                    from: internalTx.from,
                    to: internalTx.to,
                    tokenAddress: 'NATIVE',
                    amount: internalTx.value,
                    isNative: true
                });
            }

            // 3. Check main transaction value for native token (ETH/BNB/etc) sent by initiator
            if (tx.value > 0n && !tokenIn) {
                tokenIn = {
                    address: 'NATIVE',
                    amount: tx.value.toString()
                };
            }

            if (!tokenIn) {
                return null; // No input token found
            }

            // 4. Find token_out using smart heuristics
            let tokenOut = null;

            // Priority 1: Transfer TO initiator of different token (ERC20 or native ETH)
            for (const transfer of transfers) {
                if (transfer.to === initiator &&
                    transfer.tokenAddress !== tokenIn.address) {
                    tokenOut = {
                        address: transfer.tokenAddress,
                        amount: transfer.amount.toString()
                    };
                    break;
                }
            }

            // Priority 2: Largest transfer of different token (not to fee address)
            if (!tokenOut) {
                let largestTransfer = null;
                let largestAmount = 0n;

                for (const transfer of transfers) {
                    // Skip fee payments and input token transfers
                    if (transfer.to === FEE_ADDRESS.toLowerCase() ||
                        transfer.tokenAddress === tokenIn.address) {
                        continue;
                    }

                    if (transfer.amount > largestAmount) {
                        largestAmount = transfer.amount;
                        largestTransfer = transfer;
                    }
                }

                if (largestTransfer) {
                    tokenOut = {
                        address: largestTransfer.tokenAddress,
                        amount: largestTransfer.amount.toString()
                    };
                }
            }

            // Return null if we didn't find output token
            if (!tokenOut) {
                return null;
            }

            // Fetch token metadata
            const [decimalsIn, symbolIn, symbolOut] = await Promise.all([
                this.getDecimals(client, tokenIn.address),
                this.getTokenSymbol(client, tokenIn.address),
                this.getTokenSymbol(client, tokenOut.address)
            ]);

            return {
                amount: tokenIn.amount,
                token: tokenIn.address,
                tokenSymbol: symbolIn,
                decimals: decimalsIn,
                tokenOut: tokenOut.address,
                tokenOutSymbol: symbolOut,
                amountOut: tokenOut.amount,
                type: 'swap_from_logs_and_trace'
            };

        } catch (err) {
            console.error(`Error extracting swap from logs for ${txHash}:`, err.message);
            return null;
        }
    }

    async getVolume(txHash, chainName) {
        const client = this.getClient(chainName);
        if (!client) return null;

        try {
            // First, try to extract swap from transaction logs (most reliable for 1inch)
            const swapFromLogs = await this.getSwapFromLogs(txHash, chainName);
            if (swapFromLogs) {
                return swapFromLogs;
            }

            // Fallback: Try decoding function calls (for direct router calls)
            const tx = await client.getTransaction({ hash: txHash });

            let result = null;

            // 1. Check Native Value (ETH/BNB/MATIC)
            if (tx.value > 0n) {
                result = {
                    amount: tx.value.toString(),
                    token: 'NATIVE',
                    type: 'native',
                    decimals: 18
                };
            } else {
                // 2. Decode Input Data
                for (const abi of ROUTER_ABIS) {
                    try {
                        const { functionName, args } = decodeFunctionData({
                            abi: parseAbiItem(abi),
                            data: tx.input
                        });

                        if (functionName === 'depositWithExpiry') {
                            result = {
                                amount: args[2].toString(),
                                token: args[1],
                                type: 'thorchain_deposit'
                            };
                        } else if (functionName === 'swap') {
                            const desc = args[1];
                            result = {
                                amount: desc.amount.toString(),
                                token: desc.srcToken,
                                tokenOut: desc.dstToken,
                                type: '1inch_swap'
                            };
                        } else if (functionName === 'exactInput') {
                            const params = args[0];
                            result = {
                                amount: params.amountIn.toString(),
                                token: 'UNKNOWN_FROM_PATH',
                                type: 'uniswap_v3'
                            };
                        }

                        if (result) break;
                    } catch (e) {
                        // Ignore decoding errors
                    }
                }
            }

            if (result) {
                // Fetch decimals and symbols for both tokens
                if (!result.decimals && result.token) {
                    result.decimals = await this.getDecimals(client, result.token);
                }

                // Fetch token symbols
                if (result.token) {
                    result.tokenSymbol = await this.getTokenSymbol(client, result.token);
                }
                if (result.tokenOut) {
                    result.tokenOutSymbol = await this.getTokenSymbol(client, result.tokenOut);
                }

                return result;
            }

            return null;

        } catch (err) {
            console.error(`Error extracting volume for ${txHash}:`, err.message);
            return null;
        }
    }
}

module.exports = VolumeExtractor;
