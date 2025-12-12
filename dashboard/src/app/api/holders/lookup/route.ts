import { NextRequest, NextResponse } from 'next/server';
import pool from '@/lib/db';

export const dynamic = 'force-dynamic';

// In-memory rate limiting store
// In production, use Redis for distributed rate limiting
const rateLimitStore = new Map<string, { count: number; resetTime: number }>();

const RATE_LIMIT_WINDOW_MS = 60 * 1000; // 1 minute
const RATE_LIMIT_MAX_REQUESTS = 10; // 10 requests per minute per IP

function getRateLimitKey(request: NextRequest): string {
  // Get IP from various headers (Vercel, Cloudflare, etc.)
  const forwardedFor = request.headers.get('x-forwarded-for');
  const realIp = request.headers.get('x-real-ip');
  const cfIp = request.headers.get('cf-connecting-ip');

  return cfIp || realIp || forwardedFor?.split(',')[0]?.trim() || 'unknown';
}

function checkRateLimit(ip: string): { allowed: boolean; remaining: number; resetIn: number } {
  const now = Date.now();
  const record = rateLimitStore.get(ip);

  // Clean up old entries periodically
  if (rateLimitStore.size > 10000) {
    for (const [key, value] of rateLimitStore.entries()) {
      if (now > value.resetTime) {
        rateLimitStore.delete(key);
      }
    }
  }

  if (!record || now > record.resetTime) {
    // New window
    rateLimitStore.set(ip, { count: 1, resetTime: now + RATE_LIMIT_WINDOW_MS });
    return { allowed: true, remaining: RATE_LIMIT_MAX_REQUESTS - 1, resetIn: RATE_LIMIT_WINDOW_MS };
  }

  if (record.count >= RATE_LIMIT_MAX_REQUESTS) {
    return {
      allowed: false,
      remaining: 0,
      resetIn: record.resetTime - now
    };
  }

  record.count++;
  return {
    allowed: true,
    remaining: RATE_LIMIT_MAX_REQUESTS - record.count,
    resetIn: record.resetTime - now
  };
}

function isValidEthereumAddress(address: string): boolean {
  return /^0x[a-fA-F0-9]{40}$/.test(address);
}

export async function GET(request: NextRequest) {
  // Check rate limit
  const ip = getRateLimitKey(request);
  const rateLimit = checkRateLimit(ip);

  if (!rateLimit.allowed) {
    return NextResponse.json(
      {
        error: 'Rate limit exceeded',
        message: `Too many requests. Please try again in ${Math.ceil(rateLimit.resetIn / 1000)} seconds.`
      },
      {
        status: 429,
        headers: {
          'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
          'X-RateLimit-Remaining': '0',
          'X-RateLimit-Reset': Math.ceil(rateLimit.resetIn / 1000).toString(),
          'Retry-After': Math.ceil(rateLimit.resetIn / 1000).toString(),
        }
      }
    );
  }

  const searchParams = request.nextUrl.searchParams;
  const address = searchParams.get('address');

  // Validate address
  if (!address) {
    return NextResponse.json(
      { error: 'Missing address parameter' },
      {
        status: 400,
        headers: {
          'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
          'X-RateLimit-Remaining': rateLimit.remaining.toString(),
        }
      }
    );
  }

  if (!isValidEthereumAddress(address)) {
    return NextResponse.json(
      { error: 'Invalid Ethereum address format' },
      {
        status: 400,
        headers: {
          'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
          'X-RateLimit-Remaining': rateLimit.remaining.toString(),
        }
      }
    );
  }

  const client = await pool.connect();

  try {
    // Look up holder data
    const holderQuery = `
      SELECT
        address,
        vult_balance,
        has_thorguard,
        base_tier,
        effective_tier
      FROM vult_holders
      WHERE LOWER(address) = LOWER($1)
    `;

    const holderRes = await client.query(holderQuery, [address]);

    if (holderRes.rows.length === 0) {
      return NextResponse.json(
        {
          found: false,
          message: 'Address not found in holder list. This address may not hold any VULT tokens.'
        },
        {
          status: 200,
          headers: {
            'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
            'X-RateLimit-Remaining': rateLimit.remaining.toString(),
          }
        }
      );
    }

    const holder = holderRes.rows[0];

    // Get rank (position among all holders by balance)
    const rankQuery = `
      SELECT COUNT(*) + 1 as rank
      FROM vult_holders
      WHERE vult_balance > $1
    `;

    const rankRes = await client.query(rankQuery, [holder.vult_balance]);
    const rank = parseInt(rankRes.rows[0].rank);

    // Get total holder count
    const totalQuery = `SELECT COUNT(*) as total FROM vult_holders`;
    const totalRes = await client.query(totalQuery);
    const totalHolders = parseInt(totalRes.rows[0].total);

    // Tier discount mapping
    const tierDiscounts: Record<string, number> = {
      'None': 0,
      'Bronze': 5,
      'Silver': 10,
      'Gold': 20,
      'Platinum': 25,
      'Diamond': 35,
      'Ultimate': 50,
    };

    return NextResponse.json(
      {
        found: true,
        address: holder.address,
        vultBalance: parseFloat(holder.vult_balance),
        hasThorguard: holder.has_thorguard,
        baseTier: holder.base_tier,
        effectiveTier: holder.effective_tier,
        discount: tierDiscounts[holder.effective_tier] || 0,
        rank,
        totalHolders,
      },
      {
        status: 200,
        headers: {
          'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
          'X-RateLimit-Remaining': rateLimit.remaining.toString(),
        }
      }
    );

  } catch (error) {
    console.error('=== Holders Lookup API Error ===');
    console.error('Error message:', error instanceof Error ? error.message : String(error));
    console.error('Stack trace:', error instanceof Error ? error.stack : 'No stack trace');
    return NextResponse.json(
      { error: 'Failed to lookup holder data' },
      {
        status: 500,
        headers: {
          'X-RateLimit-Limit': RATE_LIMIT_MAX_REQUESTS.toString(),
          'X-RateLimit-Remaining': rateLimit.remaining.toString(),
        }
      }
    );
  } finally {
    client.release();
  }
}
