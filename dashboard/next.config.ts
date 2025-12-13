import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // Standalone output for Docker deployments
  // Note: Vercel ignores this setting and uses its own optimized builds
  output: 'standalone',

  // Image optimization configuration
  images: {
    // Disable image optimization if not using next/image with external sources
    // unoptimized: true,

    // Remote patterns for external images (uncomment if needed)
    // remotePatterns: [
    //   {
    //     protocol: 'https',
    //     hostname: '**.example.com',
    //   },
    // ],
  },

  // Experimental features for performance
  experimental: {
    // Enable optimized package imports for smaller bundles
    optimizePackageImports: ['lucide-react', 'recharts', 'date-fns'],
  },

  // Environment variables validation at build time
  // These will be embedded at build time (NEXT_PUBLIC_*)
  env: {
    // Build timestamp for cache busting
    BUILD_TIME: new Date().toISOString(),
  },

  // Headers for security and caching
  async headers() {
    return [
      {
        source: '/:path*',
        headers: [
          {
            key: 'X-DNS-Prefetch-Control',
            value: 'on',
          },
          {
            key: 'X-Frame-Options',
            value: 'SAMEORIGIN',
          },
          {
            key: 'X-Content-Type-Options',
            value: 'nosniff',
          },
          {
            key: 'Referrer-Policy',
            value: 'origin-when-cross-origin',
          },
        ],
      },
      {
        // Cache static assets
        source: '/(.*)\\.(svg|png|jpg|jpeg|gif|ico|webp)',
        headers: [
          {
            key: 'Cache-Control',
            value: 'public, max-age=31536000, immutable',
          },
        ],
      },
    ];
  },

  // Rewrites can proxy API calls in development if needed
  // async rewrites() {
  //   return [
  //     {
  //       source: '/api/:path*',
  //       destination: `${process.env.NEXT_PUBLIC_API_URL}/api/:path*`,
  //     },
  //   ];
  // },

  // Disable x-powered-by header for security
  poweredByHeader: false,

  // Enable React strict mode for better development experience
  reactStrictMode: true,

  // Compiler options for production
  compiler: {
    // Remove console.log in production (optional)
    // removeConsole: process.env.NODE_ENV === 'production',
  },
};

export default nextConfig;
