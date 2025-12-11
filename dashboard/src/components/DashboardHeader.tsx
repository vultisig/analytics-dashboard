'use client';

import Image from 'next/image';
import Link from 'next/link';
import { usePathname } from 'next/navigation';

export function DashboardHeader() {
    const pathname = usePathname();

    const navItems = [
        { label: 'Overview', href: '/' },
        { label: 'Swap Volume', href: '/swap-volume' },
        { label: 'Users', href: '/users' },
        { label: 'Revenue', href: '/revenue' },
    ];

    return (
        <header className="sticky top-0 z-[100] w-full border-b border-slate-800 bg-[#0B1120]/80 backdrop-blur-md">
            <div className="container mx-auto flex h-16 items-center px-4 justify-between">
                {/* Logo Section */}
                <div className="flex items-center gap-3 w-[200px]">
                    <Link href="/" className="flex items-center gap-3 hover:opacity-80 transition-opacity">
                        <Image
                            src="/vultisig-logo.svg"
                            alt="Vultisig"
                            width={32}
                            height={32}
                            className="h-8 w-auto"
                        />
                        <span className="text-lg font-bold bg-gradient-to-r from-blue-400 to-cyan-300 bg-clip-text text-transparent hidden sm:block">
                            Analytics
                        </span>
                    </Link>
                </div>

                {/* Centered Tab Navigation */}
                <nav className="flex items-center justify-center flex-1">
                    <div className="flex items-center p-1 bg-slate-900/50 rounded-full border border-slate-800/50 backdrop-blur-sm">
                        {navItems.map((item) => {
                            const isActive = pathname === item.href;
                            return (
                                <Link
                                    key={item.href}
                                    href={item.href}
                                    className={`px-6 py-2 text-sm font-medium rounded-full transition-all duration-200 ${isActive
                                        ? 'bg-blue-600 text-white shadow-lg shadow-blue-900/20'
                                        : 'text-slate-400 hover:text-white hover:bg-slate-800'
                                        }`}
                                >
                                    {item.label}
                                </Link>
                            );
                        })}
                    </div>
                </nav>

                {/* Right Actions (Placeholder for balance) */}
                <div className="w-[200px] flex justify-end">
                    <a
                        href="https://vultisig.com"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-sm text-slate-400 hover:text-white transition-colors hidden sm:block"
                    >
                        vultisig.com ↗
                    </a>
                </div>
            </div>
        </header>
    );
}
