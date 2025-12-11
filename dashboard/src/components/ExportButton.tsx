'use client';

import { useState } from 'react';
import { Download, FileText, Image } from 'lucide-react';

interface ExportButtonProps {
    onExportCSV: () => void;
    onExportPNG: () => void;
    disabled?: boolean;
}

export function ExportButton({ onExportCSV, onExportPNG, disabled = false }: ExportButtonProps) {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <div className="relative">
            <button
                type="button"
                onClick={() => setIsOpen(!isOpen)}
                disabled={disabled}
                className={`
                    flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm font-medium
                    transition-all
                    ${disabled
                        ? 'bg-slate-800 text-slate-500 cursor-not-allowed'
                        : 'glass-card glass-card-hover text-cyan-400 hover:text-cyan-300'
                    }
                `}
                aria-label="Export chart data"
                aria-expanded={isOpen}
            >
                <Download className="w-4 h-4" />
                <span>Export</span>
            </button>

            {isOpen && !disabled && (
                <>
                    {/* Backdrop to close dropdown */}
                    <div
                        className="fixed inset-0 z-10"
                        onClick={() => setIsOpen(false)}
                        aria-hidden="true"
                    />

                    {/* Dropdown menu */}
                    <div className="absolute right-0 top-full mt-2 z-20 w-48 glass-card rounded-xl shadow-xl overflow-hidden">
                        <button
                            type="button"
                            onClick={() => {
                                onExportCSV();
                                setIsOpen(false);
                            }}
                            className="w-full flex items-center gap-3 px-4 py-3 text-sm text-slate-300 hover:bg-white/5 hover:text-white transition-colors"
                            aria-label="Export as CSV"
                        >
                            <FileText className="w-4 h-4 text-emerald-400" />
                            <div className="text-left">
                                <div className="font-medium">Export as CSV</div>
                                <div className="text-xs text-slate-500">Download data table</div>
                            </div>
                        </button>

                        <div className="border-t border-slate-700/50" />

                        <button
                            type="button"
                            onClick={() => {
                                onExportPNG();
                                setIsOpen(false);
                            }}
                            className="w-full flex items-center gap-3 px-4 py-3 text-sm text-slate-300 hover:bg-white/5 hover:text-white transition-colors"
                            aria-label="Export as PNG"
                        >
                            <Image className="w-4 h-4 text-cyan-400" />
                            <div className="text-left">
                                <div className="font-medium">Export as PNG</div>
                                <div className="text-xs text-slate-500">Download chart image</div>
                            </div>
                        </button>
                    </div>
                </>
            )}
        </div>
    );
}
