import { NextResponse } from 'next/server';
import { holders } from '../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET() {
    return NextResponse.json(holders());
}
