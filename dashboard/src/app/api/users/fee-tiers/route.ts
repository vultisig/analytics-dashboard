import { NextResponse } from 'next/server';
import { feeTiers } from '../../_mock/seed';

export const dynamic = 'force-dynamic';

export function GET() {
    return NextResponse.json(feeTiers());
}
