// Test what buildDateFilter generates
const { getDateRangeFromParams, calculateDateRange, formatDateForQuery } = require('./src/lib/dateUtils');

const testRange = { range: '30d' };
const range = getDateRangeFromParams(testRange);

console.log('Range:', range);

const { start, end } = calculateDateRange(range);
console.log('Calculated dates:', { start, end });

const params = [];
const conditions = [];
const dateColumn = 'date_only';

if (start) {
    conditions.push(`${dateColumn} >= $${params.length + 1}`);
    params.push(formatDateForQuery(start));
}

if (end && range.type !== 'all') {
    conditions.push(`${dateColumn} <= $${params.length + 1}`);
    params.push(formatDateForQuery(end));
}

const condition = conditions.length > 0 ? conditions.join(' AND ') : '1=1';

console.log('Condition:', condition);
console.log('Params:', params);

const arkhamCondition = condition.replace(/date_only/g, 'timestamp');
console.log('Arkham condition:', arkhamCondition);
