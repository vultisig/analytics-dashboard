-- Add latest_data_timestamp column to sync_status table
-- This tracks the timestamp of the most recent transaction from each source

ALTER TABLE sync_status
ADD COLUMN IF NOT EXISTS latest_data_timestamp TIMESTAMP WITH TIME ZONE;

-- Populate initial values from existing data
UPDATE sync_status s
SET latest_data_timestamp = (
    SELECT MAX(timestamp)
    FROM swaps
    WHERE source = s.source
)
WHERE latest_data_timestamp IS NULL;

-- For Arkham (which uses dex_aggregator_revenue table)
UPDATE sync_status s
SET latest_data_timestamp = (
    SELECT MAX(timestamp)
    FROM dex_aggregator_revenue
    WHERE fee_data_source = 'arkham'
)
WHERE s.source = 'arkham' AND latest_data_timestamp IS NULL;

COMMENT ON COLUMN sync_status.latest_data_timestamp IS 'Timestamp of the most recent transaction/swap from this source (different from last_synced_timestamp which is when the sync service last ran)';
