-- Fix varchar limits that are too restrictive
-- Some token symbols and addresses can be very long (scam tokens)

ALTER TABLE dex_aggregator_revenue 
  ALTER COLUMN fee_token_symbol TYPE VARCHAR(200),
  ALTER COLUMN token_in_symbol TYPE VARCHAR(200),
  ALTER COLUMN token_out_symbol TYPE VARCHAR(200),
  ALTER COLUMN fee_token_address TYPE VARCHAR(100),
  ALTER COLUMN token_in_address TYPE VARCHAR(100),
  ALTER COLUMN token_out_address TYPE VARCHAR(100);
