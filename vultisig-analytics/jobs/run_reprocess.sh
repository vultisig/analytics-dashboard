#!/bin/bash
# Cron job wrapper for reprocessing errors
# Add to crontab: */30 * * * * /path/to/run_reprocess.sh

cd /Users/cy/Documents/Projects/vultisig-tc-affiliate-fee/vultisig-analytics
source .env
python3 jobs/reprocess_errors.py >> logs/reprocess_$(date +\%Y\%m\%d).log 2>&1
