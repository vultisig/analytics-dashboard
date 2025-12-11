import logging
from main import SyncService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('latest_sync.log'),
        logging.StreamHandler()
    ]
)

if __name__ == "__main__":
    print("Starting one-off sync...")
    service = SyncService()
    service.sync_all_sources()
    print("Sync complete.")
