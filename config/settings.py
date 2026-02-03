import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
QUARANTINE_DIR = DATA_DIR / "quarantine"

# Constraints (Externalized)
MIN_PRICE = 0
MAX_NAME_LENGTH = 100
MIN_RATING = 0
MAX_RATING = 5

# Feature Flags
USE_MULTIPROCESSING = True
CHUNK_SIZE = 25000  # Rows per core
CPU_CORES = 4  # Adjust based on system

# Database
DB_CONFIG = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD", "password") # Load from env, default for dev
}
