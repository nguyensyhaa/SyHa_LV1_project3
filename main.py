import argparse
import time
import sys
from config.settings import USE_MULTIPROCESSING
from src.extract.parallel import generate_data_parallel
from src.extract.generator import generate_products
from src.transform.cleaner import clean_and_validate_products
from src.load.loader import bulk_load_products, create_schema

def run_pipeline(total_rows=100_000, skip_db=False):
    print(f"--- 🚀 STARTING PIPELINE (N={total_rows}) ---")
    start_time = time.time()
    
    # 1. EXTRACT
    t0 = time.time()
    if USE_MULTIPROCESSING:
        print(f"[EXTRACT] Generating with Multiprocessing...")
        raw_data = generate_data_parallel(total_rows)
    else:
        print(f"[EXTRACT] Generating Single-Core...")
        raw_data = generate_products(total_rows)
    t1 = time.time()
    print(f"[EXTRACT] Completed in {t1-t0:.2f}s. Rows: {len(raw_data)}")
    
    # 2. TRANSFORM
    print(f"[TRANSFORM] Cleaning & Validating...")
    clean_df = clean_and_validate_products(raw_data)
    t2 = time.time()
    print(f"[TRANSFORM] Completed in {t2-t1:.2f}s. Valid Rows: {len(clean_df)}")
    
    # 3. LOAD
    if not skip_db:
        print(f"[LOAD] Inserting into Postgres...")
        try:
            create_schema()
            bulk_load_products(clean_df)
            t3 = time.time()
            print(f"[LOAD] Completed in {t3-t2:.2f}s")
        except Exception as e:
            print(f"[LOAD] SKIPPED/FAILED (Check DB Config): {e}")
            t3 = time.time()
    else:
        print("[LOAD] Skipped (Dry Run Mode)")
        t3 = time.time()
        
    total_time = t3 - start_time
    print(f"--- ✅ PIPELINE FINISHED in {total_time:.2f}s ---")

if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--rows", type=int, default=100000)
    # parser.add_argument("--dry-run", action="store_true")
    # args = parser.parse_args()
    
    # For demo purposes, defaulting to dry-run logic if DB fails
    try:
        run_pipeline(total_rows=100_000, skip_db=False)
    except KeyboardInterrupt:
        print("Stopped by user.")
