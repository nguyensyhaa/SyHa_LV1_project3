import pandas as pd
from config.settings import MIN_PRICE, QUARANTINE_DIR, PROCESSED_DATA_DIR
import uuid

def clean_and_validate_products(data: list) -> pd.DataFrame:
    """
    Cleans product data and separates invalid rows (Dead Letter Queue).
    """
    df = pd.DataFrame(data)
    
    # 1. Vectorized Validation Logic
    # Condition A: Price must be positive
    valid_price = df['price'] > MIN_PRICE
    
    # Condition B: Name must not be Null
    valid_name = df['name'].notna()
    
    # Combined Validity Mask
    is_valid = valid_price & valid_name
    
    # 2. Split Data
    clean_df = df[is_valid].copy()
    garbage_df = df[~is_valid].copy()
    
    # 3. Handle Garbage (DLQ)
    if not garbage_df.empty:
        # Generate unique filename for this batch of garbage
        batch_id = uuid.uuid4()
        garbage_path = QUARANTINE_DIR / f"garbage_products_{batch_id}.csv"
        QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
        garbage_df.to_csv(garbage_path, index=False)
        print(f"[WARNING] quarantined {len(garbage_df)} rows to {garbage_path}")
        
    # 4. Standardize Clean Data
    if not clean_df.empty:
        # Example: Standardize name to Title Case
        clean_df['name'] = clean_df['name'].str.title()
        
    return clean_df
