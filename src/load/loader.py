import io
import psycopg2
from config.settings import DB_CONFIG

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def bulk_load_products(df, table_name="products"):
    """
    Loads DataFrame to PostgreSQL using COPY protocol for high speed.
    """
    if df.empty:
        print("No data to load.")
        return

    conn = get_connection()
    cursor = conn.cursor()
    
    # Create Buffer
    csv_buffer = io.StringIO()
    # Write to buffer (no header, no index)
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)
    
    try:
        # Fast COPY
        cursor.copy_expert(
            f"COPY {table_name} (product_id, name, price, category, created_at) FROM STDIN WITH CSV",
            csv_buffer
        )
        conn.commit()
        print(f"[SUCCESS] Loaded {len(df)} rows into {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Bulk load failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        
def create_schema():
    """
    Creates the necessary tables.
    """
    schema_sql = """
    CREATE TABLE IF NOT EXISTS products (
        product_id UUID PRIMARY KEY,
        name VARCHAR(255),
        price DECIMAL(10, 2),
        category VARCHAR(100),
        created_at TIMESTAMP
    );
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("Schema ensured.")
    except Exception as e:
        print(f"Schema creation failed: {e}")
    finally:
        conn.close()
