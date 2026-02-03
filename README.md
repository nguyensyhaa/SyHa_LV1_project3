# SyHa Data Engineering Pipeline

## Overview
This project implements a high-performance ETL (Extract – Transform – Load) pipeline for an E-Commerce system using Python. It addresses common bottlenecks in synthetic data generation and database loading.
- Extracts fake data using Multiprocessing (Parallel CPU cores).
- Transforms and cleans data using Pandas with a Dead Letter Queue strategy.
- Loads data into PostgreSQL using the binary COPY protocol.

### 1. Table: `products` (Clean Data)
This table stores valid product data ready for analysis.

| Column Name | Data Type | Constraint | Description |
| :--- | :--- | :--- | :--- |
| `product_id` | UUID | **PK** | Unique identifier for each product |
| `name` | VARCHAR(255) | NOT NULL | Name of the product (Cleaned, Title Case) |
| `price` | DECIMAL(10, 2) |CHECK (> 0) | Product price (Must be positive) |
| `category` | VARCHAR(100) | - | Product category |
| `created_at` | TIMESTAMP | - | Record creation timestamp |

### 2. Table: `quarantine_data` (Dirty Data)
This table (or CSV folder) captures data rejected by validation rules.

| Column Name | Data Type | Description |
| :--- | :--- | :--- |
| `product_id` | UUID | ID of the rejected record |
| `name` | VARCHAR | Original name (may be null) |
| `price` | DECIMAL | Invalid price (e.g., negative values) |
| `error_reason`| VARCHAR | **Why it failed** (e.g. "Negative Price", "Missing Name") |

## ETL Pipeline

1. **Extract**
   - Generates synthetic E-Commerce data using Faker.
   - Utilizes `multiprocessing` to run on multiple CPU cores in parallel (~16x speedup).

2. **Transform**
   - Cleans and validates data using Pandas and Vectorized operations.
   - **Dead Letter Queue:** Instead of deleting bad data, invalid rows (e.g., negative price) are separated and saved to `data/quarantine/` for inspection.

3. **Load**
   - Connects to PostgreSQL using `psycopg2`.
   - Uses `COPY ... FROM STDIN` for bulk insertion, significantly faster than standard `INSERT`.

## Environment Variables

Create a `.env` file or configure `config/settings.py`:

```
DB_HOST=localhost
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=password
```

## Dependency Management (Poetry)

```bash
# Install dependencies
poetry install

# processing pipeline
poetry run python main.py
```

## Conclusion
This project demonstrates a production-grade approach to handling "garbage" data and optimizing performance for large-scale data ingestion tasks.
