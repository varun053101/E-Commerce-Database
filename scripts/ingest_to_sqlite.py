#!/usr/bin/env python3
"""
Ingest CSV files into SQLite database with proper schema, constraints, and validation.
"""

import os
import sys
import sqlite3
import csv
from decimal import Decimal
from datetime import datetime

# CLI argument parsing
DRY_RUN = '--dry-run' in sys.argv

# Database path
DB_PATH = 'db/ecommerce.db'

# Create db directory if it doesn't exist
os.makedirs('db', exist_ok=True)

def create_schema(conn):
    """Create database schema with proper types, constraints, and indexes."""
    cursor = conn.cursor()
    
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON")
    
    # Drop existing tables if they exist (for clean rebuild)
    if not DRY_RUN:
        cursor.execute("DROP TABLE IF EXISTS reviews")
        cursor.execute("DROP TABLE IF EXISTS order_items")
        cursor.execute("DROP TABLE IF EXISTS orders")
        cursor.execute("DROP TABLE IF EXISTS products")
        cursor.execute("DROP TABLE IF EXISTS customers")
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            signup_date TEXT NOT NULL,
            country TEXT NOT NULL,
            is_premium TEXT NOT NULL
        )
    """)
    
    # Create products table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price NUMERIC(10, 2) NOT NULL,
            cost NUMERIC(10, 2) NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    
    # Create orders table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            customer_id TEXT NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            total_amount NUMERIC(10, 2) NOT NULL,
            shipping_country TEXT NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # Create order_items table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price NUMERIC(10, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)
    
    # Create reviews table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            review_id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            customer_id TEXT NOT NULL,
            rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
            review_text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    # Create indexes for performance
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_reviews_customer_id ON reviews(customer_id)")
    
    conn.commit()
    print("Schema created successfully")

def load_csv_to_table(conn, csv_path, table_name, expected_columns):
    """Load CSV file into SQLite table using parameterized queries."""
    cursor = conn.cursor()
    
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    row_count = 0
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        # Validate columns
        actual_columns = set(reader.fieldnames)
        expected_set = set(expected_columns)
        if actual_columns != expected_set:
            missing = expected_set - actual_columns
            extra = actual_columns - expected_set
            if missing:
                raise ValueError(f"Missing columns in {csv_path}: {missing}")
            if extra:
                raise ValueError(f"Extra columns in {csv_path}: {extra}")
        
        # Prepare insert statement
        placeholders = ','.join(['?' for _ in expected_columns])
        columns_str = ','.join(expected_columns)
        insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        rows_to_insert = []
        for row in reader:
            values = [row[col] for col in expected_columns]
            rows_to_insert.append(values)
            row_count += 1
        
        if not DRY_RUN:
            cursor.executemany(insert_sql, rows_to_insert)
            conn.commit()
    
    return row_count

def fix_order_totals(conn):
    """Calculate and update order totals from order_items, return count of fixes."""
    cursor = conn.cursor()
    
    # Calculate correct totals from order_items
    cursor.execute("""
        SELECT 
            o.order_id,
            o.total_amount as current_total,
            COALESCE(SUM(oi.quantity * oi.unit_price), 0) as calculated_total
        FROM orders o
        LEFT JOIN order_items oi ON o.order_id = oi.order_id
        GROUP BY o.order_id, o.total_amount
        HAVING ABS(o.total_amount - COALESCE(SUM(oi.quantity * oi.unit_price), 0)) > 0.01
    """)
    
    mismatches = cursor.fetchall()
    fix_count = 0
    
    if mismatches and not DRY_RUN:
        for order_id, current_total, calculated_total in mismatches:
            cursor.execute("""
                UPDATE orders 
                SET total_amount = ROUND(?, 2)
                WHERE order_id = ?
            """, (float(calculated_total), order_id))
            fix_count += 1
        conn.commit()
    
    return len(mismatches), fix_count

def validate_foreign_keys(conn):
    """Check for foreign key violations."""
    cursor = conn.cursor()
    
    violations = []
    
    # Check orders.customer_id references
    cursor.execute("""
        SELECT COUNT(*) FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    invalid_customer_refs = cursor.fetchone()[0]
    if invalid_customer_refs > 0:
        violations.append(('orders.customer_id', invalid_customer_refs))
    
    # Check order_items.order_id references
    cursor.execute("""
        SELECT COUNT(*) FROM order_items oi
        LEFT JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_id IS NULL
    """)
    invalid_order_refs = cursor.fetchone()[0]
    if invalid_order_refs > 0:
        violations.append(('order_items.order_id', invalid_order_refs))
    
    # Check order_items.product_id references
    cursor.execute("""
        SELECT COUNT(*) FROM order_items oi
        LEFT JOIN products p ON oi.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)
    invalid_product_refs = cursor.fetchone()[0]
    if invalid_product_refs > 0:
        violations.append(('order_items.product_id', invalid_product_refs))
    
    # Check reviews.product_id references
    cursor.execute("""
        SELECT COUNT(*) FROM reviews r
        LEFT JOIN products p ON r.product_id = p.product_id
        WHERE p.product_id IS NULL
    """)
    invalid_review_product_refs = cursor.fetchone()[0]
    if invalid_review_product_refs > 0:
        violations.append(('reviews.product_id', invalid_review_product_refs))
    
    # Check reviews.customer_id references
    cursor.execute("""
        SELECT COUNT(*) FROM reviews r
        LEFT JOIN customers c ON r.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """)
    invalid_review_customer_refs = cursor.fetchone()[0]
    if invalid_review_customer_refs > 0:
        violations.append(('reviews.customer_id', invalid_review_customer_refs))
    
    return violations

def generate_integrity_report(conn):
    """Generate and print integrity report."""
    cursor = conn.cursor()
    
    print("\n=== Integrity Report ===")
    
    # Row counts per table
    tables = ['customers', 'products', 'orders', 'order_items', 'reviews']
    print("\nRow counts per table:")
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count}")
    
    # NULL counts in important columns
    print("\nNULL counts in important columns:")
    cursor.execute("SELECT COUNT(*) FROM orders WHERE customer_id IS NULL")
    null_customers = cursor.fetchone()[0]
    print(f"  orders.customer_id: {null_customers}")
    
    cursor.execute("SELECT COUNT(*) FROM order_items WHERE order_id IS NULL OR product_id IS NULL")
    null_order_items = cursor.fetchone()[0]
    print(f"  order_items (order_id or product_id): {null_order_items}")
    
    cursor.execute("SELECT COUNT(*) FROM reviews WHERE product_id IS NULL OR customer_id IS NULL")
    null_reviews = cursor.fetchone()[0]
    print(f"  reviews (product_id or customer_id): {null_reviews}")
    
    # Foreign key violations
    print("\nForeign key violations:")
    violations = validate_foreign_keys(conn)
    if violations:
        for column, count in violations:
            print(f"  {column}: {count} violations")
    else:
        print("  None detected")
    
    # Order total mismatches
    print("\nOrder total validation:")
    mismatch_count, fix_count = fix_order_totals(conn)
    if mismatch_count > 0:
        print(f"  Found {mismatch_count} mismatches")
        if not DRY_RUN:
            print(f"  Fixed {fix_count} mismatches")
    else:
        print("  All order totals match order_items")

def main():
    """Main ingestion function."""
    if DRY_RUN:
        print("=== DRY RUN MODE ===")
        print("Validating without writing to database\n")
    
    # Connect to database
    if DRY_RUN:
        # Use in-memory database for dry run
        conn = sqlite3.connect(':memory:')
    else:
        # Remove existing database for clean rebuild
        if os.path.exists(DB_PATH):
            os.remove(DB_PATH)
        conn = sqlite3.connect(DB_PATH)
    
    try:
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Create schema
        create_schema(conn)
        
        # Load data in correct order (respecting foreign key dependencies)
        print("\nLoading CSV files...")
        
        print("  Loading customers.csv...")
        load_csv_to_table(conn, 'data/customers.csv', 'customers',
                         ['customer_id', 'name', 'email', 'signup_date', 'country', 'is_premium'])
        
        print("  Loading products.csv...")
        load_csv_to_table(conn, 'data/products.csv', 'products',
                         ['product_id', 'sku', 'name', 'category', 'price', 'cost', 'created_at'])
        
        print("  Loading orders.csv...")
        load_csv_to_table(conn, 'data/orders.csv', 'orders',
                         ['order_id', 'customer_id', 'order_date', 'status', 'total_amount', 'shipping_country'])
        
        print("  Loading order_items.csv...")
        load_csv_to_table(conn, 'data/order_items.csv', 'order_items',
                         ['order_item_id', 'order_id', 'product_id', 'quantity', 'unit_price'])
        
        print("  Loading reviews.csv...")
        load_csv_to_table(conn, 'data/reviews.csv', 'reviews',
                         ['review_id', 'product_id', 'customer_id', 'rating', 'review_text', 'created_at'])
        
        # Generate integrity report
        generate_integrity_report(conn)
        
        if not DRY_RUN:
            print(f"\nDatabase created successfully at {DB_PATH}")
        else:
            print("\nDry run completed - no database file created")
        
    except Exception as e:
        print(f"\nError during ingestion: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == '__main__':
    main()


