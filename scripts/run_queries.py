#!/usr/bin/env python3
"""
Run SQL queries from queries.sql and display results.
"""

import sqlite3
import sys
import re

def run_queries(db_path='db/ecommerce.db', queries_file='queries.sql'):
    """Execute queries from file and display results."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    cursor = conn.cursor()
    
    with open(queries_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Split queries by semicolon, but preserve EXPLAIN QUERY PLAN blocks
    queries = []
    current_query = []
    in_explain = False
    
    for line in content.split('\n'):
        stripped = line.strip()
        
        # Skip comments and empty lines
        if not stripped or stripped.startswith('--'):
            if stripped.startswith('--') and 'Query' in stripped:
                # Start of a new query section
                if current_query:
                    queries.append('\n'.join(current_query))
                    current_query = []
            continue
        
        # Check for EXPLAIN QUERY PLAN
        if 'EXPLAIN QUERY PLAN' in line.upper():
            in_explain = True
            continue
        
        current_query.append(line)
        
        # If line ends with semicolon and we're not in EXPLAIN, it's end of query
        if line.rstrip().endswith(';') and not in_explain:
            query_text = '\n'.join(current_query).strip()
            if query_text:
                queries.append(query_text)
                current_query = []
            in_explain = False
    
    # Add any remaining query
    if current_query:
        queries.append('\n'.join(current_query))
    
    # Execute each query
    for i, query in enumerate(queries, 1):
        query = query.strip()
        if not query or query.endswith(';'):
            query = query.rstrip(';')
        
        if not query:
            continue
        
        try:
            print(f"\n{'='*80}")
            print(f"Query {i}")
            print('='*80)
            
            cursor.execute(query)
            
            # Fetch and display results
            rows = cursor.fetchall()
            if rows:
                # Get column names
                columns = [description[0] for description in cursor.description]
                
                # Print header
                print(' | '.join(columns))
                print('-' * (sum(len(c) for c in columns) + 3 * (len(columns) - 1)))
                
                # Print rows (limit to 10 for readability)
                for row in rows[:10]:
                    values = [str(val) if val is not None else 'NULL' for val in row]
                    print(' | '.join(values))
                
                if len(rows) > 10:
                    print(f"... ({len(rows) - 10} more rows)")
            else:
                print("(No rows returned)")
                
        except sqlite3.Error as e:
            print(f"Error executing query {i}: {e}")
            print(f"Query was: {query[:200]}...")
    
    conn.close()

if __name__ == '__main__':
    db_path = sys.argv[1] if len(sys.argv) > 1 else 'db/ecommerce.db'
    queries_file = sys.argv[2] if len(sys.argv) > 2 else 'queries.sql'
    run_queries(db_path, queries_file)


