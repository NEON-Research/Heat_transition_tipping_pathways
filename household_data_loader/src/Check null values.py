import sqlite3
from collections import defaultdict

db_path = "households.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get the table names
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()


for table in tables:
    table_name = table[0]
    print(f"First 10 rows of table: {table_name}")
    
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)

conn.close()
