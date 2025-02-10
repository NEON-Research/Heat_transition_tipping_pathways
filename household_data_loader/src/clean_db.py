import sqlite3

print("Starting the script...")

# Connect to the SQLite database
db_path = 'large_files/households.db'
print(f"Connecting to the database at {db_path}...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
print("Connected to the database")

# Fetch the list of tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()

# Drop all tables except for 'households' and 'consolidated_households'
print("Dropping all tables except for 'households' and 'consolidated_households'...")
for table in tables:
    table_name = table[0]
    if table_name not in ['households', 'consolidated_households']:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Table {table_name} dropped successfully")
conn.commit()

# Calculate the percentage and absolute number of filled values in 'consolidated_households'
cursor.execute("PRAGMA table_info(consolidated_households);")
columns = cursor.fetchall()
total_columns = len(columns)

filled_counts = {}
for column in columns:
    col_name = column[1]
    cursor.execute(f"SELECT COUNT({col_name}) FROM consolidated_households WHERE {col_name} IS NOT NULL;")
    filled_counts[col_name] = cursor.fetchone()[0]

cursor.execute("SELECT COUNT(*) FROM consolidated_households;")
total_rows = cursor.fetchone()[0]

print(f"Total rows in consolidated_households: {total_rows}")
for col_name, filled_count in filled_counts.items():
    percentage_filled = (filled_count / total_rows) * 100 if total_rows > 0 else 0
    print(f"Column '{col_name}': {filled_count} filled values ({percentage_filled:.2f}%)")

# Close the connection
conn.close()
print("Connection closed")