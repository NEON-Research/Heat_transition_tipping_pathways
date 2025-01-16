import sqlite3
import time

print("Starting the script...")

# Connect to the SQLite database
db_path = 'large_files/households.db'
print(f"Connecting to the database at {db_path}...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
print("Connected to the database")

# Print the list of tables in the database
print("Fetching the list of tables...")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in the database:")
for table in tables:
    print(table[0])

# Create indexes to speed up the query
print("Creating indexes...")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_temp_energy_labels_BAGVerblijfsobjectID ON temp_energy_labels(BAGVerblijfsobjectID);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_households_vid ON households(vid);")
conn.commit()
print("Indexes created successfully")

# Analyze the database to optimize query performance
print("Analyzing the database...")
cursor.execute("ANALYZE;")
conn.commit()
print("Database analyzed successfully")

# Measure the time taken by the update query
start_time = time.time()

# Create a temporary table to store matching items
print("Creating temporary table for matching items...")
cursor.execute("DROP TABLE IF EXISTS temp_matching_items;")
cursor.execute("""
CREATE TEMP TABLE temp_matching_items AS
SELECT households.vid, temp_energy_labels.Energieklasse, temp_energy_labels.BerekendeEnergieverbruik
FROM households
JOIN temp_energy_labels
ON temp_energy_labels.BAGVerblijfsobjectID = households.vid
""")
conn.commit()
print("Temporary table created successfully")

# Get the total number of rows to update
cursor.execute("SELECT COUNT(*) FROM temp_matching_items")
total_rows_to_update = cursor.fetchone()[0]
print(f"Total rows to update: {total_rows_to_update}")

# Update the households table in batches
batch_size = 5000
print("Updating households table in batches...")
for offset in range(0, total_rows_to_update, batch_size):
    cursor.execute("BEGIN TRANSACTION;")
    update_query_batch = f"""
    UPDATE households
    SET energieklasse_EL = (
        SELECT Energieklasse
        FROM temp_matching_items
        WHERE temp_matching_items.vid = households.vid
    ),
    energiegebruik_EL = (
        SELECT BerekendeEnergieverbruik
        FROM temp_matching_items
        WHERE temp_matching_items.vid = households.vid
    )
    WHERE ROWID IN (
        SELECT ROWID
        FROM households
        WHERE EXISTS (
            SELECT 1
            FROM temp_matching_items
            WHERE temp_matching_items.vid = households.vid
        )
        LIMIT {batch_size} OFFSET {offset}
    )
    """
    cursor.execute(update_query_batch)
    cursor.execute("COMMIT;")
    print(f"Updated batch starting at offset {offset}")

end_time = time.time()
print(f"Time taken for the update query: {end_time - start_time} seconds")

# Verify the update
print("Verifying the update...")
verify_query = """
SELECT vid, energieklasse_EL, energiegebruik_EL
FROM households
WHERE energieklasse_EL IS NOT NULL OR energiegebruik_EL IS NOT NULL
LIMIT 10
"""
cursor.execute(verify_query)
updated_rows = cursor.fetchall()
print("\nUpdated rows in households table:")
for row in updated_rows:
    print(row)

# Vacuum the database to reclaim unused space
print("Vacuuming the database...")
cursor.execute("VACUUM;")
conn.commit()
print("Database vacuumed successfully")

# Close the connection
conn.close()
print("Connection closed")