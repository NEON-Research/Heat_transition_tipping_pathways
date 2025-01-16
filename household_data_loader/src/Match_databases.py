import sqlite3
import time

# Connect to the SQLite database
db_path = 'large_files/households.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Measure the time taken by the update query
start_time = time.time()

# Update the households table with values from temp_energy_labels for matching items
batch_size = 10
update_query = """
UPDATE households
SET energieklasse_EL = (
    SELECT Energieklasse
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
),
energiegebruik_EL = (
    SELECT BerekendeEnergieverbruik
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
)
WHERE ROWID IN (
        SELECT ROWID
        FROM households
        WHERE EXISTS (
            SELECT 1
            FROM temp_energy_labels
            WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
        )
        LIMIT {batch_size}
    )
"""
cursor.execute(update_query)
conn.commit()

end_time = time.time()
print(f"Time taken for the update query: {end_time - start_time} seconds")

# Verify the update
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

# Close the connection
conn.close()


# Update a smaller subset of the households table
update_query_subset = """
UPDATE households
SET energieklasse_EL = (
    SELECT Energieklasse
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
),
energiegebruik_EL = (
    SELECT BerekendeEnergieverbruik
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
)
WHERE EXISTS (
    SELECT 1
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
)
LIMIT 1000
"""
cursor.execute(update_query_subset)
conn.commit()




# Get the total number of rows to update
cursor.execute("""
SELECT COUNT(*)
FROM households
WHERE EXISTS (
    SELECT 1
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
)
""")
total_rows_to_update = cursor.fetchone()[0]
print(f"Total rows to update: {total_rows_to_update}")

# Update in smaller batches
batch_size = 10
for offset in range(0, total_rows_to_update, batch_size):
    update_query_batch = f"""
    UPDATE households
    SET energieklasse_EL = (
        SELECT Energieklasse
        FROM temp_energy_labels
        WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
    ),
    energiegebruik_EL = (
        SELECT BerekendeEnergieverbruik
        FROM temp_energy_labels
        WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
    )
    WHERE ROWID IN (
        SELECT ROWID
        FROM households
        WHERE EXISTS (
            SELECT 1
            FROM temp_energy_labels
            WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
        )
        LIMIT {batch_size} OFFSET {offset}
    )
    """
    cursor.execute(update_query_batch)
    conn.commit()
    print(f"Updated batch starting at offset {offset}")

# Close the connection
conn.close()








### updated script
import sqlite3
import time

# Connect to the SQLite database
db_path = 'large_files/households.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Measure the time taken by the update query
start_time = time.time()

# Get the total number of rows to update
cursor.execute("""
SELECT COUNT(*)
FROM households
WHERE EXISTS (
    SELECT 1
    FROM temp_energy_labels
    WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
)
""")
total_rows_to_update = cursor.fetchone()[0]
print(f"Total rows to update: {total_rows_to_update}")

# Update in smaller batches
batch_size = 5
for offset in range(0, total_rows_to_update, batch_size):
    update_query_batch = f"""
    UPDATE households
    SET energieklasse_EL = (
        SELECT Energieklasse
        FROM temp_energy_labels
        WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
    ),
    energiegebruik_EL = (
        SELECT BerekendeEnergieverbruik
        FROM temp_energy_labels
        WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
    )
    WHERE ROWID IN (
        SELECT ROWID
        FROM households
        WHERE EXISTS (
            SELECT 1
            FROM temp_energy_labels
            WHERE temp_energy_labels.BAGVerblijfsobjectID = households.vid
        )
        LIMIT {batch_size} OFFSET {offset}
    )
    """
    cursor.execute(update_query_batch)
    conn.commit()
    print(f"Updated batch starting at offset {offset}")

end_time = time.time()
print(f"Time taken for the update query: {end_time - start_time} seconds")

# Verify the update
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

# Close the connection
conn.close()