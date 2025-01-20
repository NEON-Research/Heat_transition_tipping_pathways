import sqlite3
import time

print("Starting the script...")

# Connect to the SQLite database
db_path = 'large_files/households.db'
print(f"Connecting to the database at {db_path}...")
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
print("Connected to the database")

# Fetch the list of tables in the database
print("Fetching the list of tables...")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
table_names = [table[0] for table in tables]
print("Tables in the database:")
for table in table_names:
    print(table)

# Fetch all unique Postcode3 values from the households table
print("Fetching unique Postcode3 values from households table...")
cursor.execute("SELECT DISTINCT Postcode3 FROM households WHERE Postcode3 IS NOT NULL;")
unique_postcode3_values = cursor.fetchall()
unique_postcode3_values = [row[0] for row in unique_postcode3_values if 101 <= row[0] <= 999]
print(f"Unique Postcode3 values: {unique_postcode3_values}")

# Create subtables for each unique Postcode3 value if they do not already exist
print("Creating subtables for each unique Postcode3 value if they do not already exist...")
for postcode3 in unique_postcode3_values:
    if f"temp_energy_labels_{postcode3}" not in table_names:
        cursor.execute(f"""
            CREATE TABLE temp_energy_labels_{postcode3} AS
            SELECT * FROM temp_energy_labels WHERE Postcode3 = {postcode3};
        """)
        print(f"Subtable temp_energy_labels_{postcode3} created successfully")
    else:
        print(f"Subtable temp_energy_labels_{postcode3} already exists, skipping creation")

    if f"households_{postcode3}" not in table_names:
        cursor.execute(f"""
            CREATE TABLE households_{postcode3} AS
            SELECT * FROM households WHERE Postcode3 = {postcode3};
        """)
        print(f"Subtable households_{postcode3} created successfully")
    else:
        print(f"Subtable households_{postcode3} already exists, skipping creation")
    conn.commit()

# Function to print the size of each subtable
def print_subtable_sizes():
    for postcode3 in unique_postcode3_values:
        cursor.execute(f"SELECT COUNT(*) FROM households_{postcode3};")
        households_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM temp_energy_labels_{postcode3};")
        temp_energy_labels_count = cursor.fetchone()[0]
        print(f"Size of households_{postcode3}: {households_count} items")
        print(f"Size of temp_energy_labels_{postcode3}: {temp_energy_labels_count} items")

# Print the size of each subtable
print("Printing the size of each subtable...")
print_subtable_sizes()

# Index the subtables to speed up the queries
print("Indexing the subtables...")
for postcode3 in unique_postcode3_values:
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_temp_energy_labels_{postcode3}_BAGVerblijfsobjectID ON temp_energy_labels_{postcode3}(BAGVerblijfsobjectID);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_households_{postcode3}_vid ON households_{postcode3}(vid);")
conn.commit()
print("Indexes created successfully")

# Measure the time taken by the update query
start_time = time.time()

# Perform matching within each subtable where 0% of the connections are made
print("Performing matching within each subtable where 0% of the connections are made...")
for postcode3 in unique_postcode3_values:
    cursor.execute(f"SELECT COUNT(*) FROM households_{postcode3} WHERE energieklasse_EL IS NOT NULL;")
    non_null_count = cursor.fetchone()[0]
    cursor.execute(f"SELECT COUNT(*) FROM households_{postcode3};")
    total_count = cursor.fetchone()[0]
    if total_count > 0 and non_null_count == 0:
        print(f"Matching for Postcode3 = {postcode3}...")
        cursor.execute(f"""
            UPDATE households_{postcode3}
            SET energieklasse_EL = (
                SELECT Energieklasse
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            energiegebruik_EL = (
                SELECT BerekendeEnergieverbruik
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Warmtebehoefte_EL = (
                SELECT Warmtebehoefte
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Energiebehoefte_EL = (
                SELECT Energiebehoefte
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            GebruiksoppervlakteThermischeZone_EL = (
                SELECT GebruiksoppervlakteThermischeZone
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Gebouwsubtype_EL = (
                SELECT Gebouwsubtype
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Gebouwtype_EL = (
                SELECT Gebouwtype
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            OpBasisVanReferentiegebouw_EL = (
                SELECT OpBasisVanReferentiegebouw
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Berekeningstype_EL = (
                SELECT Berekeningstype
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Status_EL = (
                SELECT Status
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            ),
            Opnamedatum_EL = (
                SELECT Opnamedatum
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            )
            WHERE EXISTS (
                SELECT 1
                FROM temp_energy_labels_{postcode3}
                WHERE temp_energy_labels_{postcode3}.BAGVerblijfsobjectID = households_{postcode3}.vid
            );
        """)
        conn.commit()
        print(f"Matching for Postcode3 = {postcode3} completed")
    else:
        print(f"Skipping matching for Postcode3 = {postcode3} as connections are already made")

end_time = time.time()
print(f"Total time taken for the update query: {end_time - start_time:.2f} seconds")

# Create a new table to consolidate all the updated households subtables
print("Creating consolidated households table...")
cursor.execute("DROP TABLE IF EXISTS consolidated_households;")
cursor.execute("""
    CREATE TABLE consolidated_households AS
    SELECT * FROM households WHERE 1=0;
""")
conn.commit()

# Insert data from each updated subtable into the consolidated table
print("Inserting data from updated subtables into consolidated households table...")
for postcode3 in unique_postcode3_values:
    cursor.execute(f"""
        INSERT INTO consolidated_households
        SELECT * FROM households_{postcode3};
    """)
    conn.commit()
    print(f"Data from households_{postcode3} inserted into consolidated_households")

# Verify the update
print("Verifying the update...")
verify_query = """
SELECT vid, energieklasse_EL, energiegebruik_EL, Warmtebehoefte_EL, Energiebehoefte_EL,
       GebruiksoppervlakteThermischeZone_EL, Gebouwsubtype_EL, Gebouwtype_EL,
       OpBasisVanReferentiegebouw_EL, Berekeningstype_EL, Status_EL, Opnamedatum_EL
FROM consolidated_households
WHERE energieklasse_EL IS NOT NULL OR energiegebruik_EL IS NOT NULL
LIMIT 10
"""
cursor.execute(verify_query)
updated_rows = cursor.fetchall()
print("\nUpdated rows in consolidated_households table:")
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