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

# Create subtables for each Postcode2 value from 10 to 99 if they do not already exist
print("Creating subtables for each Postcode2 value if they do not already exist...")
for i in range(10, 100):
    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='temp_energy_labels_{i}';")
    if cursor.fetchone() is None:
        cursor.execute(f"""
            CREATE TABLE temp_energy_labels_{i} AS
            SELECT * FROM temp_energy_labels WHERE Postcode2 = {i};
        """)
        print(f"Subtable temp_energy_labels_{i} created successfully")
    else:
        print(f"Subtable temp_energy_labels_{i} already exists, skipping creation")

    cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='households_{i}';")
    if cursor.fetchone() is None:
        cursor.execute(f"""
            CREATE TABLE households_{i} AS
            SELECT * FROM households WHERE Postcode2 = {i};
        """)
        print(f"Subtable households_{i} created successfully")
    else:
        print(f"Subtable households_{i} already exists, skipping creation")
    conn.commit()

# Function to print the size of each subtable
def print_subtable_sizes():
    for i in range(10, 100):
        cursor.execute(f"SELECT COUNT(*) FROM households_{i};")
        households_count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM temp_energy_labels_{i};")
        temp_energy_labels_count = cursor.fetchone()[0]
        print(f"Size of households_{i}: {households_count} items")
        print(f"Size of temp_energy_labels_{i}: {temp_energy_labels_count} items")

# Print the size of each subtable
print("Printing the size of each subtable...")
print_subtable_sizes()

# Index the subtables to speed up the queries
print("Indexing the subtables...")
for i in range(10, 100):
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_temp_energy_labels_{i}_BAGVerblijfsobjectID ON temp_energy_labels_{i}(BAGVerblijfsobjectID);")
    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_households_{i}_vid ON households_{i}(vid);")
conn.commit()
print("Indexes created successfully")

# Measure the time taken by the update query
start_time = time.time()

# Iterate through the full list of households and update based on Postcode2
print("Iterating through the full list of households and updating based on Postcode2...")
for i in range(10, 100):
    cursor.execute(f"SELECT * FROM households_{i};")
    households = cursor.fetchall()
    for household in households:
        vid = household[0]
        postcode2 = household[1]  # Assuming Postcode2 is the second column
        temp_table = f"temp_energy_labels_{postcode2}"
        cursor.execute(f"""
            SELECT Energieklasse, BerekendeEnergieverbruik, Warmtebehoefte, Energiebehoefte,
                   GebruiksoppervlakteThermischeZone, Gebouwsubtype, Gebouwtype,
                   OpBasisVanReferentiegebouw, Berekeningstype, Status, Opnamedatum
            FROM {temp_table}
            WHERE BAGVerblijfsobjectID = ?;
        """, (vid,))
        match = cursor.fetchone()
        if match:
            cursor.execute(f"""
                UPDATE households_{postcode2}
                SET energieklasse_EL = ?, energiegebruik_EL = ?, Warmtebehoefte_EL = ?,
                    Energiebehoefte_EL = ?, GebruiksoppervlakteThermischeZone_EL = ?,
                    Gebouwsubtype_EL = ?, Gebouwtype_EL = ?, OpBasisVanReferentiegebouw_EL = ?,
                    Berekeningstype_EL = ?, Status_EL = ?, Opnamedatum_EL = ?
                WHERE vid = ?;
            """, (*match, vid))
            cursor.execute(f"DELETE FROM {temp_table} WHERE BAGVerblijfsobjectID = ?;", (vid,))
            conn.commit()
            print(f"Updated and removed entry for vid {vid} in households_{postcode2}")

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
for i in range(10, 100):
    cursor.execute(f"""
        INSERT INTO consolidated_households
        SELECT * FROM households_{i};
    """)
    conn.commit()
    print(f"Data from households_{i} inserted into consolidated_households")

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