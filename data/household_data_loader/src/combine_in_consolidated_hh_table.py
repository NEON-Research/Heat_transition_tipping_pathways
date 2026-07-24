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