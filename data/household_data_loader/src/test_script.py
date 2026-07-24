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

# Drop all tables except for 'households' and 'temp_energy_labels'
print("Dropping all tables except for 'households' and 'temp_energy_labels'...")
for table in tables:
    table_name = table[0]
    if table_name not in ['households', 'temp_energy_labels']:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
        print(f"Table {table_name} dropped successfully")
conn.commit()

# Add a new column to temp_energy_labels for the first two characters of Postcode
print("Adding new column 'Postcode3' to temp_energy_labels...")
cursor.execute("ALTER TABLE temp_energy_labels ADD COLUMN Postcode3 INTEGER;")
cursor.execute("""
    UPDATE temp_energy_labels
    SET Postcode3 = CAST(SUBSTR(Postcode, 1, 3) AS INTEGER)
    WHERE Postcode IS NOT NULL;
""")
conn.commit()
print("New column 'Postcode3' added successfully")    

# Add a new column to households for the first two characters of postcode
print("Adding new column 'Postcode3' to households...")
cursor.execute("ALTER TABLE households ADD COLUMN Postcode3 INTEGER;")
cursor.execute("""
    UPDATE households
    SET Postcode3 = CAST(SUBSTR(postcode, 1, 3) AS INTEGER)
    WHERE postcode IS NOT NULL;
""")
conn.commit()
print("New column 'Postcode3' added successfully to households")

# Function to get column details and missing values
def get_column_details(table_name):
    cursor.execute(f"PRAGMA table_info({table_name});")
    columns = cursor.fetchall()
    print(f"\nColumns in table '{table_name}':")
    for column in columns:
        col_name = column[1]
        col_type = column[2]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_rows = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL;")
        missing_rows = cursor.fetchone()[0]
        missing_percentage = (missing_rows / total_rows) * 100 if total_rows > 0 else 0
        print(f"Column: {col_name}, Type: {col_type}, Missing Values: {missing_rows} ({missing_percentage:.2f}%)")

# Get column details and missing values for each table
for table in tables:
    table_name = table[0]
    get_column_details(table_name)


# Function to get the range of values in Postcode3
def get_postcode3_range(table_name):
    cursor.execute(f"SELECT MIN(Postcode3), MAX(Postcode3) FROM {table_name};")
    min_postcode3, max_postcode3 = cursor.fetchone()
    print(f"\nRange of values in 'Postcode3' for table '{table_name}':")
    print(f"Minimum Postcode3: {min_postcode3}")
    print(f"Maximum Postcode3: {max_postcode3}")

# Get the range of values in Postcode3 for the households table
get_postcode3_range('temp_energy_labels')

# Close the connection
conn.close()
print("Connection closed")