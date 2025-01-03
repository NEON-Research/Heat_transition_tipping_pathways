import sqlite3
import pandas as pd
import os

print("Current working directory:", os.getcwd())
print("Files in the current directory:", os.listdir())

# Load CSV into DataFrame
csv_file = "./data/energy_label_data_202412.csv"
csv_df = pd.read_csv(csv_file, skiprows=2, delimiter=';')  # Change ',' if needed

print(csv_df.info())  # Shows non-null counts and data types
print(csv_df.head())  # Preview the first few rows
print(csv_df.columns)

# Summarize missing values in each column
missing_values = csv_df.isnull().sum()
print(missing_values)

# For a percentage of missing values:
missing_percentage = (csv_df.isnull().mean() * 100).round(2)
print(missing_percentage)

# Find duplicate rows
duplicates = csv_df[csv_df.duplicated()]
print(duplicates)

# Count duplicates
duplicate_count = csv_df.duplicated().sum()
print(f"Number of duplicate rows: {duplicate_count}")



# Connect to SQLite database
conn = sqlite3.connect('households.db')
cursor = conn.cursor()

# Inspect column names in CSV and database
cursor.execute("PRAGMA table_info(households)")
print(cursor.fetchall())




# Create a temporary SQL table from CSV
csv_df.to_sql("temp_table", conn, if_exists="replace", index=False)

# Add columns to households table if they don't already exist
columns_to_add = [
    ("verblijfsobjecten_EL", "TEXT"),
    ("energieklasse_EL", "TEXT"),
    ("energiegebruik_EL", "REAL"),
    ("warmtegebruik_EL", "REAL"),
]
for column, col_type in columns_to_add:
    try:
        cursor.execute(f"ALTER TABLE households ADD COLUMN {column} {col_type}")
        print(f"Column '{column}' added successfully.")
    except sqlite3.OperationalError:
        print(f"Column '{column}' already exists. Skipping.")


# Ensure columns exist in households table
print(csv_df.dtypes)
# Add columns to households table if they don't already exist
columns_to_add = [
    ("verblijfsobjecten_EL", "TEXT"),
    ("energieklasse_EL", "TEXT"),
    ("energiegebruik_EL", "REAL"),
    ("warmtegebruik_EL", "REAL"),
]
for column, col_type in columns_to_add:
    try:
        cursor.execute(f"ALTER TABLE households ADD COLUMN {column} {col_type}")
        print(f"Column '{column}' added successfully.")
    except sqlite3.OperationalError:
        print(f"Column '{column}' already exists. Skipping.")

# Inspect column names again
cursor.execute("PRAGMA table_info(households)")
print(cursor.fetchall())


# Check if matching column exists and ensure correct naming
matching_key = "BAGVerblijfsobjectID"  # Column to match in your_table
original_key = "vid"  # Column to match in original_table


# Index columns for faster lookup
cursor.execute("CREATE INDEX IF NOT EXISTS idx_households_original_key ON households(vid);")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_temp_table_matching_key ON temp_table(BAGVerblijfsobjectID);")


# Add or update matching info using SQL
update_query = f"""
    UPDATE households
    SET 
        verblijfsobjecten_EL = (
            SELECT temp_table.BAGVerblijfsobjectID
            FROM temp_table
            WHERE households.{original_key} = temp_table.{matching_key}
        ),
        energieklasse_EL = (
            SELECT temp_table.Energieklasse
            FROM temp_table
            WHERE households.{original_key} = temp_table.{matching_key}
        ),
        energiegebruik_EL = (
            SELECT temp_table.BerekendeEnergieverbruik
            FROM temp_table
            WHERE households.{original_key} = temp_table.{matching_key}
        ),
        warmtegebruik_EL = (
            SELECT temp_table.Warmtebehoefte
            FROM temp_table
            WHERE households.{original_key} = temp_table.{matching_key}
        )
    WHERE EXISTS (
        SELECT 1
        FROM temp_table
        WHERE households.{original_key} = temp_table.{matching_key}
    );
"""


cursor.execute(update_query)
conn.commit()


