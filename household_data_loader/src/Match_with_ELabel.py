import sqlite3
import pandas as pd

# Load CSV into DataFrame
csv_file = "./data/energy_label_data_202412.csv"
csv_df = pd.read_csv(csv_file, skiprows=2, delimiter=';')  # Change ',' if needed

print(csv_df.info())  # Shows non-null counts and data types
print(csv_df.head())  # Preview the first few rows

# Store csv to db
"""
conn_el = sqlite3.connect('energy_label_data.db')
csv_df.to_sql('energy_labels', conn, if_exists='replace', index=False)
cursor_el = conn_el.cursor()
"""

# Connect to SQLite database
conn_hh = sqlite3.connect('households.db')
cursor_hh = conn_hh.cursor()

# Connect to the first SQLite database
conn_el = sqlite3.connect('energy_label_data.db')
cursor_el = conn_el.cursor()

# Check which tables are in the first SQLite database
cursor_el.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor_el.fetchall()
print("Tables in energy_label_data.db:", tables)

# Add columns to households table if they don't already exist
columns_to_add = [
    ("verblijfsobjecten_EL", "TEXT"),
    ("energieklasse_EL", "TEXT"),
    ("energiegebruik_EL", "REAL"),
    ("warmtegebruik_EL", "REAL"),
]
for column, col_type in columns_to_add:
    try:
        cursor_hh.execute(f"ALTER TABLE households ADD COLUMN {column} {col_type}")
        print(f"Column '{column}' added successfully.")
    except sqlite3.OperationalError:
        print(f"Column '{column}' already exists. Skipping.")

# Index columns for faster lookup
cursor_hh.execute("CREATE INDEX IF NOT EXISTS idx_households_original_key ON households(vid);")
cursor_el.execute("CREATE INDEX IF NOT EXISTS idx_energy_labels_matching_key ON energy_labels(BAGVerblijfsobjectID);")


# Inspect column names again
cursor_hh.execute("PRAGMA table_info(households)")
print(cursor_hh.fetchall())
cursor_el.execute("PRAGMA table_info(energy_labels)")
print(cursor_el.fetchall())


# Fetch data from energy_labels table
cursor_el.execute("SELECT * FROM energy_labels;")
rows = cursor_el.fetchall()

# Get column names from energy_labels
cursor_el.execute("PRAGMA table_info(energy_labels);")
columns = [col[1] for col in cursor_el.fetchall()]

# Create temp_energy_labels table in households.db
cursor_hh.execute("DROP TABLE IF EXISTS temp_energy_labels;")
create_table_query = f"""
    CREATE TABLE temp_energy_labels (
        {', '.join([f'{col} TEXT' for col in columns])}
    );
"""
cursor_hh.execute(create_table_query)


# Insert data into temp_energy_labels
insert_query = f"INSERT INTO temp_energy_labels ({', '.join(columns)}) VALUES ({', '.join(['?'] * len(columns))})"
cursor_hh.executemany(insert_query, rows)
conn_hh.commit()


# Close the energy_labels database connection
conn_el.close()


# Update households table with data from temp_energy_labels table
update_query = """
    UPDATE households
    SET 
        verblijfsobjecten_EL = (SELECT temp_energy_labels.BAGVerblijfsobjectID FROM temp_energy_labels WHERE households.vid = temp_energy_labels.BAGVerblijfsobjectID LIMIT 1),
        energieklasse_EL = (SELECT temp_energy_labels.Energieklasse FROM temp_energy_labels WHERE households.vid = temp_energy_labels.BAGVerblijfsobjectID LIMIT 1),
        energiegebruik_EL = (SELECT temp_energy_labels.BerekendeEnergieverbruik FROM temp_energy_labels WHERE households.vid = temp_energy_labels.BAGVerblijfsobjectID LIMIT 1),
        warmtegebruik_EL = (SELECT temp_energy_labels.Warmtebehoefte FROM temp_energy_labels WHERE households.vid = temp_energy_labels.BAGVerblijfsobjectID LIMIT 1)
    WHERE EXISTS (
        SELECT 1
        FROM temp_energy_labels
        WHERE households.vid = temp_energy_labels.BAGVerblijfsobjectID
    );
"""

cursor_hh.execute(update_query)
conn_hh.commit()






# Check if matching column exists and ensure correct naming
matching_key = "BAGVerblijfsobjectID"  # Column to match in your_table
original_key = "vid"  # Column to match in original_table



# Update households table with data from energy_labels table
update_query = """
    UPDATE households
    SET 
        verblijfsobjecten_EL = (SELECT energy_labels.BAGVerblijfsobjectID FROM energy_labels WHERE households.vid = energy_labels.BAGVerblijfsobjectID LIMIT 1),
        energieklasse_EL = (SELECT energy_labels.Energieklasse FROM energy_labels WHERE households.vid = energy_labels.BAGVerblijfsobjectID LIMIT 1),
        energiegebruik_EL = (SELECT energy_labels.BerekendeEnergieverbruik FROM energy_labels WHERE households.vid = energy_labels.BAGVerblijfsobjectID LIMIT 1),
        warmtegebruik_EL = (SELECT energy_labels.Warmtebehoefte FROM energy_labels WHERE households.vid = energy_labels.BAGVerblijfsobjectID LIMIT 1)
    WHERE EXISTS (
        SELECT 1
        FROM energy_labels
        WHERE households.vid = energy_labels.BAGVerblijfsobjectID
    );
"""

cursor_hh.execute(update_query)
conn_hh.commit()

# Close connections
conn_hh.close()
conn_el.close()

# Add or update matching info using SQL
update_query = f"""
    UPDATE households
    SET 
        verblijfsobjecten_EL = (
            SELECT energy_label_data.BAGVerblijfsobjectID
            FROM temp_table
            WHERE households.{original_key} = energy_label_data.{matching_key}
        ),
        energieklasse_EL = (
            SELECT energy_label_data.Energieklasse
            FROM temp_table
            WHERE households.{original_key} = energy_label_data.{matching_key}
        ),
        energiegebruik_EL = (
            SELECT energy_label_data.BerekendeEnergieverbruik
            FROM temp_table
            WHERE households.{original_key} = energy_label_data.{matching_key}
        ),
        warmtegebruik_EL = (
            SELECT energy_label_data.Warmtebehoefte
            FROM temp_table
            WHERE households.{original_key} = energy_label_data.{matching_key}
        )
    WHERE EXISTS (
        SELECT 1
        FROM energy_label_data
        WHERE households.{original_key} = energy_label_data.{matching_key}
    );
"""

