import sqlite3
import pandas as pd

# Connect to the SQLite database
conn = sqlite3.connect('household_data.db')

# Query the data
df = pd.read_sql_query('SELECT * FROM households LIMIT 10', conn)

# Display the data
print(df)

# Close the connection
conn.close()


## Check if table exists
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# List all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", tables)

# Close the connection
conn.close()

## Check structure of database
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# Check the structure of the table
cursor.execute("PRAGMA table_info(households);")
columns = cursor.fetchall()
print("Columns:", columns)

# Close the connection
conn.close()

## Check if data in table
import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# Count the rows in the table
cursor.execute("SELECT COUNT(*) FROM households;")
row_count = cursor.fetchone()[0]
print("Number of rows in households table:", row_count)

# Close the connection
conn.close()