import pandas as pd
import sqlite3

# Load combined JSON data
df = pd.read_json('../data/combined.json')

# Connect to SQLite database
conn = sqlite3.connect('household_data.db')

# Insert data into the table
df.to_sql('households', conn, if_exists='append', index=False)

# Close the connection
conn.close()