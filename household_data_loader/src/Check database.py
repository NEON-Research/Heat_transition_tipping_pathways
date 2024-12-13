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