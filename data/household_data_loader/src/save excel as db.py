import pandas as pd
import sqlite3

# Load Excel file
excel_file = 'data.xlsx'
df = pd.read_excel(excel_file)

# Connect to SQLite
conn = sqlite3.connect('database.db')

# Save DataFrame as a table
df.to_sql('table_name', conn, if_exists='replace', index=False)

conn.close()