import sqlite3

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# Create a new table
cursor.execute('''
CREATE TABLE IF NOT EXISTS households (
    id INTEGER PRIMARY KEY,
    characteristic1 TEXT,
    characteristic2 TEXT,
    characteristic3 TEXT
    -- Add more columns as needed
)
''')

# Commit changes and close the connection
conn.commit()
conn.close()