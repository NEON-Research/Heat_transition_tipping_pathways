# Inspect final database
#Is github copilot now installed?


import sqlite3

# Connect to SQLite database
conn = sqlite3.connect('household_data.db')
cursor = conn.cursor()

# Inspect column names in the database
cursor.execute("PRAGMA table_info(households)")
columns = cursor.fetchall()
print("Columns in households table:", columns)

# Fetch the desired columns (0, 2, 21, 22, 26, 39, 40, 41, and 42) from the households table
cursor.execute("""
    SELECT numid, vid, energieklasse, woning_type, p6_gasm3_2023, BAGVerblijfsobjectenID, verblijfsobjecten_EL, energieklasse_EL, energiegebruik_EL 
    FROM households 
    LIMIT 100
""")
rows = cursor.fetchall()



# Print the results
for row in rows:
    print(row)



# Count the total number of rows in the households table
cursor.execute("SELECT COUNT(*) FROM households")
total_rows = cursor.fetchone()[0]

# Count the number of rows where 'energiegebruik_EL' is not NULL
cursor.execute("SELECT COUNT(*) FROM households WHERE energiegebruik_EL IS NOT NULL")
non_null_rows = cursor.fetchone()[0]

# Calculate the percentage of rows with a value for 'energiegebruik_EL'
percentage = (non_null_rows / total_rows) * 100

# Print the result
print(f"The percentage of rows with a value for 'energiegebruik_EL' is {percentage:.2f}%")    

# Close the connection
conn.close()