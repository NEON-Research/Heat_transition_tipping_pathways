import sqlite3

def check_type(value, expected_type):
    if value is None:  # NULL values are valid
        return True
    if expected_type == "INTEGER":
        return isinstance(value, int) or (isinstance(value, str) and value.isdigit())
    elif expected_type == "TEXT":
        return isinstance(value, str) and value.strip() != ""  # Exclude empty strings
    elif expected_type == "REAL":
        try:
            float(value)
            return True
        except ValueError:
            return False
    elif expected_type == "BLOB":
        return isinstance(value, bytes)
    return False

def update_mismatched_values(cursor, table_name, col_name, col_type):
    cursor.execute(f"SELECT rowid, {col_name} FROM {table_name};")
    rows = cursor.fetchall()

    mismatched_rowids = []
    for row in rows:
        rowid, value = row
        if not check_type(value, col_type):
            mismatched_rowids.append(rowid)

    if mismatched_rowids:
        placeholders = ", ".join("?" for _ in mismatched_rowids)
        cursor.execute(
            f"UPDATE {table_name} SET {col_name} = NULL WHERE rowid IN ({placeholders});",
            mismatched_rowids
        )

def main():
    db_path = "households.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]
        print(f"Checking table: {table_name}")

        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()

        for column in columns:
            col_name = column[1]
            col_type = column[2]
            print(f"Checking column: {col_name} of type {col_type}")

            cursor.execute(f"SELECT {col_name} FROM {table_name};")
            rows = cursor.fetchall()

            total_values = len(rows)
            mismatched_count = 0

            for row in rows:
                value = row[0]
                if not check_type(value, col_type):
                    mismatched_count += 1

            if total_values > 0:
                mismatch_percentage = (mismatched_count / total_values) * 100
                print(f"Column '{col_name}' has {mismatch_percentage:.2f}% values that do not match type '{col_type}'")

            update_mismatched_values(cursor, table_name, col_name, col_type)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
