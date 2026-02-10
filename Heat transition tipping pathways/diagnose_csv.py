import pandas as pd
from pathlib import Path

csv_file = "results/simulation_results.csv"
script_dir = Path(__file__).parent
csv_path = script_dir / csv_file

print("Reading first few lines of CSV file RAW...")
print("=" * 80)

# Read raw lines
with open(csv_path, 'r') as f:
    for i, line in enumerate(f):
        if i < 5:
            print(f"Line {i}: {line.rstrip()}")
            print(f"  Number of commas: {line.count(',')}")
            print()

print("=" * 80)
print("\nReading with pandas...")
print("=" * 80)

# Read with pandas
df = pd.read_csv(csv_path)

print(f"Columns found by pandas: {len(df.columns)}")
print(f"Column names: {list(df.columns)}")
print()

print("First row of data:")
print(df.iloc[0])
print()

print("Data types:")
print(df.dtypes)
print()

