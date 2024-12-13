import pandas as pd
import json
import glob

# List all JSON files in the directory
files = glob.glob('c:/Users/s124129/Documents/GitHub/Heat_transition_tipping_pathways/household_data_loader/data/*.json')


# Read and concatenate JSON files
dataframes = [pd.read_json(file) for file in files]
combined_df = pd.concat(dataframes, ignore_index=True)

# Save the combined dataframe to a new JSON file
combined_df.to_json('combined.json', orient='records')

