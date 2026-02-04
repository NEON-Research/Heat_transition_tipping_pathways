import pandas as pd
import matplotlib.pyplot as plt
import os

# Create charts folder if it doesn't exist
os.makedirs('charts', exist_ok=True)

# Load the Excel file
file_path = r"c:\Users\naudl\Documents\GitHub\Heat_transition_tipping_pathways\Heat transition tipping pathways\_export_results.xlsx"
df = pd.read_excel(file_path)

# Display the first few rows
print(df.head())
print(df.info())
print(df.shape)

# Get unique scenarios (runs) and heating methods - limit to first 4
scenarios = df['run'].unique()[:4]
# Identify the heating method columns ending with _total
heating_methods = [col for col in df.columns if col.endswith('_total')]

print(f"\nScenarios: {scenarios}")
print(f"Heating methods: {heating_methods}")

# Define colors and labels for heating methods (by prefix)
scenario_map = {
    'baseline': 'Baseline',
    'sl_low': Social learning Low',
    'sl_med': 'Social learning Medium', 
    'sl_high': 'Social learning High'
    'el_low': 'Economic learning Low',
    'el_med': 'Economic learning Medium', 
    'el_high': 'Economic learning High'
    'dd_dh': 'Demand driven District Heating',
    'pd_dh': 'Policy driven District Heating',
    'al_no': 'No actor allginment',
    'al_yes': 'Actor allginment'
}

color_map = {
    'ngb': '#C0C0C0',           # silver
    'hhp': '#D02090',           # violetRed
    'ehp': '#1E90FF',           # dodger blue
    'dh': '#CD853F'            # peru
}

label_map = {
    'ngb': 'Natural gas boiler',
    'hhp': 'Hybrid heat pump',
    'ehp': 'Electric heat pump',
    'dh': 'District heating'
}

# Create stacked line charts for each scenario
num_scenarios = len(scenarios)
fig, axes = plt.subplots(num_scenarios, 1, figsize=(12, 4 * num_scenarios))

# Handle single scenario case
if num_scenarios == 1:
    axes = [axes]

for idx, scenario in enumerate(scenarios):
    # Filter data for this scenario and keep only first occurrence of each year
    scenario_data = df[df['run'] == scenario].drop_duplicates(subset=['run', 'year'], keep='first').sort_values('year')
    
    # Prepare data for stacking
    scenario_data_pivot = scenario_data.set_index('year')[heating_methods]
    
    # Extract prefixes from column names and create rename/color mappings
    rename_dict = {col: label_map.get(col.split('_')[0], col) for col in heating_methods}
    color_dict = {col: color_map.get(col.split('_')[0], '#000000') for col in heating_methods}
    
    # Rename columns for display
    scenario_data_pivot_display = scenario_data_pivot.rename(columns=rename_dict)
    
    # Create stacked line chart (area plot) with colors
    colors = [color_dict[col] for col in heating_methods]
    scenario_data_pivot_display.plot(kind='area', stacked=True, ax=axes[idx],
                             title=f'Scenario: {scenario}',
                             xlabel='Year', ylabel='Dwellings',
                             color=colors)
    axes[idx].legend(title='Heating Methods', bbox_to_anchor=(1.05, 1), loc='upper left')
    axes[idx].grid(axis='y', alpha=0.3)

plt.tight_layout()
plt.savefig('charts/stacked_charts_per_scenario.png', dpi=300, bbox_inches='tight')
plt.show()

print("\nChart saved to 'charts/stacked_charts_per_scenario.png'")