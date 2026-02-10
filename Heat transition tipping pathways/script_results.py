from unittest import result
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import os

# Fixed order and colors for heating systems
HEATING_SYSTEM_ORDER = [
    'NATURAL_GAS_BOILER',
    'NATURAL_GAS_BLOCK',
    'HYBRID_HEAT_PUMP',
    'ELECTRIC_HEAT_PUMP',
    'DISTRICT_HEATING'
]

HEATING_SYSTEM_COLORS = {
    'NATURAL_GAS_BOILER': '#C0C0C0',   # silver
    'NATURAL_GAS_BLOCK': 'gray',
    'HYBRID_HEAT_PUMP': '#D02090',     # violet red
    'ELECTRIC_HEAT_PUMP': '#1E90FF',   # dodger blue
    'DISTRICT_HEATING': '#CD853F'      # peru
}

SCENARIO_PAIRS = [
    ("social_learning_factor_low", "social_learning_factor_high"),
    ("economic_learning_factor_low", "economic_learning_factor_high"),
    ("policy_driven_dh_strategy", "policy_driven_sha_strategy"),
    ("baseline", "actor_allignment_strategy"),
    ("individual_technologies", "collective_technologies"),
]


def load_and_prepare_results(csv_file: str) -> pd.DataFrame:
    """Load simulation results CSV and convert columns to proper dtypes."""
    
    script_dir = Path(__file__).parent
    csv_path = script_dir / csv_file if not Path(csv_file).is_absolute() else Path(csv_file)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found at: {csv_path}")

    df = pd.read_csv(csv_path, header=0)

    #print("Converting columns to correct data types...")

    numeric_cols = {
        'scenario': int,
        'iteration': int,
        'year': int,
        'installed_cumulative': int,
        'installed_current': int,
        'installed_annually': int,
        'removed_annually': int,
        'avg_att': float,
        'avg_util': float,
        'avg_sub_norm': float,
        'avg_eac': float,
        'avg_pbc': float
    }

    for col, dtype in numeric_cols.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            if dtype is int:
                df[col] = df[col].astype('Int64')  # safer than int with NaNs
            else:
                df[col] = df[col].astype(dtype)

    
    # Drop rows with missing key values
    df = df.dropna(subset=['scenario', 'ownership'])
    df['scenario'] = df['scenario'].astype(int)

    # Sanity check
    if df['scenario'].isna().any():
        print("⚠️ Warning: Some scenarios are still missing after cleaning!")
    
    print("✓ Data preparation complete")
    print(df.dtypes)

    print()

    return df

def create_stacked_percentage_plots(df: pd.DataFrame, output_dir='plots'):
    """
    Create stacked area plots showing percentage distribution of heating systems
    over time for each ownership category.
    
    Parameters:
    -----------
    csv_file : str
        Path to the CSV file
    output_dir : str
        Directory to save the plots (default: 'plots')
    """
    # Get unique ownership types
    ownership_types = df['ownership'].unique()
    
    # Get unique scenarios and iterations
    scenarios = df['scenario'].unique()
    
    print(f"Found {len(ownership_types)} ownership types: {ownership_types}")
    print(f"Found {len(scenarios)} scenarios: {scenarios}")
    
    # Loop through each combination of scenario and iteration
    for scenario in scenarios:
        # Filter data for this scenario and iteration
        df_filtered = df[(df['scenario'] == scenario)]
            
        if df_filtered.empty:
            continue
            
        # Loop through each ownership type
        #for ownership in ownership_types: ownership loop excluded
        # Filter for this ownership type
        ownership = 'TOTAL'  # Only plot TOTAL ownership
        df_ownership = df_filtered[df_filtered['ownership'] == ownership]
            
        if df_ownership.empty:
            continue
        
        # Mean over iterations
        df_ownership_mean = (
            df_ownership
            .groupby(['year', 'heating_system'], as_index=False)
            ['installed_current']
            .mean()
        )

        # Pivot the data: years as index, heating systems as columns
        pivot_data = df_ownership_mean.pivot_table(
            index='year',
            columns='heating_system',
            values='installed_current',
                    fill_value=0
            )
            
        # Calculate percentages
        row_totals = pivot_data.sum(axis=1)
        percentage_data = pivot_data.div(row_totals, axis=0) * 100
            
        # Sort columns alphabetically for consistent ordering
        # Reindex to fixed order (keep only systems that exist in the data)
        percentage_data = percentage_data.reindex(
            [hs for hs in HEATING_SYSTEM_ORDER if hs in percentage_data.columns],
            axis=1
        )
            
        # Create the stacked area plot
        fig, ax = plt.subplots(figsize=(12, 7))
            
        # Create color palette
        heating_systems = percentage_data.columns
        colors = [HEATING_SYSTEM_COLORS[hs] for hs in heating_systems]

            
        # Create stacked area plot
        ax.stackplot(percentage_data.index, 
                    *[percentage_data[col] for col in heating_systems],
                    labels=heating_systems,
                    colors=colors,
                    alpha=0.8)
        
        df_scenario = df[df['scenario'] == scenario]
        scenario_name = df_scenario['scenario_name'].iloc[0]
        
        # Customize the plot
        ax.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax.set_ylabel('Percentage (%)', fontsize=12, fontweight='bold')
        ax.set_title(f'Heating System Distribution - {ownership}\n'
                    f'Scenario: {scenario_name}',
                    fontsize=14, fontweight='bold', pad=20)
        
        # Set y-axis limits
        ax.set_ylim(0, 100)
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Add legend
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), 
                    frameon=True, fontsize=10)
        
        # Tight layout
        plt.tight_layout()
        
        # Save the plot
        filename = f"{output_dir}/stacked_plot_{scenario_name}_mean_{ownership}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Saved: {filename}")
        
        plt.close()

    print(f"\nAll plots saved to '{output_dir}/' directory")


def create_combined_plot(df: pd.DataFrame, output_file='plots/combined_scenarios_plot.png'):
    
    for scenario in df['scenario'].unique():
           
        # Filter scenario
        df_filtered = df[df['scenario'] == scenario].dropna(subset=['ownership'])

        ownership_types = sorted(df_filtered['ownership'].unique())
        n_plots = len(ownership_types)

        if n_plots == 0:
            print(f"⚠️ Scenario {scenario} has no ownership data. Skipping.")
            continue

        n_cols = min(2, n_plots)
        n_rows = int(np.ceil(n_plots / n_cols))

        # Now create the figure AFTER checking n_plots > 0
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5*n_rows))
        axes = axes.flatten() if n_plots > 1 else [axes]

        # Get all heating systems for consistent coloring
        color_map = HEATING_SYSTEM_COLORS
        
        # Plot each ownership type
        for idx, ownership in enumerate(ownership_types):
            ax = axes[idx]
            
            # Filter for this ownership type
            df_ownership = df_filtered[df_filtered['ownership'] == ownership]

            # Mean over iterations
            df_ownership_mean = (
                df_ownership
                .groupby(['year', 'heating_system'], as_index=False)
                ['installed_current']
                .mean()
            )
            
            # Pivot the data
            pivot_data = df_ownership_mean.pivot_table(
                index='year',
                columns='heating_system',
                values='installed_current',
                fill_value=0
            )
            
            # Calculate percentages
            row_totals = pivot_data.sum(axis=1)
            percentage_data = pivot_data.div(row_totals, axis=0) * 100
            
            # Sort columns
            # Reindex to fixed order (keep only systems that exist in the data)
            percentage_data = percentage_data.reindex(
                [hs for hs in HEATING_SYSTEM_ORDER if hs in percentage_data.columns],
                axis=1
            )

            
            # Get colors for this ownership's heating systems
            plot_colors = [color_map[hs] for hs in percentage_data.columns]
            
            # Create stacked area plot
            ax.stackplot(percentage_data.index,
                        *[percentage_data[col] for col in percentage_data.columns],
                        labels=percentage_data.columns,
                        colors=plot_colors,
                        alpha=0.8)
            
            # Customize subplot
            ax.set_xlabel('Year', fontsize=11)
            ax.set_ylabel('Percentage (%)', fontsize=11)
            ax.set_title(f'{ownership}', fontsize=12)
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.legend(loc='best', fontsize=8)
        
        # Hide unused subplots
        for idx in range(n_plots, len(axes)):
            axes[idx].set_visible(False)
        
        # Overall title
        scenario_name = df_filtered['scenario_name'].iloc[0]
        

        fig.suptitle(f'Heating System Distribution by Ownership Type\n'
                    f'_{scenario_name}',
                    fontsize=16, y=1.00)
        
        output_file_scenario = output_file.replace('.png', f'_{scenario_name}.png')
        plt.tight_layout()
        plt.savefig(output_file_scenario, dpi=300, bbox_inches='tight')
        plt.close()

def create_total_ownership_scenario_comparison(
    df: pd.DataFrame,
    output_file="plots/scenario_comparison_TOTAL.png"
):
    SCENARIOS = [
    "social_learning_factor_low",
    "social_learning_factor_high",
    "economic_learning_factor_low",
    "economic_learning_factor_high",
    "policy_driven_dh_strategy",
    "policy_driven_sha_strategy",
    "baseline",
    "actor_allignment_strategy",
    "individual_technologies",
    "collective_technologies",
    ]
    result = (
        df[
            (df["ownership"] == "TOTAL") &
            (df["year"] == 2050) &
            (df["heating_system"] == "NATURAL_GAS_BOILER") &
            (df["scenario_name"].isin(SCENARIOS))
        ]
        .groupby("scenario_name")["installed_current"]
        .mean()
        .sort_values()
    )

    print("Installed NATURAL_GAS_BOILER in 2050 (TOTAL ownership, mean over iterations):")
    print(result)



    #Get datagrame for TOTAL ownership only
    df_total = df[df["ownership"] == "TOTAL"]

    # Create figures and axes
    n_rows = len(SCENARIO_PAIRS)
    fig, axes = plt.subplots(
        n_rows, 2,
        figsize=(16, 4 * n_rows),
        sharey=True
    )

    #Loop through scenario pairs
    for row, (scen_left, scen_right) in enumerate(SCENARIO_PAIRS):
        for col, scenario_name in enumerate([scen_left, scen_right]):
            ax = axes[row, col]

            df_s = df_total[df_total["scenario_name"] == scenario_name]

            if df_s.empty:
                ax.set_visible(False)
                continue

            # 👉 mean over iterations FIRST
            mean_data = (
                df_s
                .groupby(["year", "heating_system"])["installed_current"]
                .mean()
                .reset_index()
            )

            pivot = mean_data.pivot(
                index="year",
                columns="heating_system",
                values="installed_current"
            ).fillna(0)

            pivot = pivot.reindex(columns=HEATING_SYSTEM_ORDER, fill_value=0)

            # 👉 percentages within TOTAL ownership
            percentages = pivot.div(pivot.sum(axis=1), axis=0) * 100

            ax.stackplot(
                percentages.index,
                [percentages[col] for col in percentages.columns],
                colors=[HEATING_SYSTEM_COLORS[c] for c in percentages.columns],
                alpha=0.85
            )

            ax.set_title(scenario_name.replace("_", " ").title(), fontsize=11)
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3, linestyle="--")

            if col == 0:
                ax.set_ylabel("Share of houses (%)")

            if row == n_rows - 1:
                ax.set_xlabel("Year")

    handles = [
    plt.Line2D([0], [0], color=HEATING_SYSTEM_COLORS[h], lw=8)
    for h in HEATING_SYSTEM_ORDER
    ]

    fig.legend(
        handles,
        HEATING_SYSTEM_ORDER,
        loc="lower center",
        ncol=len(HEATING_SYSTEM_ORDER),
        frameon=False
    )

    fig.suptitle(
        "Heating System Transition – TOTAL Ownership\nScenario Comparisons",
        fontsize=16,
        fontweight="bold"
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.95])
    plt.savefig(output_file, dpi=300)
    plt.close()



if __name__ == "__main__":
    csv_file = "results/simulation_results.csv"

    # Load & clean once
    df = load_and_prepare_results(csv_file)
    
    print("Creating stacked percentage plots for each scenario and ownership type...")
    create_stacked_percentage_plots(df, output_dir="plots/stacked_percentage_plots")

    print("\nCreating combined scenario comparison plots for each scenario...")
    create_combined_plot(df, output_file="plots/combined_ownership_plot.png")

    print("\nCreating scenario comparison plot for TOTAL ownership...")
    create_total_ownership_scenario_comparison(df, output_file="plots/scenario_comparison_TOTAL.png")
    