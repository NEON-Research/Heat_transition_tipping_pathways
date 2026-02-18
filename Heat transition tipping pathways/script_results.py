import math
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
    ("baseline", "actor_allignment_strategy"),
    ("social_learning_factor_low", "social_learning_factor_high"),
    ("economic_learning_factor_low", "economic_learning_factor_high"),
    ("policy_driven_dh_strategy", "policy_driven_sha_strategy"),
    ("individual_technologies", "collective_technologies"),
]

figwidth = 6.3+1  # A4 width in inches minus margins (21cm - 2*2.54cm) converted to inches
fontsizeLegend = 9
fontsizeGraphTitle = 12
fontsizePlotTitle = 9
fontsizeLabels = 7

def translate_heating_system_name(hs: str) -> str:
    """Translate heating system names from CAPSLOCK_WITH_UNDERSCORES to regular text."""
    return hs.replace('_', ' ').title()

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
        ax.set_xlabel('Year', fontsize=fontsizeLabels)
        ax.set_ylabel('Percentage (%)', fontsize=fontsizeLabels)
        ax.set_title(f'Heating System Distribution - {ownership}\n'
                    f'Scenario: {scenario_name}',
                    fontsize=fontsizePlotTitle, pad=10)
        
        # Set y-axis limits
        ax.set_ylim(0, 100)
        
        # Add grid
        ax.grid(True, alpha=0.3, linestyle='--')
        
        # Add legend
        ax.legend(loc='center left', bbox_to_anchor=(1, 0.5), 
                    frameon=True, fontsize=fontsizeLegend)
        
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
            ax.set_xlabel('Year', fontsize=fontsizeLabels)
            ax.set_ylabel('Percentage (%)', fontsize=fontsizeLabels)
            ax.set_title(f'{translate_heating_system_name(ownership)}', fontsize=fontsizePlotTitle)
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3, linestyle='--')
            ax.legend(loc='best', fontsize=fontsizeLegend)
        
        # Hide unused subplots
        for idx in range(n_plots, len(axes)):
            axes[idx].set_visible(False)
        
        # Overall title
        scenario_name = df_filtered['scenario_name'].iloc[0]
        

        fig.suptitle(f'Heating System Distribution by Ownership Type\n'
                    f'_{scenario_name}',
                    fontsize=fontsizeGraphTitle, y=1.00)
        
        output_file_scenario = output_file.replace('.png', f'_{scenario_name}.png')
        plt.tight_layout()
        plt.savefig(output_file_scenario, dpi=300, bbox_inches='tight')
        plt.close()

def create_total_ownership_scenario_comparison(
    df: pd.DataFrame,
    output_file="plots/scenario_comparison_TOTAL.png"
):
    SCENARIOS = [
    "baseline",
    "actor_allignment_strategy",
    "social_learning_factor_low",
    "social_learning_factor_high",
    "economic_learning_factor_low",
    "economic_learning_factor_high",
    "policy_driven_dh_strategy",
    "policy_driven_sha_strategy",
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

    # print("Installed NATURAL_GAS_BOILER in 2050 (TOTAL ownership, mean over iterations):")
    # print(result)

    #Get dataframe for TOTAL ownership only
    df_total = df[df["ownership"] == "TOTAL"]

    # Create figures and axes
    n_rows = len(SCENARIO_PAIRS)
    fig, axes = plt.subplots(
        n_rows, 2,
        figsize=(figwidth, 2 * n_rows),
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

            ax.set_title(scenario_name.replace("_", " ").title(), fontsize=fontsizePlotTitle, pad=9)
            ax.set_ylim(0, 100)
            ax.grid(True, alpha=0.3, linestyle="--")
            ax.tick_params(axis='both', labelsize=fontsizeLabels)

            if col == 0:
                ax.set_ylabel("Share (%)", fontsize = fontsizeLabels)

            if row == n_rows - 1:
                ax.set_xlabel("Year", fontsize = fontsizeLabels)

    handles = [
    plt.Line2D([0], [0], color=HEATING_SYSTEM_COLORS[h], lw=8)
    for h in HEATING_SYSTEM_ORDER
    ]

    # Get legend data
    legend_labels = [translate_heating_system_name(label) for label in HEATING_SYSTEM_ORDER]

    fig.legend(
        handles,
        legend_labels,
        loc="lower center",
        ncol= 3, #len(HEATING_SYSTEM_ORDER),
        frameon=False,
        fontsize=fontsizeLegend
    )

    fig.suptitle(
        "Scenario comparison of heating system distribution over time",
        fontsize=fontsizeGraphTitle
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.98])
    plt.savefig(output_file, dpi=300)
    plt.close()


def translate_metric_name(metric: str) -> str:
    """Translate metric column names to readable names."""
    translation = {
        'avg_att': 'Attitude',
        'avg_util': 'Perceived Utility',
        'avg_sub_norm': 'Subjective Norm',
        'avg_pbc': 'Perceived Behavioural Control',
        'avg_eac': 'Equivalent Annual Costs (€)'
    }
    return translation.get(metric, metric)


def create_detail_values_per_hm_per_scenario_plot(
    df: pd.DataFrame,
    scen = "baseline",
    output_file="plots/detail_values_per_hm_per_scenario.png"
):
    # Get privately-owned and scenario-specific data
    df_filtered = df[(df["ownership"] == "PRIVATELY_OWNED") & (df["scenario_name"] == scen) & (df["year"] != 2023)].copy()

    # Guard: nothing to do
    if df_filtered.empty:
        print(f"No data for scenario '{scen}' and ownership PRIVATELY_OWNED. Skipping.")
        return

    # Sort by year
    df_filtered = df_filtered.sort_values("year")

    # Compute mean and 90% CI (5th-95th percentile) over iterations per year and heating_system
    cols_to_avg = ["avg_att", "avg_util", "avg_sub_norm", "avg_pbc", "avg_eac"]
    available_cols = [c for c in cols_to_avg if c in df_filtered.columns]

    # Compute mean
    mean_data = (
        df_filtered
        .groupby(["year", "heating_system"], as_index=False)[available_cols]
        .mean()
    )

    # Compute 90% CI (5th and 95th percentiles)
    def ci_lower(x):
        return x.quantile(0.05)

    def ci_upper(x):
        return x.quantile(0.95)

    ci_lower_data = (
        df_filtered
        .groupby(["year", "heating_system"], as_index=False)[available_cols]
        .agg(ci_lower)
    )
    ci_lower_data.columns = ["year", "heating_system"] + [f"{c}_ci_lower" for c in available_cols]

    ci_upper_data = (
        df_filtered
        .groupby(["year", "heating_system"], as_index=False)[available_cols]
        .agg(ci_upper)
    )
    ci_upper_data.columns = ["year", "heating_system"] + [f"{c}_ci_upper" for c in available_cols]

    # Merge CI bounds with mean data
    mean_data = mean_data.merge(ci_lower_data, on=["year", "heating_system"])
    mean_data = mean_data.merge(ci_upper_data, on=["year", "heating_system"])

    # Use heating systems present in the averaged data
    heating_systems = mean_data["heating_system"].unique()
    if len(heating_systems) == 0:
        print("No heating systems found after averaging. Skipping.")
        return

    # Metrics to plot (use available averaged columns)
    metrics = available_cols.copy()
    if not metrics:
        print("No metrics available to plot. Skipping.")
        return

    # Create subplots: one subplot per metric, lines for each heating system
    # Figure width set to A4 width without default margins (21cm - 2*2.54cm = ~6.3 inches)
    n_metrics = len(metrics)
    n_cols = 2
    n_rows = math.ceil(n_metrics / n_cols)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6.3, 2.7 * n_rows), sharex=True)
    axes = np.array(axes).flatten()

    color_map = HEATING_SYSTEM_COLORS

    for mi, metric in enumerate(metrics):
        ax = axes[mi]
        for hs in heating_systems:
            hs_df = mean_data[mean_data["heating_system"] == hs]
            if metric not in hs_df.columns:
                continue
            
            # Plot mean line and 90% CI shaded region
            color = color_map.get(hs, "#888888")
            readable_hs = translate_heating_system_name(hs)
            ax.plot(hs_df["year"], hs_df[metric], label=readable_hs, color=color, linewidth=2)
            
            # Add 90% CI shaded region
            ci_lower_col = f"{metric}_ci_lower"
            ci_upper_col = f"{metric}_ci_upper"
            if ci_lower_col in hs_df.columns and ci_upper_col in hs_df.columns:
                ax.fill_between(hs_df["year"], hs_df[ci_lower_col], hs_df[ci_upper_col], 
                               color=color, alpha=0.15)

        readable_metric = translate_metric_name(metric)
        ax.set_title(readable_metric, fontsize = fontsizePlotTitle, pad = 10)
        if metric != "avg_eac":
            ax.set_ylim(0, 1)
        ax.set_xlabel("Year", fontsize=fontsizeLabels)
        ax.set_ylabel("", fontsize=fontsizeLabels)
        ax.tick_params(axis='both', labelsize=fontsizeLabels)  # Set tick label size
        ax.grid(True, alpha=0.3, linestyle="--")

    # Hide any unused axes
    for ax in axes[n_metrics:]:
        ax.set_visible(False)

    fig.suptitle("Average values for decision-making metrics for home owners\nin baseline scenario with 90% confidence interval", 
                 fontsize=fontsizeGraphTitle, y=0.95, wrap=True)

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Reserve space and add legend below plots
    plt.subplots_adjust(bottom=0.15, hspace=0.35, top=0.85)
    
    # Get legend data
    handles, labels = axes[0].get_legend_handles_labels()
    legend_labels = [translate_heating_system_name(label) for label in labels]
    
    # Add heating systems legend - let it wrap naturally, then add CI note separately
    fig.legend(handles, legend_labels, 
               loc='lower center', 
               bbox_to_anchor=(0.1, 0.04, 0.8, 0.0),  # (x, y, width, height) - width=0.8 constrains it
               ncol=3,  # 3 columns will give us 2 rows of heating systems (5 total)
               frameon=False, 
               fontsize=9,
               mode='expand',  # Expand to fill the bbox width
               columnspacing=1.0)
    
    # # Add CI note as centered text below the legend
    # fig.text(0.1, 0.015, "Shaded regions: 90% confidence interval", 
    #          ha='left', va='bottom', fontsize=8, style='italic')

    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close(fig)


def create_installed_current_grid_plot(
    df: pd.DataFrame,
    ownership="TOTAL",
    output_file="plots/installed_current_grid.png"
):
    """
    Create a 5x2 grid plot showing all heating systems with confidence intervals
    for each scenario pair (one pair per row).
    
    Parameters:
    -----------
    df : pd.DataFrame
        The full dataframe
    ownership : str
        Ownership type (TOTAL, PRIVATELY_OWNED, etc.)
    output_file : str
        Output file path
    """
    SCENARIO_PAIRS = [
        ("baseline", "actor_allignment_strategy"),
        ("social_learning_factor_low", "social_learning_factor_high"),
        ("economic_learning_factor_low", "economic_learning_factor_high"),
        ("policy_driven_dh_strategy", "policy_driven_sha_strategy"),
        ("individual_technologies", "collective_technologies"),
    ]
    
    # Create figure with 5 rows x 2 columns subplots with shared axes
    fig, axes = plt.subplots(5, 2, figsize=(figwidth, 1.8 * 5), sharex=True, sharey=True)
    
    # Track if we've collected legend handles yet
    legend_handles = None
    legend_labels = None
    
    # Iterate through scenario pairs (one pair per row)
    for row_idx, (scen1, scen2) in enumerate(SCENARIO_PAIRS):
        scenarios = [scen1, scen2]
        
        for col_idx, scen in enumerate(scenarios):
            ax = axes[row_idx, col_idx]

            # Filter data
            df_filtered = df[
                (df["ownership"] == ownership) & 
                (df["scenario_name"] == scen) & 
                (df["year"] != 2023)
            ].copy()

            df_trace = df_filtered[df_filtered["year"] == 2050]

            cols = ["SLF", "ELF", "GRR", "DHCT", "DHES", "SHAES", "DHCO", "GCHPB"]

            if not df_trace.empty:
                values = df_trace.iloc[0][cols].to_dict()
                print(f"Scenario '{scen}' → {values}")

            
            # Guard: nothing to do
            if df_filtered.empty:
                ax.text(0.5, 0.5, f"No data for\n{scen}", 
                       ha='center', va='center', transform=ax.transAxes)
                ax.set_title(scen.replace('_', ' ').title(), fontsize=10)
                continue
            
            # Sort by year
            df_filtered = df_filtered.sort_values("year")
            
            # Get unique heating systems
            heating_systems = [hs for hs in HEATING_SYSTEM_ORDER 
                             if hs in df_filtered["heating_system"].unique()]
            
            if len(heating_systems) == 0:
                ax.text(0.5, 0.5, "No heating systems", 
                       ha='center', va='center', transform=ax.transAxes)
                ax.set_title(scen.replace('_', ' ').title(), fontsize=10)
                continue
            
            # Plot each heating system
            for hs in heating_systems:
                # Filter for this heating system
                hs_data = df_filtered[df_filtered["heating_system"] == hs]
                
                if hs_data.empty:
                    continue
                
                # Group by year and compute statistics
                stats = hs_data.groupby("year")["installed_current"].agg([
                    ('mean', 'mean'),
                    ('p5', lambda x: x.quantile(0.05)),
                    ('p95', lambda x: x.quantile(0.95))
                ]).reset_index()
                
                color = HEATING_SYSTEM_COLORS.get(hs, "#888888")
                readable_hs = translate_heating_system_name(hs)
                
                # Plot mean line (values in millions)
                ax.plot(stats["year"], stats["mean"] / 1000000, color=color, 
                       linewidth=2, label=readable_hs)
                
                # 90% CI (5th-95th percentile) in millions
                ax.fill_between(stats["year"], stats["p5"] / 1000000, stats["p95"] / 1000000, 
                               color=color, alpha=0.15)
            
            # Collect legend handles from the first non-empty plot
            if legend_handles is None and ax.get_legend_handles_labels()[0]:
                legend_handles, legend_labels = ax.get_legend_handles_labels()
            
            # Formatting
            ax.tick_params(axis='both', labelsize=7)
            ax.grid(True, alpha=0.3, linestyle="--")
            ax.set_ylim(bottom=0)
            
            # Title for each subplot
            ax.set_title(scen.replace('_', ' ').title(), fontsize=9, pad=9)
            
            # Only show x-label on bottom row
            if row_idx == 4:
                ax.set_xlabel("Year", fontsize=7)
            
            # Only show y-label on left column
            if col_idx == 0:
                ax.set_ylabel("Installed (mil)", fontsize=7)
    
    # Overall title
    fig.suptitle("Heating system adoption over time scenario comparison\nwith 90% confidence intervals", 
                 fontsize=12, y=0.995, wrap=True)

    # Ensure output directory exists
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    # Reserve space and add legend below plots
    plt.subplots_adjust(bottom=0.1, hspace=0.35, top=0.92)
    
    # Add heating systems legend if we have handles
    if legend_handles:
        fig.legend(legend_handles, legend_labels, 
                   loc='lower center',  # Changed back to 'lower center'
                   bbox_to_anchor=(0.0, 0.005, 1.0, 0.0),  # (x, y, width, height) - full width
                   ncol=3,
                   frameon=False, 
                   fontsize=9,
                   columnspacing=1.0)
        
        # # Add CI note as left-aligned text below the legend
        # fig.text(0.1, 0.001, "Shaded regions: 90% confidence interval", 
        #          ha='left', va='bottom', fontsize=8, style='italic')
    
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved: {output_file}")

if __name__ == "__main__":
    csv_file = "results/simulation_results.csv"

    # Load & clean once
    df = load_and_prepare_results(csv_file)
    
    # print("Creating stacked percentage plots for each scenario and ownership type...")
    # create_stacked_percentage_plots(df, output_dir="plots/stacked_percentage_plots")

    print("\nCreating combined scenario comparison plots for each scenario...")
    create_combined_plot(df, output_file="plots/combined_ownership_plot.png")

    print("\nCreating scenario comparison plot for TOTAL ownership...")
    create_total_ownership_scenario_comparison(df, output_file="plots/scenario_comparison_TOTAL.png")

    print("\nCreating detail values per heating method plot for baseline scenario (privately owned)...")
    create_detail_values_per_hm_per_scenario_plot(df, scen="baseline", output_file="plots/detail_values_per_hm_baseline_privately_owned.png")
    
    print("\nCreating detail values per heating method plot for individual technologies scenario (privately owned)...")
    create_detail_values_per_hm_per_scenario_plot(df, scen="individual_technologies", output_file="plots/detail_values_per_hm_individual_technologies_privately_owned.png")
   

    create_installed_current_grid_plot(df, ownership="TOTAL",
    output_file="plots/installed_current_all.png")