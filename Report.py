import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import sqlite3
import os
import logging
import numpy as np
logging.getLogger('matplotlib').setLevel(logging.WARNING)
logging.getLogger('PIL').setLevel(logging.WARNING)

    # Generates a visual report for MQTT load tester. 
    # Bar chart with delay (ms) on y-axis and message index on x-axis.
    # Table chart with delay summary (min, max, avg, median) and message succesratio.

class LoadTestReport:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_filename = os.path.splitext(os.path.basename(db_path))[0]

    # Connect to SQLite database and query the necessary data.
    def read_data(self):
        conn = sqlite3.connect(self.db_path)
        query = "SELECT MessageIndex, HighResPublishTime, Delay, Failed FROM results"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Handling NULL values for better data manipulation and visualization
        df['Delay'] = pd.to_numeric(df['Delay'], errors='coerce')  # Convert delay to numeric, making NULL to NaN
        df.replace('N/A', np.nan, inplace=True)  # Replace 'N/A' with NaN
        return df

    def generate_summary_statistics(self):
        df = self.read_data()
        # Filter to only include successful messages (where Failed is 0)
        success_df = df[df['Failed'] == 0]

        # Calculate summary statistics only for successful messages
        if not success_df.empty:
            summary = {
                'Min (ms)': round(success_df['Delay'].min(), 3),
                'Max (ms)': round(success_df['Delay'].max(), 3),
                'Average (ms)': round(success_df['Delay'].mean(), 3),
                'Median (ms)': round(success_df['Delay'].median(), 3)
            }
        else:
            summary = {'Min (ms)': 'N/A', 'Max (ms)': 'N/A', 'Average (ms)': 'N/A', 'Median (ms)': 'N/A'}

        return summary


    # Generates a bar chart and a table for delay summary/message succes.
    def generate_charts_and_tables(self):
        df = self.read_data()
        # Assuming 'PublishTime' holds perf_counter values, convert them to strings with 's' suffix to indicate seconds
        df['FormattedPublishTime'] = df['HighResPublishTime'].astype(float).apply(lambda x: f"{x:.3f}s")

        # Create visible bars for failed messages
        min_height_for_failed = df['Delay'].max() * 0.5  # Minimal height 50% from max to ensure visibility.
        df['AdjustedDelay'] = df.apply(lambda x: min_height_for_failed if x['Failed'] == 1 else x['Delay'], axis=1)


        df['Color'] = df['Failed'].apply(lambda x: 'red' if x else 'green')

        # Create the figure
        fig = plt.figure(figsize=(14, 6))
        grid_spec = fig.add_gridspec(2, 3, width_ratios=[3, 1, 1], height_ratios=[1, 1])
        ax_bar = fig.add_subplot(grid_spec[:, 0])

        sns.barplot(x='MessageIndex', y='AdjustedDelay', hue='Color', data=df, dodge=False, ax=ax_bar, palette={'green': 'green', 'red': 'red'})
        ax_bar.set_title(f'Message Delay Visualization - {self.db_filename}')
        ax_bar.set_xlabel('Message Index')
        ax_bar.set_ylabel('Delay (ms)')

        # Customize the legend
        success_patch = mpatches.Patch(color='green', label='Success')
        failed_patch = mpatches.Patch(color='red', label='Failed')
        ax_bar.legend(handles=[success_patch, failed_patch], title='Message Status')

        total_messages = len(df)
        # Set tick frequency based on the number of messages
        if total_messages <= 10:
            tick_frequency = 1  # Show every message
        else:
            # Calculate tick frequency to display 10 labels if there are more than 10 messages
            tick_frequency = total_messages // 10

        # Set x-axis ticks at the calculated interval
        ax_bar.set_xticks(range(0, total_messages, tick_frequency))

        # Set the labels for the x-axis ticks, also at the same interval
        ax_bar.set_xticklabels(df['MessageIndex'].iloc[::tick_frequency], rotation=90)
            # Ensure that x-tick labels do not overlap and are readable
        plt.setp(ax_bar.get_xticklabels(), rotation=45, ha="right", rotation_mode="anchor")

        # Summary statistics
        summary_stats = pd.DataFrame({
            'Min (ms)': [round(df['Delay'].min(), 3)],
            'Max (ms)': [round(df['Delay'].max(), 3)],
            'Average (ms)': [round(df['Delay'].mean(), 3)],
            'Median (ms)': [round(df['Delay'].median(), 3)]
        })

        # Calculate the summary statistics including success rate
        success_stats = pd.DataFrame({
            'Msg success': [len(df) - df['Failed'].sum()],
            'Msg failed': [df['Failed'].sum()],
            'Total msgs': [len(df)],
            'Success rate (%)': [round((1 - df['Failed'].mean()) * 100, 2)]
        })

        # Place tables on the right side of the bar chart
        ax_table1 = fig.add_subplot(grid_spec[0, 1:])
        ax_table1.axis('off')
        table1 = ax_table1.table(cellText=summary_stats.values,
                                 colLabels=summary_stats.columns,
                                 loc='center')
        table1.auto_set_font_size(False)
        table1.set_fontsize(10)
        table1.scale(1, 2)

        ax_table2 = fig.add_subplot(grid_spec[1, 1:])
        ax_table2.axis('off')
        table2 = ax_table2.table(cellText=success_stats.values,
                                 colLabels=success_stats.columns,
                                 loc='center')
        table2.auto_set_font_size(False)
        table2.set_fontsize(10)
        table2.scale(1, 2)

        plt.tight_layout()

        # Save the entire figure
        plt.savefig(f'{self.db_filename}_report.png')