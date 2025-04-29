import os
from typing import Dict, List

import pandas as pd
from rich.console import Console
import rich

from scheduler_algo import PlanAlgorithm


class ScheduleVisualization:

    def __init__(self, schedule_map: Dict[PlanAlgorithm, pd.DataFrame], associations_df: pd.DataFrame, job_list: List,
                 node_list: List, algo_times: Dict):
        self.schedule_map = schedule_map
        self.df = associations_df
        self.job_list = job_list
        self.job_ids = []
        self.job_bytes = {}
        self.job_bytes = {job['id']: job['bytes'] for job in self.job_list}
        self.job_ids = [job['id'] for job in self.job_list]
        self.node_list = node_list
        self.algo_times = algo_times
        self.console = Console()
        self.output_dir = '/workspace/schedules/'

    def save_schedules(self):
        for key, schedule in self.schedule_map.items():
            filepath = os.path.join(self.output_dir, key.value.lower())
            schedule.to_csv(filepath+'.csv')
            self.console.print(f"\n[bold green]Schedule saved to: [underline]{filepath}[/underline][/bold green]")

    def visualize(self):
        comparison_df = self._calculate_metrics()
        self._print_comparison_table(comparison_df)
        self._print_insights(comparison_df)
        self._save_comparison_csv(comparison_df)  # Add this line
        return comparison_df

    def _calculate_metrics(self) -> pd.DataFrame:
        """Calculate all comparison metrics for each algorithm."""
        comparison_data = []

        for algo, schedule_df in self.schedule_map.items():
            job_bytes_sent = self._calculate_bytes_sent(schedule_df)
            completion_stats = self._calculate_completion_stats(job_bytes_sent)
            deadline_stats = self._calculate_deadline_stats(schedule_df, job_bytes_sent)
            efficiency_stats = self._calculate_efficiency_stats(schedule_df, job_bytes_sent)

            comparison_data.append({
                'Algorithm': algo.value,
                **completion_stats,
                **deadline_stats,
                **efficiency_stats
            })

        return pd.DataFrame(comparison_data)

    def _calculate_bytes_sent(self, schedule_df: pd.DataFrame) -> Dict[str, float]:
        """Calculate bytes sent for each job in the schedule."""
        job_bytes_sent = {}
        for _, row in schedule_df.iterrows():
            job_id = row['job_id']
            bytes_sent = total_gb = (row['throughput'] * row['allocated_time']) / (8 * 1024**3)
            job_bytes_sent[job_id] = job_bytes_sent.get(job_id, 0) + bytes_sent
        return job_bytes_sent

    def _calculate_completion_stats(self, job_bytes_sent: Dict[str, float]) -> Dict:
        """Calculate job completion statistics."""
        job_requirements = {job['id']: job['bytes'] for job in self.job_list}
        completed_jobs = 0
        completed_bytes = 0
        for job_id, bytes_sent in job_bytes_sent.items():
            required_bytes = job_requirements.get(job_id, 0)
            if bytes_sent >= required_bytes * 0.99:  # 99% threshold
                completed_jobs += 1
            completed_bytes += min(bytes_sent, required_bytes)

        return {
            'Jobs Completed': f"{completed_jobs}/{len(self.job_list)}",
            'Completion %': f"{completed_bytes / sum(job_requirements.values()):.1%}",
            'Total Bytes': f"{completed_bytes / 1e9:.2f}GB",
        }

    def _calculate_deadline_stats(self, schedule_df: pd.DataFrame, job_bytes_sent: Dict[str, float]) -> Dict:
        """Calculate deadline-related statistics."""
        job_requirements = {job['id']: job['bytes'] for job in self.job_list}
        deadline_met = 0
        for job_id in job_bytes_sent.keys():
            job_df = schedule_df[schedule_df['job_id'] == job_id]
            if not job_df.empty:
                last_slot = job_df.iloc[-1]
                if (last_slot['forecast_id'] <= last_slot['deadline'] and
                        job_bytes_sent[job_id] >= job_requirements.get(job_id, 0) * 0.99):
                    deadline_met += 1

        return {
            'Deadline Met': f"{deadline_met}/{len(job_bytes_sent)}",
            'On Time %': f"{deadline_met / len(job_bytes_sent):.1%}" if job_bytes_sent else "0%"
        }

    def _calculate_efficiency_stats(self, schedule_df: pd.DataFrame, job_bytes_sent: Dict[str, float]) -> Dict:
        """Calculate efficiency and environmental metrics."""
        total_carbon = schedule_df['carbon_emissions'].sum()
        avg_throughput_bps = schedule_df['throughput'].mean()  # in bits per second
        total_transfer_time = schedule_df['transfer_time'].sum()
        completed_bytes = sum(min(bytes, next(j['bytes'] for j in self.job_list if j['id'] == job_id))
                              for job_id, bytes in job_bytes_sent.items())

        # Convert to appropriate units
        avg_throughput_gbps = avg_throughput_bps / 1e9 if pd.notna(avg_throughput_bps) else None

        return {
            'Carbon (kgCO₂)': f"{total_carbon / 1000:.2f}",
            'Avg Throughput': f"{avg_throughput_gbps:.2f} Gbps" if avg_throughput_gbps is not None else "N/A",
            'Schedule Time (sec)': f"{total_transfer_time:.2f} sec" if total_transfer_time is not None else "N/A",
            'Carbon/GB': f"{(total_carbon * 1e9) / completed_bytes:.2f} gCO₂/GB" if completed_bytes > 0 else "N/A"
        }

    def _print_comparison_table(self, comparison_df: pd.DataFrame):
        """Print the comparison table using rich."""
        table = rich.table.Table(
            title="[bold]Schedule Comparison[/bold]",
            show_header=True,
            header_style="bold magenta",
            expand=True
        )

        # Add columns
        for column in comparison_df.columns:
            table.add_column(column, justify="right")

        # Add rows
        for _, row in comparison_df.iterrows():
            table.add_row(*row.values.tolist())

        self.console.print(table)

    def _print_insights(self, comparison_df: pd.DataFrame):
        """Print key insights from the comparison."""
        self.console.print("\n[bold underline]Key Insights:[/bold underline]")

        # Create numeric versions of columns for comparison
        numeric_df = comparison_df.copy()
        numeric_columns = {
            'Completion %': lambda x: float(x.strip('%')) / 100,
            'On Time %': lambda x: float(x.strip('%')) / 100 if isinstance(x, str) and '%' in x else 0,
            'Carbon (kgCO₂)': lambda x: float(x.split()[0]),
            'Avg Throughput': lambda x: float(x.split()[0]) if isinstance(x, str) else 0,
            'Efficiency': lambda x: float(x.split()[0]) if isinstance(x, str) else 0
        }

        for col, converter in numeric_columns.items():
            if col in numeric_df.columns:
                numeric_df[col] = numeric_df[col].apply(converter)

        # Find best in each category
        insights = [
            ("Highest completion rate", "Completion %", max),
            ("Best deadline adherence", "On Time %", max),
            ("Lowest carbon emissions", "Carbon (kgCO₂)", min),
            ("Highest throughput", "Avg Throughput", max),
            ("Most efficient transfer", "Efficiency", max)
        ]

        for description, column, func in insights:
            if column in numeric_df.columns:
                best_idx = numeric_df[column].idxmax() if func == max else numeric_df[column].idxmin()
                best_algo = comparison_df.loc[best_idx, 'Algorithm']
                best_value = comparison_df.loc[best_idx, column]

                self.console.print(
                    f"[bold]{description}:[/bold] [green]{best_algo}[/green] "
                    f"({best_value})"
                )

    def _save_comparison_csv(self, comparison_df: pd.DataFrame):
        """Save the comparison table to a CSV file."""
        csv_path = os.path.join(self.output_dir, 'algorithm_comparison.csv')
        comparison_df.to_csv(csv_path, index=False)
        self.console.print(f"\n[bold green]Comparison table saved to: [underline]{csv_path}[/underline][/bold green]")