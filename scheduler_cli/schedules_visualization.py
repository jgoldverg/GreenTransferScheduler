import os
from typing import Dict, List, Optional
import pandas as pd
from rich.console import Console
import rich
from rich.table import Table
from scheduler_algo import PlanAlgorithm


class ScheduleVisualization:
    def __init__(self, schedule_map: Dict[PlanAlgorithm, pd.DataFrame],
                 associations_df: pd.DataFrame, job_list: List[Dict],
                 node_list: List[str], algo_times: Dict[str, float]):
        self.schedule_map = schedule_map
        self.df = associations_df
        self.job_list = job_list
        self.node_list = node_list
        self.algo_times = algo_times
        self.console = Console()
        self.output_dir = '/workspace/schedules/'

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Precompute job requirements
        self.job_requirements = {job['id']: job['bytes'] for job in self.job_list}
        self.total_bytes_required = sum(self.job_requirements.values())

    def save_schedules(self):
        """Save all schedules to CSV files."""
        for algo, schedule in self.schedule_map.items():
            filename = f"{algo.value.lower()}_schedule.csv"
            filepath = os.path.join(self.output_dir, filename)
            schedule.to_csv(filepath, index=False)
            self.console.print(f"\n[bold green]Schedule saved to: [underline]{filepath}[/underline][/bold green]")

    def visualize(self) -> pd.DataFrame:
        """Generate and display comparison metrics."""
        comparison_df = self._calculate_metrics()
        self._print_comparison_table(comparison_df)
        self._print_insights(comparison_df)
        self._save_comparison_csv(comparison_df)
        return comparison_df

    def _calculate_metrics(self) -> pd.DataFrame:
        """Calculate comparison metrics for each algorithm."""
        comparison_data = []

        for algo, schedule_df in self.schedule_map.items():
            if schedule_df.empty:
                self.console.print(f"[yellow]Warning: Empty schedule for {algo.value}[/yellow]")
                continue

            job_stats = self._calculate_job_stats(schedule_df)
            efficiency_stats = self._calculate_efficiency_stats(schedule_df, job_stats)
            comparison_data.append({
                'Algorithm': algo.value,
                'Runtime (ms)': f"{self.algo_times.get(str(algo), 0):.2f}",
                **job_stats,
                **efficiency_stats
            })

        return pd.DataFrame(comparison_data)

    def _calculate_job_stats(self, schedule_df: pd.DataFrame) -> Dict:
        """Calculate job completion and deadline statistics."""
        job_bytes_sent = self._calculate_bytes_sent(schedule_df)
        completed_jobs = 0
        completed_bytes = 0
        deadline_met = 0

        for job_id, bytes_sent in job_bytes_sent.items():
            required_bytes = self.job_requirements.get(job_id, 0)

            # Completion stats
            if bytes_sent >= required_bytes * 0.99:  # 99% threshold
                completed_jobs += 1
            completed_bytes += min(bytes_sent, required_bytes)

            # Deadline stats
            job_schedule = schedule_df[schedule_df['job_id'] == job_id]
            if not job_schedule.empty:
                last_slot = job_schedule.iloc[-1]
                job_deadline = next((j['deadline'] for j in self.job_list if j['id'] == job_id), float('inf'))
                if (last_slot['forecast_id'] <= job_deadline and
                        bytes_sent >= required_bytes * 0.99):
                    deadline_met += 1

        return {
            'Jobs Completed': f"{completed_jobs}/{len(self.job_list)}",
            'Completion %': f"{completed_bytes / self.total_bytes_required:.1%}",
            'Deadline Met': f"{deadline_met}/{len(job_bytes_sent)}",
            'On Time %': f"{deadline_met / len(job_bytes_sent):.1%}" if job_bytes_sent else "0%"
        }

    def _calculate_bytes_sent(self, schedule_df: pd.DataFrame) -> Dict[int, float]:
        """Calculate bytes sent for each job."""
        job_bytes = {}

        for _, row in schedule_df.iterrows():
            job_id = row['job_id']
            # Calculate bytes sent: throughput (bps) * time (s) / 8 = bytes
            bytes_sent = (row['throughput'] * row['allocated_time']) / 8
            job_bytes[job_id] = job_bytes.get(job_id, 0) + bytes_sent

        return job_bytes

    def _calculate_efficiency_stats(self, schedule_df: pd.DataFrame, job_stats: Dict) -> Dict:
        """Calculate efficiency and environmental metrics."""
        total_carbon = schedule_df['carbon_emissions'].sum()
        avg_throughput = schedule_df['throughput'].mean()
        total_time = schedule_df['allocated_time'].sum()

        # Calculate completed bytes from job_stats
        completion_pct = float(job_stats['Completion %'].strip('%')) / 100
        completed_bytes = self.total_bytes_required * completion_pct

        return {
            'Carbon (kgCO₂)': f"{total_carbon / 1000:.2f}",
            'Avg Throughput': f"{avg_throughput / 1e9:.2f} Gbps",
            'Total Time': f"{total_time:.2f} sec",
            'Carbon/GB': f"{(total_carbon * 1e9) / completed_bytes:.2f}" if completed_bytes > 0 else "N/A",
            'Efficiency': f"{completed_bytes / total_time / 1e6:.2f} MB/s" if total_time > 0 else "N/A"
        }

    def _print_comparison_table(self, comparison_df: pd.DataFrame):
        """Print a formatted comparison table."""
        table = Table(title="[bold]Schedule Algorithm Comparison[/bold]", show_header=True, header_style="bold magenta")

        # Add columns
        for col in comparison_df.columns:
            table.add_column(col, justify="right")

        # Add rows
        for _, row in comparison_df.iterrows():
            table.add_row(*[str(x) for x in row])

        self.console.print(table)

    def _print_insights(self, comparison_df: pd.DataFrame):
        """Print key insights from the comparison."""
        self.console.print("\n[bold underline]Key Insights:[/bold underline]")

        # Convert percentage columns to numeric values
        numeric_df = comparison_df.copy()
        for col in ['Completion %', 'On Time %']:
            numeric_df[col] = numeric_df[col].str.rstrip('%').astype(float) / 100

        # Convert other metrics to numeric
        for col in ['Carbon (kgCO₂)', 'Avg Throughput', 'Carbon/GB']:
            numeric_df[col] = numeric_df[col].replace('N/A', 0).apply(
                lambda x: float(x.split()[0]) if isinstance(x, str) else x)

        # Find best performers
        metrics = [
            ('Highest completion rate', 'Completion %', max),
            ('Best deadline adherence', 'On Time %', max),
            ('Lowest carbon emissions', 'Carbon (kgCO₂)', min),
            ('Highest throughput', 'Avg Throughput', max),
            ('Most carbon efficient', 'Carbon/GB', min)
        ]

        for desc, col, func in metrics:
            if col in numeric_df.columns:
                best_val = func(numeric_df[col])
                best_algo = comparison_df.loc[numeric_df[col] == best_val, 'Algorithm'].iloc[0]
                best_display = comparison_df.loc[numeric_df[col] == best_val, col].iloc[0]

                self.console.print(
                    f"[bold]{desc}:[/bold] [green]{best_algo}[/green] ({best_display})"
                )

    def _save_comparison_csv(self, comparison_df: pd.DataFrame):
        """Save the comparison results to CSV."""
        csv_path = os.path.join(self.output_dir, 'algorithm_comparison.csv')
        comparison_df.to_csv(csv_path, index=False)
        self.console.print(f"\n[bold green]Comparison saved to: [underline]{csv_path}[/underline][/bold green]")