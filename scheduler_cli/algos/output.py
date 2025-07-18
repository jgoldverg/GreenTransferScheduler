import json
import os
from collections import defaultdict

from rich.console import Console
from rich.box import ROUNDED
from rich.panel import Panel
from rich.table import Table


class OutputFormatter:
    def __init__(self, job_list, node_list, time_slots, associations_df, output_dir='/workspace/schedules'):
        self.job_list = job_list
        self.node_list = node_list
        self.time_slots = time_slots
        self.slot_capacity = 3600
        self.output_dir = output_dir
        self.associations_df = associations_df
        self.console = Console()

        # Calculate job requirements (total bytes needed for each job)
        self.job_requirements = {
            j['id']: j['bytes']  # Using the 'size' field from job_list as total bytes needed
            for j in job_list
        }
        os.makedirs(self.output_dir, exist_ok=True)

    def save_to_csv(self, schedule_df, filename):
        filepath = os.path.join(self.output_dir, filename)
        parent_dir = os.path.dirname(filepath)
        os.makedirs(parent_dir,exist_ok=True)
        schedule_df.to_csv(filepath, index=False)
        self.console.print(f"\n[bold green]Schedule saved to: [underline]{filepath}[/underline][/bold green]")
        return filepath

    def _get_completion_style(self, percentage):
        if percentage >= 99.99:
            return "bold green"
        elif percentage >= 75:
            return "bold yellow"
        else:
            return "bold red"

    def _get_metric_style(self, value, thresholds):
        """Return style based on value thresholds (low, medium, high)"""
        if value <= thresholds[0]:
            return "green"
        elif value <= thresholds[1]:
            return "yellow"
        else:
            return "red"

    def calculate_metrics(self, schedule_df):
        metrics = {
            'job_metrics': defaultdict(dict),
            'node_utilization': defaultdict(lambda: {'total_time': 0, 'total_carbon': 0, 'total_bytes': 0}),
            'unscheduled_jobs': [],
            'partially_scheduled_jobs': []
        }

        scheduled_bytes = defaultdict(float)
        for _, row in schedule_df.iterrows():
            # Calculate bytes transferred in this allocation: throughput * allocated_time
            bytes_transferred = row['throughput'] * row['allocated_time']
            scheduled_bytes[row['job_id']] += bytes_transferred

            metrics['node_utilization'][row['node']]['total_time'] += row['allocated_time']
            metrics['node_utilization'][row['node']]['total_carbon'] += row['carbon_emissions']
            metrics['node_utilization'][row['node']]['total_bytes'] += bytes_transferred

        for job in self.job_list:
            job_id = job['id']
            job_bytes = scheduled_bytes.get(job_id, 0)
            required_bytes = self.job_requirements.get(job_id, 0)

            # Skip jobs with no requirements
            if required_bytes <= 0:
                continue

            completion_pct = (job_bytes / required_bytes) * 100 if required_bytes > 0 else 0

            if job_bytes == 0:
                metrics['unscheduled_jobs'].append(job_id)
            elif completion_pct < 99.99:  # Account for floating point precision
                metrics['partially_scheduled_jobs'].append({
                    'job_id': job_id,
                    'scheduled_bytes': job_bytes,
                    'required_bytes': required_bytes,
                    'completion_percentage': completion_pct
                })

            if job_bytes > 0:
                job_df = schedule_df[schedule_df['job_id'] == job_id]
                metrics['job_metrics'][job_id] = {
                    'total_bytes': job_bytes,
                    'total_time': job_df['allocated_time'].sum(),
                    'total_carbon': job_df['carbon_emissions'].sum(),
                    'nodes_used': job_df['node'].nunique(),
                    'completion_percentage': completion_pct
                }

        return metrics

    def generate_summary_stats(self, metrics, optimization_mode=None):
        total_carbon = sum(job['total_carbon'] for job in metrics['job_metrics'].values())
        total_time = sum(job['total_time'] for job in metrics['job_metrics'].values())
        total_bytes = sum(job['total_bytes'] for job in metrics['job_metrics'].values())

        return {
            'total_jobs': len(self.job_list),
            'scheduled_jobs': len(metrics['job_metrics']),
            'unscheduled_jobs': len(metrics['unscheduled_jobs']),
            'partially_scheduled_jobs': len(metrics['partially_scheduled_jobs']),
            'total_carbon': total_carbon,
            'total_time': total_time,
            'total_bytes': total_bytes,
            'avg_carbon_per_job': total_carbon / len(metrics['job_metrics']) if metrics['job_metrics'] else 0,
            'avg_time_per_job': total_time / len(metrics['job_metrics']) if metrics['job_metrics'] else 0,
            'avg_bytes_per_job': total_bytes / len(metrics['job_metrics']) if metrics['job_metrics'] else 0,
            'optimization_mode': optimization_mode
        }

    def print_summary(self, summary_stats, metrics):
        # Header Panel
        title = f"SCHEDULING RESULTS ({summary_stats['optimization_mode'].upper()})"
        self.console.print(Panel.fit(title, style="bold blue", padding=(1, 4)))

        # Scheduling Completion Table
        completion_table = Table(title="Scheduling Completion", box=ROUNDED, style="dim")
        completion_table.add_column("Metric", style="cyan")
        completion_table.add_column("Value", justify="right")

        completion_table.add_row("Total jobs", str(summary_stats['total_jobs']))
        completion_table.add_row("Fully scheduled", f"[green]{summary_stats['scheduled_jobs']}")

        if summary_stats['partially_scheduled_jobs']:
            completion_table.add_row("Partially scheduled", f"[yellow]{summary_stats['partially_scheduled_jobs']}")

        if summary_stats['unscheduled_jobs']:
            completion_table.add_row("Unscheduled jobs", f"[red]{summary_stats['unscheduled_jobs']}")

        self.console.print(completion_table)

        # Partially Scheduled Jobs Table
        if metrics['partially_scheduled_jobs']:
            partial_table = Table(title="Partially Scheduled Jobs", box=ROUNDED)
            partial_table.add_column("Job ID", style="cyan")
            partial_table.add_column("Scheduled (GB)", justify="right")
            partial_table.add_column("Required (GB)", justify="right")
            partial_table.add_column("Completion", justify="right")

            for job in metrics['partially_scheduled_jobs']:
                style = self._get_completion_style(job['completion_percentage'])
                partial_table.add_row(
                    str(job['job_id']),
                    f"{job['scheduled_bytes'] / 1e9:.2f}",
                    f"{job['required_bytes'] / 1e9:.2f}",
                    f"[{style}]{job['completion_percentage']:.1f}%[/]"
                )
            self.console.print(partial_table)

        # Performance Metrics Table
        perf_table = Table(title="Performance Metrics", box=ROUNDED)
        perf_table.add_column("Metric", style="cyan")
        perf_table.add_column("Total", justify="right")
        perf_table.add_column("Per Job", justify="right")

        carbon_style = self._get_metric_style(summary_stats['avg_carbon_per_job'], [100, 200])
        time_style = self._get_metric_style(summary_stats['avg_time_per_job'], [500, 1000])
        bytes_style = self._get_metric_style(summary_stats['avg_bytes_per_job'] / 1e9, [10, 50])  # Thresholds in GB

        perf_table.add_row(
            "Carbon Emissions",
            f"{summary_stats['total_carbon']:.2f}g",
            f"[{carbon_style}]{summary_stats['avg_carbon_per_job']:.2f}g[/]"
        )
        perf_table.add_row(
            "Transfer Time",
            f"{summary_stats['total_time']:.1f}s",
            f"[{time_style}]{summary_stats['avg_time_per_job']:.1f}s[/]"
        )
        perf_table.add_row(
            "Data Transferred",
            f"{summary_stats['total_bytes'] / 1e9:.2f} GB",
            f"[{bytes_style}]{summary_stats['avg_bytes_per_job'] / 1e9:.2f} GB[/]"
        )
        self.console.print(perf_table)

        # Node Utilization Table
        node_table = Table(title="Node Utilization", box=ROUNDED)
        node_table.add_column("Node", style="cyan")
        node_table.add_column("Time", justify="right")
        node_table.add_column("Time %", justify="right")
        node_table.add_column("Carbon", justify="right")
        node_table.add_column("Carbon %", justify="right")
        node_table.add_column("Data (GB)", justify="right")

        for node in self.node_list:
            node_name = node['name']
            stats = metrics['node_utilization'].get(node_name, {'total_time': 0, 'total_carbon': 0, 'total_bytes': 0})
            time_pct = (stats['total_time'] / summary_stats['total_time']) * 100 if summary_stats[
                                                                                        'total_time'] > 0 else 0
            carbon_pct = (stats['total_carbon'] / summary_stats['total_carbon']) * 100 if summary_stats[
                                                                                              'total_carbon'] > 0 else 0

            node_table.add_row(
                node_name,
                f"{stats['total_time']:.1f}s",
                f"{time_pct:.1f}%",
                f"{stats['total_carbon']:.2f}g",
                f"{carbon_pct:.1f}%",
                f"{stats['total_bytes'] / 1e9:.2f}"
            )
        self.console.print(node_table)

        # Unscheduled Jobs (if any)
        if metrics['unscheduled_jobs']:
            self.console.print("\n[bold red]Unscheduled Job IDs:")
            self.console.print(", ".join(str(job_id) for job_id in metrics['unscheduled_jobs']))

    def compare_algorithms(self, algorithm_results):
        """Compare multiple algorithm results in a single table"""
        comp_table = Table(title="Algorithm Comparison", box=ROUNDED, style="blue")

        # Columns
        comp_table.add_column("Algorithm", style="bold cyan")
        comp_table.add_column("Scheduled", justify="right")
        comp_table.add_column("Unscheduled", justify="right")
        comp_table.add_column("Partial", justify="right")
        comp_table.add_column("Total Carbon", justify="right")
        comp_table.add_column("Avg Carbon", justify="right")
        comp_table.add_column("Total Time", justify="right")
        comp_table.add_column("Avg Time", justify="right")
        comp_table.add_column("Total Data (GB)", justify="right")

        # Rows
        for algo_name, result in algorithm_results.items():
            summary = result['summary']

            # Determine styles
            scheduled_style = "green" if summary['unscheduled_jobs'] == 0 else "yellow" if summary[
                                                                                               'unscheduled_jobs'] < 3 else "red"
            carbon_style = self._get_metric_style(summary['avg_carbon_per_job'], [100, 200])
            time_style = self._get_metric_style(summary['avg_time_per_job'], [500, 1000])
            bytes_style = self._get_metric_style(summary['avg_bytes_per_job'] / 1e9, [10, 50])

            comp_table.add_row(
                algo_name.upper(),
                f"[{scheduled_style}]{summary['scheduled_jobs']}[/]",
                f"[red]{summary['unscheduled_jobs']}[/]" if summary['unscheduled_jobs'] else "0",
                f"[yellow]{summary['partially_scheduled_jobs']}[/]" if summary['partially_scheduled_jobs'] else "0",
                f"{summary['total_carbon']:.2f}g",
                f"[{carbon_style}]{summary['avg_carbon_per_job']:.2f}g[/]",
                f"{summary['total_time']:.1f}s",
                f"[{time_style}]{summary['avg_time_per_job']:.1f}s[/]",
                f"[{bytes_style}]{summary['total_bytes'] / 1e9:.2f}[/]"
            )

        self.console.print(comp_table)

    def format_output(self, schedule_df, filename=None, optimization_mode=None, algorithm_results=None):
        metrics = self.calculate_metrics(schedule_df)
        summary_stats = self.generate_summary_stats(metrics, optimization_mode)

        if filename:
            csv_path = self.save_to_csv(schedule_df, filename)
        else:
            csv_path = None

        self.print_summary(summary_stats, metrics)

        if algorithm_results:
            self.compare_algorithms(algorithm_results)

        return {
            'schedule': schedule_df,
            'csv_path': csv_path,
            'job_metrics': metrics['job_metrics'],
            'node_utilization': metrics['node_utilization'],
            'unscheduled_jobs': metrics['unscheduled_jobs'],
            'partially_scheduled_jobs': metrics['partially_scheduled_jobs'],
            'summary': summary_stats
        }
