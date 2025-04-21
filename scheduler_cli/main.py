import datetime
import json
import math
import os.path
from datetime import timedelta
from pathlib import Path

import click
import pandas as pd
from generator import DataGenerator
from scheduler_algo import Scheduler
from scheduler_algo import PlanAlgorithm
import random


@click.group()
def scheduler_cli():
    pass


@scheduler_cli.command()
@click.option('-t', '--trace', type=click.Path(),
              default='../config/traceroutes/many_sources_to_one_transfer_node/',
              show_default=True,
              help="Path to the trace route file.")
@click.option('-j', '--jobs', type=click.Path(),
              default='../config/jobs_config/9_jobs.json',
              show_default=True,
              help="Path to the job file.")
@click.option('-n', '--nodes', type=click.Path(),
              default="../config/node_configs/nodes_space_3_config.json",
              show_default=True,
              help="Path to the source nodes file.")
@click.option('-f', '--forecast', type=click.Path(),
              default='../data/forecast_data.csv',
              show_default=True,
              help="Path to the electricity forecast file.")
@click.option('-u', '--update', type=click.BOOL,
              default=False,
              show_default=True,
              help="Whether to download fresh forecasts.")
@click.option('-d', '--df', type=click.Path(),
              default="../data/associations_df.csv",
              show_default=True,
              help="Path to output the associations_df.")
def gen(trace, jobs, nodes, forecast, update, df):
    click.secho("\n⚙️ Associations Df creation parameters:", fg="cyan", bold=True)
    click.echo(f"  • Trace Route: {click.style(trace, fg='yellow')}")
    click.echo(f"  • Job File: {click.style(jobs, fg='yellow')}")
    click.echo(f"  • Nodes Config: {click.style(nodes, fg='yellow')}")
    click.echo(f"  • Forecast File: {click.style(forecast, fg='yellow')}")
    click.echo(f"  • Update Forecasts: {click.style(str(update), fg='yellow')}")
    click.echo(f"  • Associations DF Path: {click.style(df, fg='yellow')}")
    click.secho("\n✅ Starting scheduler...", fg="green")

    #node_file_path, ip_list_file_path, job_file_path, update_forecasts, forecasts_path
    scheduler_algo = DataGenerator(node_file_path=nodes, ip_list_file_path=trace, job_file_path=jobs,
                                   update_forecasts=update, forecasts_path=forecast)
    scheduler_algo.prepare_fields()
    scheduler_algo.load_in_forecasts()
    scheduler_algo.generate_energy_data()
    scheduler_algo.create_intervals(df)


@scheduler_cli.command()
@click.argument('plan_algo', default=PlanAlgorithm.BRUTE_FORCE_GREEN_CASE.value,
                type=click.Choice([algo.value for algo in PlanAlgorithm]))
@click.option('--job_file', type=click.Path(),
              default='../config/jobs_config/9_jobs.json',
              show_default=True,
              help="Path to the job file.")
@click.option("--nodes_config", type=click.Path(),
              default="../config/node_configs/nodes_space_3_config.json",
              help="Path to source nodes")
@click.option("--df-path", type=click.Path(), show_default=True, default="../data/associations_df.csv",
              help="The path to output the associations_df")
def schedule(plan_algo, job_file, nodes_config, df_path):
    """Schedule a job using the given file paths."""
    click.secho("\n⚙️ Scheduling Parameters:", fg="cyan", bold=True)
    click.echo(f"  • Plan Algorithm: {click.style(plan_algo, fg='yellow')}")
    click.echo(f"  • Job File: {click.style(job_file, fg='yellow')}")
    click.echo(f"  • Nodes Config: {click.style(nodes_config, fg='yellow')}")
    click.echo(f"  • Associations DF Path: {click.style(df_path, fg='yellow')}")
    click.secho("\n✅ Starting scheduler...", fg="green")

    scheduler_algo = Scheduler(node_file_path=nodes_config, job_file_path=job_file, df_path=df_path)
    scheduler_algo.create_plan(PlanAlgorithm(plan_algo))


@scheduler_cli.command()
@click.argument("num_of_jobs", type=click.INT)
@click.option("--job-output-path", type=click.Path(), default="../config/jobs_config/")
@click.option('--deadline-end', type=click.INT, default="25")
def generate_job_config(num_of_jobs, job_output_path, deadline_end):
    """Generate job configs with deadlines as hours into future (0-25 hours)"""
    new_jobs = []
    job_id = 0
    total_bytes = 0

    for _ in range(num_of_jobs):
        # Generate log-distributed file sizes (1B to 100TB)
        bytes_val = log_randint(1, 100 * 10 ** 12)
        total_bytes += bytes_val

        # Generate proportional deadline based on size
        deadline_hours = generate_proportional_deadline(bytes_val, deadline_end)

        new_jobs.append({
            "bytes": bytes_val,
            "files_count": random.randint(1, 10000),
            "id": job_id + 1,
            "deadline": deadline_hours  # Hours into future
        })
        job_id += 1

    output_path = Path(job_output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / f"{num_of_jobs}_jobs.json"

    with open(output_file, 'w') as f:
        json.dump(new_jobs, f, indent=2)

    click.echo(f"Generated {num_of_jobs} jobs with:")
    click.echo(f"- Total data: {total_bytes / 10 ** 12:.2f} TB")
    click.echo(f"- Deadline range: 1-25 hours")
    click.echo(f"Saved to: {output_file}")


def log_randint(min_bytes, max_bytes):
    """Generate log-distributed random bytes"""
    log_min = math.log10(min_bytes)
    log_max = math.log10(max_bytes)
    rand_log = random.uniform(log_min, log_max)
    return int(10 ** rand_log)


def generate_proportional_deadline(file_size, max_deadline):
    """Generate deadline proportional to file size (1-25 hours)"""
    # Normalize size to 0-1 range (log scale)
    log_size = math.log10(file_size)
    min_log = math.log10(1)
    max_log = math.log10(100 * 10 ** 12)
    normalized = (log_size - min_log) / (max_log - min_log)

    # Map to 1-25 hour range (smaller files get tighter deadlines)
    min_hours = 1
    deadline = max_deadline - (normalized * (max_deadline - min_hours))

    # Add some randomness and ensure integer hours
    deadline = int(deadline * random.uniform(0.9, 1.1))
    return max(min_hours, min(deadline, max_deadline))


if __name__ == "__main__":
    scheduler_cli()
