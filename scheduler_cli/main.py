import datetime
import json
import math
from datetime import timedelta
from pathlib import Path

import click
from scheduler_algo import Scheduler
from scheduler_algo import PlanAlgorithm
import random


@click.group()
def scheduler_cli():
    pass


@scheduler_cli.command()
@click.argument('plan_algo', default=PlanAlgorithm.BRUTE_FORCE_GREEN_CASE.value,
                type=click.Choice([algo.value for algo in PlanAlgorithm]))
@click.option('--trace_route', type=click.Path(), default='../config/traceroutes/many_sources_to_one_transfer_node',
              show_default=True,
              help="Path to the trace route file.")
@click.option('--job_file', type=click.Path(),
              default='../config/jobs_config/jobs.json',
              show_default=True,
              help="Path to the job file.")
@click.option('--node_file', type=click.Path(), default='../config/node_configs/nodes_config.json', show_default=True,
              help="Path to the node file.")
@click.option('--forecast-file', type=click.Path(), default='../data/forecast_data.csv',
              show_default=True,
              help="Path to the file containing electricity maps forecast for every ip in a trace file.")
@click.option("--update-forecasts", type=click.BOOL, default=True,
              help="Download fresh forecasts from electricity maps")
@click.option("--associations-df-path", type=click.Path(), show_default=True,
              help="Path to the associations to bypass running simgrid and all simulations.")
@click.option("--associations-df-name", type=click.Path(), show_default=True, default="associations_df.csv",
              help="The name of the output file following simgrids energy consumption")
def schedule(plan_algo, trace_route, job_file, node_file, forecast_file, update_forecasts, associations_df_path,
             associations_df_name):
    """Schedule a job using the given file paths."""

    click.echo(f"Trace Route File: {trace_route}, Job File: {job_file}, Node File: {node_file}")
    scheduler_algo = Scheduler(node_file_path=node_file, ip_list_file_path=trace_route, job_file_path=job_file,
                               update_forecasts=update_forecasts)
    if associations_df_path is not None:
        path = Path(associations_df_path)
        if path.exists():
            scheduler_algo.read_in_associations_df(associations_df_path)
            scheduler_algo.create_plan(PlanAlgorithm(plan_algo))
            return

    scheduler_algo.load_in_forecasts(forecast_file)
    scheduler_algo.generate_energy_data()
    scheduler_algo.create_intervals(associations_df_name)
    scheduler_algo.create_plan(PlanAlgorithm(plan_algo))


@scheduler_cli.command()
@click.argument("num_of_jobs", type=click.INT)
@click.option("--job-output-path", type=click.Path(), default="../config/jobs_config/")
def generate_job_config(num_of_jobs, job_output_path):
    new_jobs = []
    job_id = 0
    total_bytes = 0
    for i in range(num_of_jobs):
        bytes_val = log_randint(1, 100 * 10 ** 12)
        total_bytes += bytes_val
        files_count = random.randint(1, 10000)
        job_id += 1
        deadline, extendable = generate_deadline(datetime.datetime.now())
        new_jobs.append({
            "bytes": bytes_val,
            "files_count": files_count,
            "id": job_id,
            "deadline": deadline,
            "extendable": extendable
        })

    all_jobs = new_jobs

    output_path = Path(job_output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / f"{len(all_jobs)}_jobs.json"
    with open(output_file, 'w') as f:
        json.dump(all_jobs, f, indent=2)
    click.echo(f"Successfully generated {num_of_jobs} new jobs")
    click.echo(f"Total jobs now: {len(all_jobs)} total bytes = {total_bytes}")
    click.echo(f"Saved to: {output_file}")


def log_randint(min_bytes, max_bytes):
    log_min = math.log10(min_bytes)
    log_max = math.log10(max_bytes)
    rand_log = random.uniform(log_min, log_max)
    return int(10 ** rand_log)


def generate_deadline(job_creation_time):
    """Generate realistic deadline constraints"""
    choice = random.choice([
        # Type 1: Urgent (must complete within forecast window)
        {"hours": random.randint(1, 24), "extendable": False},

        # Type 2: Flexible with hard cutoff (can delay but must finish by X)
        {"hours": random.randint(24, 72), "extendable": False},

        # Type 3: Fully flexible (can be continuously delayed)
        {"hours": random.randint(24, 168), "extendable": True}
    ])

    deadline = job_creation_time + timedelta(hours=choice["hours"])
    return deadline.isoformat(), choice["extendable"]


if __name__ == "__main__":
    scheduler_cli()
