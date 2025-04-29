import json
import math
from pathlib import Path

import click
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

    # node_file_path, ip_list_file_path, job_file_path, update_forecasts, forecasts_path
    scheduler_algo = DataGenerator(node_file_path=nodes, ip_list_file_path=trace, job_file_path=jobs,
                                   update_forecasts=update, forecasts_path=forecast)
    scheduler_algo.prepare_fields()
    scheduler_algo.load_in_forecasts()
    scheduler_algo.generate_energy_data()
    scheduler_algo.create_intervals(df)


@scheduler_cli.command()
@click.argument('plan_algo', default=PlanAlgorithm.BRUTE_FORCE_GREEN_CASE.value,
                type=click.Choice([algo.value for algo in PlanAlgorithm]))
@click.option('-j', '--job_file', type=click.Path(),
              default='../config/jobs_config/9_jobs.json',
              show_default=True,
              help="Path to the job file.")
@click.option('-n', "--nodes_config", type=click.Path(),
              default="../config/node_configs/nodes_space_3_config.json",
              help="Path to source nodes")
@click.option('-d', "--df-path", type=click.Path(), show_default=True, default="../data/associations_df.csv",
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

    jobs, total_size = generate_realistic_jobs(num_of_jobs, deadline_end)

    output_path = Path(job_output_path)
    output_path.mkdir(parents=True, exist_ok=True)
    output_file = output_path / f"{num_of_jobs}_jobs.json"

    with open(output_file, 'w') as f:
        json.dump(jobs, f, indent=2)

    click.echo(f"Generated {num_of_jobs} jobs with:")
    click.echo(f"- Total data: {total_size / 10 ** 12:.2f} TB")
    click.echo(f"- Deadline range: 1-25 hours")
    click.echo(f"Saved to: {output_file}")


def generate_realistic_jobs(num_of_jobs, deadline_end=25):
    """Generate more realistic HPC file transfer jobs with clustered distributions"""
    job_types = {
        'small_urgent': {'size_range': (1024 ** 1, 100 * 1024 ** 2),  # 1B-100MB
                         'deadline_range': (1, 6),
                         'count_range': (1, 100),
                         'weight': 0.4},
        'medium_standard': {'size_range': (100 * 1024 ** 2, 10 * 1024 ** 3),  # 100MB-10GB
                            'deadline_range': (6, 18),
                            'count_range': (100, 1000),
                            'weight': 0.35},
        # 'large_relaxed': {'size_range': (10 * 1024 ** 3, 500 * 1024 ** 3),  # 10GB-100GB
        #                   'deadline_range': (12, 25),
        #                   'count_range': (1000, 5000),
        #                   'weight': 0.2},
        # 'huge_flexible': {'size_range': (100 * 1024 ** 3, 10 * 1024 ** 4),  # 100GB-1TB
        #                   'deadline_range': (18, 25),
        #                   'count_range': (5000, 10000),
        #                   'weight': 0.05}
    }

    jobs = []
    total_data = 0
    for job_id in range(1, num_of_jobs + 1):
        # Weighted random selection of job type
        job_type = random.choices(
            list(job_types.keys()),
            weights=[t['weight'] for t in job_types.values()]
        )[0]

        params = job_types[job_type]

        # Generate log-distributed size within range
        size = 10 ** random.uniform(
            math.log10(params['size_range'][0]),
            math.log10(params['size_range'][1]))
        total_data += size
        # Generate deadline within range with some randomness
        deadline = random.randint(*params['deadline_range'])
        if random.random() < 0.2:  # 20% chance to be more urgent
            deadline = max(1, deadline - random.randint(1, 3))

        jobs.append({
            "bytes": int(size),
            "files_count": random.randint(*params['count_range']),
            "id": job_id,
            "deadline": deadline,
            "type": job_type  # For visualization grouping
        })

    return jobs, total_data


def human_readable_bytes(bytes, units=[' bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB']):
    """Returns a human-readable string representation of bytes."""
    return str(bytes) + units[0] if bytes < 1024 else human_readable_bytes(bytes >> 10, units[1:])


if __name__ == "__main__":
    scheduler_cli()
