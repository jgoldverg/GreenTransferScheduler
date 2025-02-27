from enum import EnumType

import click

from scheduler_algo import Scheduler
from algos import PlanAlgorithm


@click.group()
def scheduler_cli():
    pass


@scheduler_cli.command()
@click.argument('plan_algo', default=PlanAlgorithm.BRUTE_FORCE_GREEN_CASE.value , type=click.Choice([algo.value for algo in PlanAlgorithm]))
@click.option('--trace_route', type=click.Path(), default='../config/traceroutes/',
              show_default=True,
              help="Path to the trace route file.")
@click.option('--job_file', type=click.Path(),
              default='../config/jobs.json',
              show_default=True,
              help="Path to the job file.")
@click.option('--node_file', type=click.Path(), default='../config/nodes_config.json', show_default=True,
              help="Path to the node file.")
@click.option('--forecast-file', type=click.Path(), default='/workspace/data/forecast_data.csv',
              show_default=True,
              help="Path to the file containing electricity maps forecast for every ip in a trace file.")
@click.option("--update-forecasts", type=click.BOOL, default=False, help="Download fresh forecasts from electricity maps")
def schedule(plan_algo, trace_route, job_file, node_file, forecast_file, update_forecasts):
    """Schedule a job using the given file paths."""

    click.echo(f"Trace Route File: {trace_route}, Job File: {job_file}, Node File: {node_file}")
    scheduler_algo = Scheduler(node_file_path=node_file, ip_list_file_path=trace_route, job_file_path=job_file, update_forecasts=update_forecasts)
    scheduler_algo.load_in_forecasts(forecast_file)
    scheduler_algo.generate_energy_data()
    scheduler_algo.create_intervals()
    scheduler_algo.create_plan(PlanAlgorithm(plan_algo))
    # scheduler_cli.visulize_ci_matrix(0)


if __name__ == "__main__":
    scheduler_cli()
