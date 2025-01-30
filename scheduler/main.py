import click
from scheduler_algo import SchedulerAlgo


@click.group()
def scheduler_cli():
    pass


@scheduler_cli.command()
@click.option('--trace_route', type=click.Path(), default='../config/trace_route_with_coordinates.json', show_default=True,
              help="Path to the trace route file.")
@click.option('--job_file', type=click.Path(), default='../config/jobs.json', show_default=True,
              help="Path to the job file.")
@click.option('--node_file', type=click.Path(), default='../config/nodes_config.json', show_default=True,
              help="Path to the node file.")
@click.option('--forecast-file', type=click.Path(), default='../config/forecasts_config.json', show_default=True,
              help="Path to the file containing electricity maps forecast for every ip in a trace file.")
@click.option('--save-forecasts', type=click.BOOL, default=False, help="Saves the forecasts into the forecast-file path provided above")
def schedule(trace_route, job_file, node_file, forecast_file, save_forecasts):
    """Schedule a job using the given file paths."""
    click.echo(f"Trace Route File: {trace_route}, Job File: {job_file}, Node File: {node_file}")
    scheduler_algo = SchedulerAlgo(node_file_path=node_file, ip_list_file_path=trace_route, job_file_path=job_file)
    scheduler_algo.load_in_forecasts(forecast_file)
    scheduler_algo.create_intervals()
    # scheduler.visulize_ci_matrix(0)


if __name__ == "__main__":
    scheduler_cli()
