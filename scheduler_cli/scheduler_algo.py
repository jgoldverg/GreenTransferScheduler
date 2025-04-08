import concurrent.futures
import os.path
import pathlib
from enum import Enum
from typing import List, Dict
import pandas as pd
import click
from models import IpToLonAndLat, IpOrderAndForecastData, read_in_node_file, read_in_job_file, read_in_ip_map, read_in_node_list_to_map
from simgrid_simulator import SimGridSimulator

from algos.milp_green import MixedIntegerLinearProgrammingGreenPlanner
from algos.basic_planner import BasicPlanner
from algos.random_planner import RandomPlanner
from algos.brute_force_green_planner import BruteForceGreenPlanner
from algos.worst_case_planner import WorstCasePlanner
from algos.greedy_carbon_planner import CarbonAwarePlanner
from algos.rl_algo import RLGreenScheduler


class PlanAlgorithm(Enum):
    BASIC_CASE = "basic"
    RANDOM = "random"
    WORST_CASE = "worst"
    BRUTE_FORCE_GREEN_CASE = "green"
    LINEAR_PROGRAMMING_GREEN = "milp_green"
    ALL = "all"
    RL_GREEN_SCHEDULER = "rl_green"


class Scheduler:

    def __init__(self, node_file_path, ip_list_file_path, job_file_path, update_forecasts):
        # Load in Nodes
        self.node_list = read_in_node_file(node_file_path)
        self.node_map = read_in_node_list_to_map(node_file_path)
        click.secho(f"Loaded {len(self.node_list)} Nodes", fg="green")

        # Load in Traceroute Measurements
        self.nodeid_to_map_traceroutes = read_in_ip_map(ip_list_file_path)
        self.ip_list = get_unique_ips(self.nodeid_to_map_traceroutes)
        self.update_forecasts = update_forecasts
        click.secho(
            f"Loaded Total Traceroute Measurements {len(self.nodeid_to_map_traceroutes)} reduced to Unique IP's {len(self.ip_list)}",
            fg="green")

        self.job_list = read_in_job_file(job_file_path)
        click.secho(f"Loaded {len(self.job_list)} Jobs", fg="green")

        self.simulator = SimGridSimulator(traceroute_data=self.nodeid_to_map_traceroutes, node_list=self.node_list,
                                          job_list=self.job_list)
        self.simulator.create_xml_for_traceroute()

        self.ci_matrix = None
        self.ip_forecast = []

    def load_in_forecasts(self, forecasts_file_path):
        # Load directory or single file worth of traceroutes with lon,lat,
        if os.path.exists(forecasts_file_path):
            try:
                self.forecasts_df = pd.read_csv(forecasts_file_path)
                click.secho(f"Loaded {len(self.forecasts_df)} past forecasts from {forecasts_file_path}")
            except Exception as e:
                click.secho(f"Error loading existing forecasts: {e}", fg="red")
                self.forecasts_df = pd.DataFrame()
        else:
            self.forecasts_df = pd.DataFrame()

        click.secho("Downloading forecasts from Electricity Maps...", fg="yellow", bold=True)
        ipForecast = IpOrderAndForecastData()
        forecast_entries = []
        if self.update_forecasts:
            with click.progressbar(self.ip_list, show_eta=True, show_percent=True) as ips:
                for ipCoord in ips:
                    forecast_list = ipForecast.fetch_forecast_for_ip(ipCoord)
                    for idx, forecast in enumerate(forecast_list):
                        forecast_entries.append({
                            'timestamp': forecast['timestamp'],
                            'ci': forecast['ci'],
                            'ip': forecast['ip'],
                            'lat': ipCoord.lat,
                            'lon': ipCoord.lon,
                            'forecast_idx': idx,
                            'node_id': ipCoord.node_id
                        })
                    click.secho(f"Processed {ipCoord.ip}", fg="blue", dim=True)

            # Convert new forecast entries to a DataFrame
            new_forecasts_df = pd.DataFrame(forecast_entries)

            # Merge old and new forecasts, then drop duplicates based on timestamp
            if not self.forecasts_df.empty:
                self.forecasts_df = pd.concat([self.forecasts_df, new_forecasts_df], ignore_index=True)
            else:
                self.forecasts_df = new_forecasts_df

            self.forecasts_df.drop_duplicates(inplace=True)

            # Save updated forecasts
            self.forecasts_df.to_csv(forecasts_file_path, index=False)
            click.secho(f"Forecasts saved to {forecasts_file_path}", fg="green", bold=True)
            click.secho("Forecasts downloaded and processed successfully!", fg="green", bold=True)

    def create_plan(self, plan_algo: PlanAlgorithm):
        """
        Every job needs to have a node and start time on that node assigned without collisions
        :return: Some kind of dictionary
        """
        click.secho(f"Running algo: {plan_algo}")
        if plan_algo == PlanAlgorithm.ALL:
            for plan in PlanAlgorithm:
                if plan == PlanAlgorithm.ALL or plan == PlanAlgorithm.RL_GREEN_SCHEDULER: continue
                planner = planner_factory(plan, self.associations_df, self.job_list, self.node_list)
                planner.plan()
        else:
            if plan_algo == PlanAlgorithm.BRUTE_FORCE_GREEN_CASE:
                planner = planner_factory(plan_algo, self.associations_df, self.job_list, self.node_list, mode='min')
            elif plan_algo == PlanAlgorithm.WORST_CASE:
                planner = planner_factory(plan_algo, self.associations_df, self.job_list, self.node_list, mode='max')
            else:
                planner = planner_factory(plan_algo, self.associations_df, self.job_list, self.node_list)
            planner.plan()

    def generate_energy_data(self):
        """Parallel execution with Click progress tracking"""
        tasks = [(node['name'], job['bytes'], job['id'])
                 for node in self.node_list
                 for job in self.job_list]

        with click.progressbar(
                length=len(tasks),
                label="Running energy simulations",
                show_pos=True,
                show_percent=True,
                bar_template='%(label)s  %(bar)s | %(info)s',
                fill_char='=',
                empty_char=' '
        ) as bar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
                futures = {}
                for node_name, job_size, job_id in tasks:
                    future = executor.submit(
                        self.simulator.run_simulation,
                        node_name,
                        1,  # flows
                        job_size,
                        job_id
                    )
                    futures[future] = (node_name, job_id)

                for future in concurrent.futures.as_completed(futures):
                    node_name, job_id = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        click.secho(
                            f"\nError in node={node_name}, job={job_id}: {str(e)}",
                            fg="red",
                            err=True
                        )
                    finally:
                        bar.update(1)  # Ensure progress always advances

    def read_in_associations_df(self, path):
        assoc_path = pathlib.Path(path)
        if assoc_path.exists():
            self.associations_df = pd.read_csv(path)

    def create_intervals(self, associations_df_name):
        data_list = []  # Collect data here

        # Group by both node_id and forecast_idx to get node-specific carbon intensities
        node_list = self.forecasts_df['node_id'].unique()
        node_forecast_ci = self.forecasts_df.groupby(['node_id', 'forecast_idx'])['ci'].mean().reset_index()

        # Create a lookup dictionary for faster access
        ci_lookup = {(row['node_id'], row['forecast_idx']): row['ci']
                     for _, row in node_forecast_ci.iterrows()}

        # Get all unique forecast indices
        forecast_indices = self.forecasts_df['forecast_idx'].unique()

        for forecast_idx in forecast_indices:
            for node_name in node_list:
                # Get the CI for this node at this forecast index
                ci_avg = ci_lookup.get((node_name, forecast_idx))

                if ci_avg is None:
                    # Fallback to global average for this forecast_idx if no node-specific data
                    ci_avg = self.forecasts_df[self.forecasts_df['forecast_idx'] == forecast_idx]['ci'].mean()
                    click.secho(f"No CI data for node {node_name} at forecast {forecast_idx}, using global average",
                                fg="yellow", dim=True)

                for job in self.job_list:
                    data_bits = int(job['bytes']) * 8
                    node_job_energy_data = self.simulator.parse_simulation_output(node_name, job['id'])
                    transfer_time_seconds = node_job_energy_data['transfer_duration']
                    throughput = data_bits / transfer_time_seconds  # bps
                    total_energy = int(node_job_energy_data['total_energy_hosts']) + int(
                        node_job_energy_data['total_link_energy'])
                    emissions = self.carbon_emissions_formula(total_energy, ci_avg)

                    # Append data to list
                    data_list.append({
                        "node": node_name,
                        "job_id": job['id'],
                        "forecast_id": forecast_idx,
                        "transfer_time": transfer_time_seconds,
                        "throughput": throughput,
                        "host_joules": node_job_energy_data['total_energy_hosts'],
                        "link_joules": node_job_energy_data['total_link_energy'],
                        "total_joules": total_energy,
                        "avg_ci": ci_avg,
                        "carbon_emissions": emissions
                    })

        # Convert list to DataFrame (efficient)
        associations_df = pd.DataFrame(data_list)
        associations_df_path = '../data/' + associations_df_name
        associations_df.to_csv(associations_df_path, index=False)
        click.secho(f"\nIntervals created successfully path {associations_df_path}", fg="green", bold=True)
        self.associations_df = associations_df
        return associations_df

    # SLA represents the baseline improvement the scheduled ci needs to experience
    def carbon_emissions_formula(self, joules, ci):
        kwh = joules / 3600000
        return kwh * ci


def get_unique_ips(pmeter_data: Dict[str,List[IpToLonAndLat]]) -> List[IpToLonAndLat]:
    unique_ips = set()
    for node_id, ip_objects in pmeter_data.items():
        for ip_object in ip_objects:
            unique_ips.add(IpToLonAndLat(ip=ip_object.ip, lat=ip_object.lat, lon=ip_object.lon, rtt=ip_object.rtt,
                                         ttl=ip_object.ttl, node_id=ip_object.node_id))

    return list(unique_ips)


# Factory function to instantiate correct class
def planner_factory(algo: PlanAlgorithm, *args, **kwargs):
    planners = {
        PlanAlgorithm.RANDOM: RandomPlanner,
        PlanAlgorithm.WORST_CASE: CarbonAwarePlanner,
        PlanAlgorithm.BRUTE_FORCE_GREEN_CASE: CarbonAwarePlanner,
        PlanAlgorithm.LINEAR_PROGRAMMING_GREEN: MixedIntegerLinearProgrammingGreenPlanner,
        PlanAlgorithm.BASIC_CASE: BasicPlanner,
        PlanAlgorithm.RL_GREEN_SCHEDULER: RLGreenScheduler
    }
    return planners[PlanAlgorithm(algo)](*args, **kwargs)  # Instantiate the selected planner
