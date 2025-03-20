import os.path
from enum import Enum
from typing import List
import pandas as pd
import click
from models import IpToLonAndLat, IpOrderAndForecastData, read_in_node_file, read_in_job_file, read_in_ip_map
from simgrid_simulator import SimGridSimulator

from algos.milp_green import MixedIntegerLinearProgrammingGreenPlanner
from algos.basic_planner import BasicPlanner
from algos.random_planner import RandomPlanner
from algos.brute_force_green_planner import BruteForceGreenPlanner
from algos.worst_case_planner import WorstCasePlanner


class PlanAlgorithm(Enum):
    BASIC_CASE = "basic"
    RANDOM = "random"
    WORST_CASE = "worst"
    BRUTE_FORCE_GREEN_CASE = "green"
    LINEAR_PROGRAMMING_GREEN = "milp_green"
    ALL = "all"


class Scheduler:

    def __init__(self, node_file_path, ip_list_file_path, job_file_path, update_forecasts):
        # Load in Nodes
        self.node_list = read_in_node_file(node_file_path)
        click.secho(f"Loaded {len(self.node_list)} Nodes", fg="green")

        # Load in Traceroute Measurements
        ip_pmeter_measurements_traceroute = read_in_ip_map(ip_list_file_path)
        self.ip_list = get_unique_ips(ip_pmeter_measurements_traceroute)
        self.update_forecasts = update_forecasts
        click.secho(
            f"Loaded Total Traceroute Measurements {len(ip_pmeter_measurements_traceroute)} reduced to Unique IP's {len(self.ip_list)}",
            fg="green")

        self.job_list = read_in_job_file(job_file_path)
        click.secho(f"Loaded {len(self.job_list)} Jobs", fg="green")

        self.simulator = SimGridSimulator(traceroute_data=ip_pmeter_measurements_traceroute, node_list=self.node_list,
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
                            'forecast_idx': idx
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
                if plan == PlanAlgorithm.ALL: continue
                planner = planner_factory(plan, self.associations_df, self.job_list, self.node_list)
                planner.plan()
        else:
            planner = planner_factory(plan_algo, self.associations_df, self.job_list, self.node_list)
            planner.plan()

    def generate_energy_data(self):
        # Generates all energy data for jobs.
        for node in self.node_list:
            for job in self.job_list:
                self.simulator.run_simulation(node['name'], 1, job['bytes'], job['id'])

    def create_intervals(self):
        data_list = []  # Collect data here
        forecast_idx = 0
        click.secho(f"Forecasts df: {self.forecasts_df}")
        mean_per_forecast = self.forecasts_df.groupby('forecast_idx')['ci'].mean()
        for ci_avg in mean_per_forecast:
            for node in self.node_list:
                for job in self.job_list:
                    data_bits = int(job['bytes']) * 8
                    node_job_energy_data = self.simulator.parse_simulation_output(node['name'], job['id'])
                    transfer_time_seconds = node_job_energy_data['transfer_duration']
                    throughput = data_bits / transfer_time_seconds  # bps
                    total_energy = int(node_job_energy_data['total_energy_hosts']) + int(
                        node_job_energy_data['total_link_energy'])
                    emissions = self.carbon_emissions_formula(total_energy, ci_avg)

                    # Append data to list
                    data_list.append({
                        "node": node['name'],
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
            forecast_idx += 1

        # Convert list to DataFrame (efficient)
        associations_df = pd.DataFrame(data_list)
        association_path = '/workspace/data/associations_df.csv'
        associations_df.to_csv(association_path, index=False)
        click.secho(f"\nIntervals created successfully path {association_path}", fg="green", bold=True)
        self.associations_df = associations_df
        return associations_df  # Return the DataFrame

    # SLA represents the baseline improvement the scheduled ci needs to experience
    def carbon_emissions_formula(self, joules, ci):
        kwh = joules / 3600000
        return kwh * ci


def get_unique_ips(pmeter_data: List[List[IpToLonAndLat]]) -> List[IpToLonAndLat]:
    unique_ips = set()
    for ip_objects in pmeter_data:
        print(ip_objects)
        for ip_object in ip_objects:
            unique_ips.add(IpToLonAndLat(ip=ip_object.ip, lat=ip_object.lat, lon=ip_object.lon, rtt=ip_object.rtt,
                                         ttl=ip_object.ttl))

    return list(unique_ips)


# Factory function to instantiate correct class
def planner_factory(algo: PlanAlgorithm, *args, **kwargs):
    planners = {
        PlanAlgorithm.RANDOM: RandomPlanner,
        PlanAlgorithm.WORST_CASE: WorstCasePlanner,
        PlanAlgorithm.BRUTE_FORCE_GREEN_CASE: BruteForceGreenPlanner,
        PlanAlgorithm.LINEAR_PROGRAMMING_GREEN: MixedIntegerLinearProgrammingGreenPlanner,
        PlanAlgorithm.BASIC_CASE: BasicPlanner
    }
    return planners[PlanAlgorithm(algo)](*args, **kwargs)  # Instantiate the selected planner
