import os.path
from typing import List
import pandas as pd
from pathlib import Path
import click
from datetime import datetime
from models import IpToLonAndLat, IpOrderAndForecastData, read_in_node_file, read_in_job_file, read_in_ip_map, \
    parse_speed_to_bps
from algos import PlanAlgorithm
from simgrid_simulator import SimGridSimulator
from algos import planner_factory


class Scheduler:

    def __init__(self, node_file_path, ip_list_file_path, job_file_path):
        # Load in Nodes
        self.node_list = read_in_node_file(node_file_path)
        click.secho(f"Loaded {len(self.node_list)} Nodes", fg="green")

        # Load in Traceroute Measurements
        ip_pmeter_measurements_traceroute = read_in_ip_map(ip_list_file_path)
        self.ip_list = get_unique_ips(ip_pmeter_measurements_traceroute)
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
        self.forecasts: dict[IpToLonAndLat, IpOrderAndForecastData] = {}

    def load_in_forecasts(self, forecasts_file_path=None):
        if os.path.exists(forecasts_file_path):
            self.forecasts_df = pd.read_json(forecasts_file_path)
            click.secho(f"Loaded forecasts from {forecasts_file_path}, entries loaded {len(self.forecasts_df)}")
            return

        click.secho("Downloading forecasts from Electricity Maps...", fg="yellow", bold=True)
        ipForecast = IpOrderAndForecastData()
        forecast_entries = []
        with click.progressbar(self.ip_list, show_eta=True, show_percent=True) as ips:
            for ipCoord in ips:
                forecast_list = ipForecast.fetch_forecast_for_ip(ipCoord)
                click.secho(forecast_list)
                self.forecasts[ipCoord] = ipForecast
                for idx, forecast in enumerate(forecast_list):
                    forecast_entries.append({
                        'timestamp': forecast['timestamp'],
                        'ci': forecast['ci'],
                        'ip': forecast['ip'],
                        'forecast_idx': idx
                    })
                click.secho(f"Processed {idx}/{len(self.ip_list)}: {ipCoord.ip}", fg="blue", dim=True)
        self.forecasts_df = pd.DataFrame(forecast_entries)

        if forecasts_file_path:
            self.forecasts_df.to_json(forecasts_file_path + f"_{datetime.now().isoformat()}.json")
            click.secho(f"Forecasts saved to {forecasts_file_path}")

        click.secho("Forecasts downloaded and processed successfully!", fg="green", bold=True)

    def create_plan(self, plan_algo: PlanAlgorithm):
        """
        Every job needs to have a node and start time on that node assigned without collisions
        :return: Some kind of dictionary
        """
        planner = planner_factory(plan_algo, self.simulator, self.associations_df, self.node_list)
        planner.plan()

    def generate_energy_data(self):
        # Generates all energy data for jobs.
        for node in self.node_list:
            for job in self.job_list:
                self.simulator.run_simulation(node['name'], 1, job['bytes'], job['id'])

    def create_intervals(self):
        data_list = []  # Collect data here
        forecast_idx = 0
        click.secho(f"{self.forecasts_df}")
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
        associations_df.to_csv(association_path)
        click.secho(f"\nIntervals created successfully path {association_path}", fg="green", bold=True)
        self.associations_df = associations_df
        return associations_df  # Return the DataFrame

    # def create_plan(self):
    #     for ipCoord in self.list_ip_coord:
    #         measurement_forecast = IpOrderAndForecastData(ipCoord)
    #         measurement_forecast.create_and_populate_forecast()

    # def visulize_ci_matrix(self, forecast, entry='throughput'):
    #     # Create a new matrix with just the throughput values
    #     throughput_matrix = self.ci_matrix.applymap(lambda x: x[entry] if isinstance(x, dict) else 0)
    #
    #     # Create the heatmap
    #     plt.figure(figsize=(12, 8))
    #     sns.heatmap(throughput_matrix, annot=True, fmt=".2f", cmap="viridis", cbar_kws={'label': 'Throughput (bps)'})
    #
    #     # Add titles and labels
    #     plt.title("Node-Job Throughput Heatmap")
    #     plt.xlabel("Job ID")
    #     plt.ylabel("Node Name")
    #
    #     # Show the plot
    #     plt.savefig(f"graphs/ci_heat_graph_{forecast}.png")

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
