import json
import os.path
from typing import List
import pandas as pd
import pickle

import click
from models import IpToLonAndLat, IpOrderAndForecastData, read_in_node_file, read_in_job_file, read_in_ip_map, \
    parse_speed_to_bps, convert_to_bits


class SchedulerAlgo:

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

        # Load in Jobs
        self.job_list = read_in_job_file(job_file_path)
        click.secho(f"Loaded {len(self.job_list)} Jobs", fg="green")

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
                self.forecasts[ipCoord] = ipForecast
                for idx, forecast in enumerate(forecast_list):
                    forecast_entries.append({
                        'timestamp': forecast['timestamp'],
                        'ci': forecast['ci'],
                        'ip': forecast['ip'],
                        'forecast_idx': idx
                    })
                # click.secho(f"Processed {idx}/{len(self.ip_list)}: {ipCoord.ip}", fg="blue", dim=True)
        self.forecasts_df = pd.DataFrame(forecast_entries)

        if forecasts_file_path:
            self.forecasts_df.to_json(forecasts_file_path)
            click.secho(f"Forecasts saved to {forecasts_file_path}")

        click.secho("Forecasts downloaded and processed successfully!", fg="green", bold=True)

    def create_plan(self):
        """
        Every job needs to have a node and start time on that node assigned without collisions
        :return: Some kind of dictionary
        """
        all_jobs_assigned = 0
        job_assignment_map = {}
        while True:
            if all_jobs_assigned == len(self.job_list): break

    def create_intervals(self):
        data_list = []  # Collect data here
        forecast_idx = 0
        for idx, row in self.forecasts_df.iterrows():
            filtered_df = self.forecasts_df[(self.forecasts_df['forecast_idx'] == forecast_idx) & (self.forecasts_df['ip'].isin(self.ip_list))]
            ci_avg = filtered_df.mean()
            click.secho(f"\nProcessing Forecast {forecast_idx}/{len(self.forecasts)} with CI Avg={ci_avg}", fg="cyan", bold=True)

            for node in self.node_list:
                nic_speed_bps = parse_speed_to_bps(node["NIC_SPEED"])
                click.secho(f"  Node: {node['name']} (NIC Speed: {node['NIC_SPEED']})", fg="green")

                for job in self.job_list:
                    data_bits = convert_to_bits(job["bytes"])
                    transfer_time_seconds = data_bits / nic_speed_bps
                    joules_for_node_max = transfer_time_seconds * node['power']['max']
                    joules_for_node_min = transfer_time_seconds * node['power']['min']
                    emissions = self.carbon_emissions_formula(joules_for_node_max, ci_avg)

                    # Log key details
                    click.secho(
                        f"    Job {job['id']} -> Transfer: {transfer_time_seconds:.2f}s, {data_bits}Bps, Emissions: {emissions:.2f} kg CO2",
                        fg="yellow")
                    click.secho(f"        Details: JoulesMin: {joules_for_node_min}, JoulesMax: {joules_for_node_max}")
                    click.secho(
                        f"Forecast {forecast_idx} | Node: {node['name']} | Job: {job['id']} | Emissions: {emissions:.2f} kg CO2")

                    # Append data to list
                    data_list.append({
                        "node": node['name'],
                        "job_id": job['id'],
                        "forecast_id": forecast_idx,
                        "transfer_time": transfer_time_seconds,
                        "throughput": nic_speed_bps,
                        "max_energy": joules_for_node_max,
                        "min_energy": joules_for_node_min,
                        "avg_ci": ci_avg,
                        "carbon_emissions": emissions
                    })

            forecast_idx += 1

        # Convert list to DataFrame (efficient)
        associations_df = pd.DataFrame(data_list)

        click.secho("\nIntervals created successfully!", fg="green", bold=True)
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
        for ip_object in ip_objects:
            unique_ips.add(IpToLonAndLat(ip=ip_object.ip, lat=ip_object.lat, lon=ip_object.lon))

    return list(unique_ips)
