import concurrent.futures
import json
import math
import multiprocessing
from functools import partial
from typing import Dict, List

import click
import pandas as pd

from models import get_unique_ips, IpToLonAndLat, process_pmeter_tr
from simgrid_simulator import SimGridSimulator
from zone_discovery import HistoricalForecastService


def _run_simulation_task(args, simulator):
    """Standalone function that can be pickled for multiprocessing"""
    route_key, job_size, job_id = args
    try:
        simulator.run_simulation(
            node_name=route_key,
            flows=1,
            job_size=job_size,
            job_id=job_id
        )
        return True
    except Exception as e:
        click.secho(f"\nFailed to simulate {route_key} job {job_id}: {str(e)}", fg='red')
        return False


def _process_single_combination(args, simulator, node_map, forecasts_df, emissions_for_path_forecast):
    """Process a single (route, job, forecast) combination - standalone function for multiprocessing"""
    route_key, job, fidx = args
    source_node, destination_node = route_key.split('_')

    # Validate nodes exist
    if source_node not in node_map or destination_node not in node_map:
        return None

    # Get simulation results
    energy_json = simulator.parse_simulation_output(route_key, job['id'])
    if not energy_json:
        return None

    # Calculate metrics
    transfer_time_seconds = energy_json['transfer_duration']
    job_size_bytes = energy_json['job_size_bytes']
    throughput = (job_size_bytes * 8) / transfer_time_seconds if transfer_time_seconds > 0 else 0
    total_energy = int(energy_json['total_energy_hosts']) + int(energy_json['total_link_energy'])

    # Calculate emissions
    emissions = emissions_for_path_forecast(route_key, job['id'], fidx, energy_json)
    if emissions is None:
        return None

    return {
        "source_node": source_node,
        "destination_node": destination_node,
        "route_key": route_key,
        "job_id": job['id'],
        "forecast_id": fidx,
        "transfer_time": transfer_time_seconds,
        "throughput": throughput,
        "host_joules": energy_json['total_energy_hosts'],
        "link_joules": energy_json['total_link_energy'],
        "total_joules": total_energy,
        "carbon_emissions": emissions,
        'job_deadline': job['deadline'],
        'source_cpu': node_map[source_node]['CPU'],
        'source_ram': node_map[source_node]['total_ram'],
        'source_nic_speed': node_map[source_node]['NIC_SPEED'],
        'destination_cpu': node_map[destination_node]['CPU'],
        'destination_ram': node_map[destination_node]['total_ram'],
        'destination_nic_speed': node_map[destination_node]['NIC_SPEED'],
    }


class DataGenerator:

    def __init__(self, node_file_path, ip_list_file_path, job_file_path, forecast_service: HistoricalForecastService,
                 forecast_len=24):
        self.forecasts_df = None
        self.simulator = None
        self.nodeid_to_map_traceroutes: Dict[str, List[IpToLonAndLat]] = None
        self.job_list = None
        self.node_map = None
        self.node_list = None
        self.node_file_path = node_file_path
        self.traceroute_path = ip_list_file_path
        self.job_file_path = job_file_path
        self.routers_ip = {}
        self.links_ip = {}
        self.forecast_service = forecast_service
        self.forecast_len = forecast_len

    def prepare_fields(self):
        with open(self.node_file_path, 'r') as f:
            self.node_list = json.load(f)  # Parse the entire JSON array at once
        self.node_map = {}
        for node in self.node_list:
            self.node_map[node['name']] = node

        with open(self.job_file_path, 'r') as f:
            self.job_list = json.load(f)

        self.nodeid_to_map_traceroutes: Dict[str, List[IpToLonAndLat]] = process_pmeter_tr(self.traceroute_path)
        click.secho(
            f"Node Id Map to Traceroute: Keys={self.nodeid_to_map_traceroutes.keys()} the len of values are {len(self.nodeid_to_map_traceroutes)}",
            fg='cyan')
        self.ip_list = get_unique_ips(self.nodeid_to_map_traceroutes)

        self.simulator = SimGridSimulator(traceroute_data=self.nodeid_to_map_traceroutes, node_list=self.node_list,
                                          job_list=self.job_list)
        self.routers_ip, self.links_ip = self.simulator.create_xml_for_traceroute()

    def load_in_forecasts(self):
        self.forecasts_df = pd.DataFrame()
        for _, traceroute in self.nodeid_to_map_traceroutes.items():
            local_df = self.forecast_service.ips_to_forecasts(traceroute)
            self.forecasts_df = pd.concat([self.forecasts_df, local_df], ignore_index=True)
        self.forecasts_df.drop_duplicates(inplace=True)
        self.forecasts_df.to_csv('/workspace/data/forecast_data.csv')

    def generate_energy_data(self, mode='time', max_workers=20):
        # Create tasks for all source-destination routes and jobs
        tasks = []
        for route_key in self.simulator.traceroute_data.keys():
            try:
                source_node, destination_node = route_key.split('_')

                if source_node not in self.node_map or destination_node not in self.node_map:
                    click.secho(f"Skipping invalid route {route_key} - nodes not found", fg='yellow')
                    continue

                if self.node_map[source_node].get('type') != 'source':
                    continue

                for job in self.job_list:
                    tasks.append((route_key, job['bytes'], job['id']))

            except ValueError:
                click.secho(f"Invalid route key format: {route_key}", fg='red')
                continue

        if not tasks:
            click.secho("No valid routes found for energy simulation!", fg='red', bold=True)
            return False

        # Parallel execution
        with click.progressbar(
                length=len(tasks),
                label="Running energy simulations",
                show_pos=True,
                show_percent=True,
                bar_template='%(label)s  %(bar)s | %(info)s',
                fill_char='=',
                empty_char=' '
        ) as bar:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Use the standalone function instead of local one
                worker = partial(_run_simulation_task, simulator=self.simulator)
                futures = [executor.submit(worker, task) for task in tasks]

                for future in concurrent.futures.as_completed(futures):
                    bar.update(1)
                    future.result()  # Raise exceptions if any

        return True

    def create_intervals_historical(self, df_path, max_workers=20):
        """Parallel version of historical intervals creation"""
        route_keys = []
        for route_key in self.simulator.traceroute_data.keys():
            source_node, destination_node = route_key.split('_')
            if source_node in self.node_map and destination_node in self.node_map:
                route_keys.append(route_key)

        forecast_idx = self.forecasts_df['forecast_idx'].unique()
        total_iterations = len(route_keys) * len(self.job_list) * len(forecast_idx)
        data_list = []

        # Prepare all combinations to process
        all_combinations = [
            (route_key, job, fidx)
            for route_key in route_keys
            for job in self.job_list
            for fidx in forecast_idx
        ]

        with click.progressbar(
                length=total_iterations,
                label='Processing routes and jobs...',
                show_percent=True,
                show_pos=True,
                width=50
        ) as bar:
            with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
                # Create partial function with required context
                worker = partial(
                    _process_single_combination,
                    simulator=self.simulator,
                    node_map=self.node_map,
                    forecasts_df=self.forecasts_df,
                    emissions_for_path_forecast=self.emissions_for_path_forecast
                )

                # Submit all tasks
                futures = {executor.submit(worker, combo): combo for combo in all_combinations}

                # Process results as they complete
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        data_list.append(result)
                    bar.update(1)

        # Create DataFrame and save results (same as original)
        associations_df = pd.DataFrame(data_list)
        associations_df['throughput_gbps'] = associations_df['throughput'] / 1e9
        associations_df['transfer_time_hours'] = associations_df['transfer_time'] / 3600
        associations_df.to_csv(df_path, index=False)

        click.secho(f"\nIntervals created successfully at {df_path}", fg="green", bold=True)
        self.associations_df = associations_df
        return associations_df

    def emissions_for_path_forecast(self, route_key, job_id, forecast_id, energy_data):
        """Calculate CO₂ emissions for a path forecast."""
        # Get transfer duration
        transfer_duration = energy_data['transfer_duration']
        transfer_hours = transfer_duration / 3600
        num_hours = min(math.ceil(transfer_hours), self.forecast_len)

        # Get source node and traceroute
        source_node = route_key.split('_')[0]
        destination_name = route_key.split('_')[1]
        traceroute = self.nodeid_to_map_traceroutes[route_key]

        # Get route-specific mappings
        route_routers = self.routers_ip.get(route_key)
        route_links = self.links_ip.get(route_key)

        total_emissions = 0.0

        for i, hop in enumerate(traceroute):
            ip = hop.ip
            # Determine component names
            if i == 0:  # Source node
                host_name = source_node
                link_name = None
            elif i == len(traceroute) - 1:  # Destination node
                host_name = destination_name
                link_name = None
            else:  # Router node
                host_name = route_routers.get(ip, f"router_{route_key}_{i}")
                link_name = route_links.get(ip, f"link_{route_key}_{i}")

            # Get energy values
            host_energy = energy_data['hosts'].get(host_name)
            link_energy = energy_data['links'].get(link_name, 0.0)
            total_energy = host_energy + link_energy

            # Calculate hourly emissions
            hourly_energy = total_energy / transfer_hours
            hop_emissions = 0.0

            for hour_offset in range(num_hours):
                current_fidx = (forecast_id + hour_offset) % self.forecast_len

                # Get CI data
                ci_data = self.forecasts_df[
                    (self.forecasts_df['forecast_idx'] == current_fidx) &
                    (self.forecasts_df['ip'] == ip)
                    ]

                ci_value = ci_data.iloc[0]['ci']
                if hour_offset == num_hours - 1:  # Partial last hour
                    hour_frac = transfer_hours - (num_hours - 1)
                    energy_kwh = (hourly_energy * hour_frac) / 3.6e6
                else:
                    energy_kwh = hourly_energy / 3.6e6

                hop_emissions += energy_kwh * ci_value

            total_emissions += hop_emissions

        if total_emissions is None or total_emissions == 0.0:
            click.secho(f"{route_key}, {job_id}, {forecast_id}")
        return total_emissions
