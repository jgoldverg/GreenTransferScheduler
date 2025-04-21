import concurrent.futures
import json
import os.path

import click
import pandas as pd

from models import read_in_ip_map, get_unique_ips, IpOrderAndForecastData, carbon_emissions_formula
from simgrid_simulator import SimGridSimulator


class DataGenerator:

    def __init__(self, node_file_path, ip_list_file_path, job_file_path, update_forecasts, forecasts_path):
        self.forecasts_df = None
        self.simulator = None
        self.nodeid_to_map_traceroutes = None
        self.job_list = None
        self.node_map = None
        self.node_list = None
        self.node_file_path = node_file_path
        self.traceroute_path = ip_list_file_path
        self.job_file_path = job_file_path
        self.update_forecasts = update_forecasts
        self.forecasts_path = forecasts_path
        self.routers_ip = {}

    def prepare_fields(self):
        with open(self.node_file_path, 'r') as f:
            self.node_list = json.load(f)  # Parse the entire JSON array at once
        self.node_map = {}
        for node in self.node_list:
            self.node_map[node['name']] = node

        with open(self.job_file_path, 'r') as f:
            self.job_list = json.load(f)

        self.nodeid_to_map_traceroutes = read_in_ip_map(self.traceroute_path)
        self.ip_list = get_unique_ips(self.nodeid_to_map_traceroutes)

        self.simulator = SimGridSimulator(traceroute_data=self.nodeid_to_map_traceroutes, node_list=self.node_list,
                                          job_list=self.job_list)
        self.routers_ip, self.links_ip = self.simulator.create_xml_for_traceroute()

    def load_in_forecasts(self):
        if os.path.exists(self.forecasts_path):
            try:
                self.forecasts_df = pd.read_csv(self.forecasts_path)
                click.secho(f"Loaded {len(self.forecasts_df)} past forecasts from {self.forecasts_path}")
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
            self.forecasts_df.to_csv(self.forecasts_path, index=False)
            click.secho(f"Forecasts saved to {self.forecasts_path}", fg="green", bold=True)
            click.secho("Forecasts downloaded and processed successfully!", fg="green", bold=True)

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

    def create_intervals(self, df_path):
        data_list = []  # Collect data here

        # Group by both node_id and forecast_idx to get node-specific carbon intensities
        node_list = []
        for node in self.node_list:
            node_name = node['name']
            if "macmini" in node_name:
                continue
            else:
                node_list.append(node_name)

        self.forecasts_df = self.forecasts_df[self.forecasts_df['node_id'].isin(node_list)]
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
                    emissions = self.emissions_for_path_forecast(node_name, job['id'], forecast_idx)
                    # emissions = carbon_emissions_formula(total_energy, ci_avg)

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
        associations_df.to_csv(df_path, index=False)
        click.secho(f"\nIntervals created successfully path {df_path}", fg="green", bold=True)
        self.associations_df = associations_df
        return associations_df

    def emissions_for_path_forecast(self, node_name, job_id, forecast_id):
        """Calculate CO₂ emissions for a path forecast based on energy consumption and carbon intensity data.

        Args:
            node_name: Name of the node
            job_id: ID of the job
            forecast_id: ID of the forecast to use for carbon intensity data

        Returns:
            Total emissions in grams of CO₂ for the path, or None if data is missing
        """
        try:
            # Construct path - using consistent format with the comment example
            path = f"../data/energy_consumption_{node_name}_{job_id}_.json"

            # Load energy data
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
            except FileNotFoundError:
                print(f"Energy consumption file not found: {path}")
                return None
            except json.JSONDecodeError:
                print(f"Invalid JSON in file: {path}")
                return None

            hosts = data.get('hosts', {})
            links = data.get('links', {})

            if node_name not in self.nodeid_to_map_traceroutes:
                print(f"No traceroute data found for node: {node_name}")
                return None

            node_traceroute = self.nodeid_to_map_traceroutes[node_name]
            total_emissions_g_co2 = 0.0
            found_ci_data = False

            for hop in node_traceroute:
                ip = hop.ip
                hop_name = self.routers_ip.get(ip, None)
                if hop_name is None:
                    continue

                link_name = self.links_ip.get(ip, None)
                if link_name is None:
                    continue

                host_energy_joules = hosts.get(hop_name, 0.0)
                link_energy_joules = links.get(link_name, 0.0)

                ci_hop = self.forecasts_df[
                    (self.forecasts_df['forecast_idx'] == forecast_id) &
                    (self.forecasts_df['node_id'] == node_name) &
                    (self.forecasts_df['ip'] == ip)
                    ]
                click.secho(f"IP: {ip} node_name:{node_name} link_name:{link_name}")
                if not ci_hop.empty:
                    ci_value = ci_hop.iloc[0]['ci']  # gCO₂/kWh
                    # Convert energy (J) to kWh
                    total_energy_kwh = (host_energy_joules + link_energy_joules) / 3.6e6
                    # Compute emissions in grams of CO₂
                    emissions_g_co2 = total_energy_kwh * ci_value
                    total_emissions_g_co2 += emissions_g_co2
                    found_ci_data = True
                else:
                    print(f"CI data not found for hop {ip}")

            return total_emissions_g_co2 if found_ci_data else None

        except Exception as e:
            print(f"Error calculating emissions: {str(e)}")
            return None
