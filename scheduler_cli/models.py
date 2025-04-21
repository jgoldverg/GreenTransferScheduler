import os
from pathlib import Path
from typing import List, Union, Dict
import json
import pandas as pd
import click
import requests


# def convert_to_bits(size):
#     units = {"KB": 10 ** 3, "MB": 10 ** 6, "GB": 10 ** 9, "TB": 10 ** 12}
#     size, unit = size[:-2], size[-2:].upper()
#     return int(size) * units[unit] * 8  # Convert to bits


# Utility to convert NIC speed to bps
def parse_speed_to_bps(speed_str):
    # Extract the numeric value and the unit
    speed_str = speed_str.strip().upper()
    if not speed_str.endswith("BPS"):
        raise ValueError("Invalid speed format. Must end with 'bps', e.g., '1Gbps'.")

    value = float(speed_str[:-4])  # Get the numeric part (strip last 4 chars: 'BPS')
    unit = speed_str[-4:]  # Extract the unit (bps)

    # Convert based on unit prefix
    if "G" in speed_str:
        return int(value * 1_000_000_000)  # Gbps to bps
    elif "M" in speed_str:
        return int(value * 1_000_000)  # Mbps to bps
    elif "K" in speed_str:
        return int(value * 1_000)  # Kbps to bps
    elif "BPS" in speed_str:
        return int(value)  # Already in bps
    else:
        raise ValueError("Unsupported unit in speed string.")


# Key class for all forecasts related to this IP: Lat and Lon pair
class IpToLonAndLat:
    def __init__(self, ip, lon, lat, rtt, ttl, node_id):
        self.ip = ip
        self.lon = lon
        self.lat = lat
        self.rtt = rtt
        self.ttl = ttl
        self.node_id = node_id

    def __eq__(self, other):
        # Define equality based on the attributes of the object
        if isinstance(other, IpToLonAndLat):
            return (self.ip, self.lon, self.lat, self.rtt, self.ttl, self.node_id) == (
                other.ip, other.lon, other.lat, other.rtt, other.ttl, other.node_id)
        return False

    def __hash__(self):
        # Define a hash based on the attributes of the object
        return hash((self.ip, self.lon, self.lat))

    def __repr__(self):
        return f"IpToLonAndLat(ip='{self.ip}', lon={self.lon}, lat={self.lat})"

    def to_dict(self):
        """Converts the IpToLonAndLat instance to a dictionary."""
        return {
            'ip': self.ip,
            'lon': self.lon,
            'lat': self.lat,
            'rtt': self.rtt,
            'ttl': self.ttl,
            'node_id': self.node_id
        }


# The value and forecast entry for and IPCoordinate
class ForecastEntry:
    def __init__(self, timestamp, ci):
        self.timestamp = timestamp
        self.ci = ci

    def __str__(self):
        # Used in print() and user-friendly output
        return f"TimeStamp: {self.timestamp}, Carbon Intensity: {self.ci}"

    def __repr__(self):
        # Used in lists and for developer-friendly output
        return f"TimeStamp: {self.timestamp}, Carbon Intensity: {self.ci}"


class IpOrderAndForecastData:

    def __init__(self, ipCoordinate: IpToLonAndLat = None):
        self.forecast_list: List[ForecastEntry] = []
        self.ipCoordinate = ipCoordinate

    def fetch_forecast_for_ip(self, ipCoordinate: IpToLonAndLat):
        """
        Helper method to fetch forecast data for a single IP coordinate.
        """
        headers = {"auth-token": os.getenv("ELECTRICITY_MAPS_FORECAST_TOKEN")}
        param = {"lon": ipCoordinate.lon, "lat": ipCoordinate.lat, "horizonHours": 24}
        resp = requests.get("https://api.electricitymap.org/v3/carbon-intensity/forecast", params=param,
                            headers=headers)
        if 199 < resp.status_code < 300:
            data = resp.json()
            forecast_list = data.get('forecast', [])
            results = []
            for entry in forecast_list:
                results.append({
                    "ip": ipCoordinate.ip,
                    "timestamp": entry['datetime'],
                    "ci": entry['carbonIntensity']
                })
            return results
        else:
            click.secho(f"\nElectricity Maps Gave status code: {resp.status_code} with body {resp.text}")
            raise Exception(resp.text)

    def create_and_populate_forecast(self, forecasts_file_path):
        list_json_forecast = self.fetch_forecast_for_ip(self.ipCoordinate)

        for json_forecast in list_json_forecast:
            forecast_entry = ForecastEntry(json_forecast['timestamp'], json_forecast['ci'])
            self.forecast_list.append(forecast_entry)

    def average(self) -> float:
        # Calculate the average carbon intensity (ci)
        if len(self.forecast_list) == 0:
            return 0.0

        total_ci = 0.0
        for forecast in self.forecast_list:
            total_ci += forecast.ci

        average_ci = total_ci / len(self.forecast_list)
        return average_ci

    def populate_from_data(self, forecast_data):
        # Clear existing forecast list to avoid duplicates when populating from data
        self.forecast_list.clear()
        # Loop through the forecast data and populate forecast entries
        for entry in forecast_data:
            timestamp = entry.get("timestamp")
            ci = entry.get("ci")
            # Assuming that timestamp is a string and ci is a numeric value
            if timestamp and ci is not None:
                forecast_entry = ForecastEntry(timestamp, ci)
                self.forecast_list.append(forecast_entry)
            else:
                click.secho(f"Missing data for IP {self.ipCoordinate.ip}, skipping entry.", fg="red")


def read_in_ip_map(ip_path: Union[str, Path]) -> Dict[str, List[IpToLonAndLat]]:
    # If the path is a directory, process all JSON files inside
    node_to_traceroute: Dict[str, List[IpToLonAndLat]] = {}

    if os.path.isdir(ip_path):
        json_files = [f for f in os.listdir(ip_path) if f.endswith(".json")]
        for file in json_files:
            file_path = os.path.join(ip_path, file)
            ip_objects = process_single_file(file_path)
            node_id = ip_objects[0].node_id
            node_to_traceroute[node_id] = ip_objects

    # If it's a single file, process it directly
    elif os.path.isfile(ip_path):
        ip_objects = process_single_file(ip_path)
        node_to_traceroute[ip_objects[0].node_id] = ip_objects
    else:
        print(f"Invalid path: {ip_path}")

    return node_to_traceroute


def process_single_file(file_path: str) -> List[IpToLonAndLat]:
    """Helper function to process a single JSON file."""
    head, tail = os.path.split(file_path)
    traceroute_measurement = tail.split(".json")[0]
    traceroute_measurement = traceroute_measurement.split("_")[0]
    try:
        with open(file_path, 'r') as f:
            pmeter_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON in {file_path}: {e}")
        return []

    ip_obj_list = []
    for key, value in pmeter_data.items():
        if key in ['time', 'node_id', 'job_id']:
            continue  # Skip metadata keys

        ip_obj = IpToLonAndLat(
            ip=key,
            lon=value['lon'],
            lat=value['lat'],
            rtt=value['rtt'],
            ttl=value['ttl'],
            node_id=traceroute_measurement
        )
        ip_obj_list.append(ip_obj)

    return ip_obj_list


def read_in_node_file(node_path):
    with open(node_path, 'r') as f:
        node_list = json.load(f)  # Parse the entire JSON array at once
    return node_list


def read_in_node_list_to_map(node_path):
    with open(node_path, 'r') as f:
        node_list = json.load(f)
        node_map = {}
        for node in node_list:
            node_name = node['name']
            if node_name not in node_map:
                node_map[node_name] = node

        return node_map


def read_in_job_file(job_file_path):
    with open(job_file_path, 'r') as f:
        job_list = json.load(f)
    return job_list


def get_unique_ips(pmeter_data: Dict[str, List[IpToLonAndLat]]) -> List[IpToLonAndLat]:
    unique_ips = set()
    for node_id, ip_objects in pmeter_data.items():
        for ip_object in ip_objects:
            unique_ips.add(IpToLonAndLat(ip=ip_object.ip, lat=ip_object.lat, lon=ip_object.lon, rtt=ip_object.rtt,
                                         ttl=ip_object.ttl, node_id=ip_object.node_id))

    return list(unique_ips)


def carbon_emissions_formula(joules, ci):
    kwh = joules / 3600000
    return kwh * ci



