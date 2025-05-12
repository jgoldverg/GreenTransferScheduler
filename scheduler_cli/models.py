import os
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Union, Dict, Tuple, Optional
import json
import click
import requests


class PlanAlgorithm(Enum):
    GNN_CARBON_MIND = "gnn"
    EARLIEST_DEADLINE_FIRST = "edf"
    SHORTEST_JOB_FIRST = "sjf"
    WORST_CASE = "worst"
    BRUTE_FORCE_GREEN_CASE = "green"
    MILP_NORM = "milp_norm"
    MILP_BINARY = "milp_binary"
    ALL = "all"
    ROUND_ROBIN = "rr"


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


def geo_locate_ips(ip_list) -> Dict[str, Tuple[float, float]]:
    """Geolocate IPs with smart fallback to nearest valid coordinates"""
    if not ip_list:
        return {}

    # Batch geolocation request
    payload = [{"query": ip} for ip in ip_list]
    response = requests.post("http://ip-api.com/batch", json=payload)
    geo_data = response.json()

    coord_map = {}

    # First pass: mark successful geolocations
    for result in geo_data:
        ip = result['query']
        if result.get('status') == 'success':
            coord_map[ip] = (result['lat'], result['lon'])
        else:
            coord_map[ip] = None  # Mark as needing fallback

    # Second pass: fill gaps with nearest valid coordinates
    for i, ip in enumerate(ip_list):
        if coord_map[ip] is None:
            # Look backward first
            for j in range(i - 1, -1, -1):
                if coord_map[ip_list[j]]:
                    coord_map[ip] = coord_map[ip_list[j]]
                    break
            # If nothing behind, look forward
            if coord_map[ip] is None:
                for j in range(i + 1, len(ip_list)):
                    if coord_map[ip_list[j]]:
                        coord_map[ip] = coord_map[ip_list[j]]
                        break
    return coord_map


def process_mtr_files(dir_path='/workspace/config/traceroutes/formal_mtr/') -> Dict[str, List[IpToLonAndLat]]:
    """Process MTR files with clean debug output"""
    file_name_to_data: Dict[str, List[IpToLonAndLat]] = {}
    files = [f for f in os.listdir(dir_path) if f.endswith('.json')]

    # Debug: Print file list
    click.secho("=== Files to Process ===", fg='cyan')
    click.secho(f"Found {len(files)} JSON files in directory:", fg='cyan')
    for i, file in enumerate(sorted(files), 1): click.secho(f"{i}. {file}", fg='cyan')

    for file in files:
        # File processing header
        click.secho(f"=== Processing: {file} ===", fg='yellow')

        # Parse filename
        parts = file.split('_')
        source_name = parts[0]
        dest_name = parts[-2]
        click.secho(f"Source: {source_name} → Destination: {dest_name}", fg='blue')

        # Load MTR data
        file_path = os.path.join(dir_path, file)
        with open(file_path, 'r') as f:
            mtr_data = json.load(f)

        # Extract unique IPs
        ips = list({hop["host"] for hop in mtr_data["report"]["hubs"] if "host" in hop})

        # Geolocate IPs
        click.secho("Geolocating IPs...", fg='blue')
        geo_map = geo_locate_ips(ips)

        trace_list: List[IpToLonAndLat] = []

        click.secho("Processing hops:", fg='blue')
        for hop in mtr_data["report"]["hubs"]:
            if "host" not in hop:
                continue

            ip = hop["host"]
            ip_lat_long = IpToLonAndLat(
                ip=ip,
                lat=geo_map[ip][0],
                lon=geo_map[ip][1],
                ttl=hop["count"],
                rtt=hop["Avg"] / 1000,
                node_id=source_name
            )
            trace_list.append(ip_lat_long)

            # Debug print for each hop
            click.secho(f"  • Hop {hop['count']}: {ip}", nl=False)
            click.secho(f" @ ({geo_map[ip][0]}, {geo_map[ip][1]})", fg='green')

        file_name_to_data[source_name] = trace_list
        click.secho(f"✔ Processed {len(trace_list)} hops from {file}", fg='green')

    # Final summary
    click.secho("=== Processing Complete ===", fg='cyan')
    click.secho(f"Processed {len(file_name_to_data)}/{len(files)} files successfully",
                fg='green' if len(file_name_to_data) == len(files) else 'yellow')
    click.secho(f"Results contain {sum(len(v) for v in file_name_to_data.values())} total hops", fg='cyan')

    return file_name_to_data


def process_pmeter_tr(pmeter_tr_path: str) -> Dict[str, List[IpToLonAndLat]]:
    """Process traceroute files in JSON Lines format with smart geolocation filling."""
    results = {}

    click.secho(f"\nProcessing traceroutes from: {pmeter_tr_path}", fg='cyan', bold=True)

    for file in sorted(f for f in os.listdir(pmeter_tr_path) if f.endswith('.json')):
        file_path = os.path.join(pmeter_tr_path, file)
        click.secho(f"\nFile: {file}", fg='yellow')

        try:
            with open(file_path, 'r') as f:
                line_number = 0
                for line in f:
                    line_number += 1
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        trace = json.loads(line)
                    except json.JSONDecodeError as e:
                        click.secho(f"  ⚠ Line {line_number}: Invalid JSON - {str(e)}", fg='red')
                        continue

                    if not isinstance(trace, dict):
                        click.secho(f"  ⚠ Line {line_number}: Expected JSON object", fg='red')
                        continue

                    # Get route info from metadata
                    meta = trace.get('metadata', {})
                    src = meta.get('source', 'unknown')
                    dest = meta.get('destination', 'unknown')

                    click.secho(f"  Route: {src} → {dest} (Line {line_number})", fg='blue')

                    if not isinstance(trace.get('hops'), list):
                        click.secho("    ⚠ No valid hops array found", fg='red')
                        continue

                    hops = []
                    previous_geo = None

                    # First pass to collect future geolocations
                    future_geos = []
                    for hop in reversed(trace['hops']):
                        geo = hop.get('geo', {})
                        if geo.get('lat') is not None and geo.get('lon') is not None:
                            future_geos.append((geo['lat'], geo['lon']))
                        else:
                            future_geos.append(None)
                    future_geos.reverse()

                    for i, hop in enumerate(trace['hops']):
                        try:
                            ip = hop.get('ip', 'unknown')
                            geo = hop.get('geo', {})
                            ttl = hop.get('ttl', 0)
                            rtt = hop.get('rtt_ms', 0) / 1000

                            # Get current or nearest valid geolocation
                            lat, lon = geo.get('lat'), geo.get('lon')
                            geo_source = "original"

                            if lat is None or lon is None:
                                if previous_geo:
                                    lat, lon = previous_geo
                                    geo_source = "prev_hop"
                                elif i < len(future_geos) - 1 and future_geos[i + 1]:
                                    lat, lon = future_geos[i + 1]
                                    geo_source = "next_hop"
                                else:
                                    geo_source = "unknown"
                            else:
                                previous_geo = (lat, lon)

                            hops.append(IpToLonAndLat(
                                ip=ip,
                                lat=lat,
                                lon=lon,
                                ttl=ttl,
                                rtt=rtt,
                                node_id=src
                            ))

                            # Format coordinates for display
                            coord_str = (f"{lat:.3f}, {lon:.3f}" if lat is not None and lon is not None
                                         else "None, None")
                            status = {
                                "original": "",
                                "prev_hop": " [prev hop]",
                                "next_hop": " [next hop]",
                                "unknown": " [unknown]"
                            }[geo_source]

                            click.secho(
                                f"    ✓ {ip:15} (TTL {ttl:2}) @ {coord_str}{status}",
                                fg='green' if geo_source == "original" else 'yellow'
                            )

                        except Exception as e:
                            click.secho(f"    ✗ Error processing hop: {str(e)}", fg='red')

                    key = f"{src}_{dest}"
                    if key in results:
                        click.secho(f"    ⚠ Overwriting previous results for {key}", fg='yellow')
                    results[key] = hops
                    click.secho(f"    Processed {len(hops)} hops", fg='blue')

        except Exception as e:
            click.secho(f"⚠ Error processing {file}: {str(e)}", fg='red')

    return results


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
