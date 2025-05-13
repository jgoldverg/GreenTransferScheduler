import json
import os.path
import subprocess
import xml.etree.ElementTree as ET
from typing import List, Dict

import click

from models import IpToLonAndLat


class SimGridSimulator:

    def __init__(self, traceroute_data: Dict[str, List[IpToLonAndLat]], job_list: List[dict], node_list: List[dict]):
        self.traceroute_data = traceroute_data
        self.network_file = '../config/network.xml'
        self.job_list = job_list
        self.node_list = node_list
        self.node_network_xml_paths = []
        self.node_name = ""
        self.node_map = {node["name"]: {k: v for k, v in node.items() if k != "name"} for node in self.node_list}
        for key, value in self.node_map.items():
            if value["type"] == "destination":
                self.destination_node = key
                click.secho(f"destination node: {self.destination_node}")

    def create_xml_for_traceroute(self):
        """Generate SimGrid XML files with optimized energy profiles for all network components."""
        route_routers = {}  # {route_key: {ip: router_name}}
        route_links = {}  # {route_key: {ip: link_name}}
        self.node_network_xml_paths = []

        # Energy profile configurations
        ENERGY_PROFILES = {
            'core_router': {
                'wattage_per_state': "500:1000:1500",
                'wattage_off': "10.0",
                'speed': "10Gf",  # Reduced for routing efficiency
                'cores': "1"
            },
            'edge_router': {
                'wattage_per_state': "50:200:400",
                'wattage_off': "5.0",
                'speed': "1Gf",
                'cores': "1"
            },
            'backbone_link': {
                'wattage_range': "100:200",
                'wattage_off': "15.0"
            },
            'standard_link': {
                'wattage_range': "50:100",
                'wattage_off': "10.0"
            }
        }

        for route_key, traceroute in self.traceroute_data.items():
            source_node, destination_node = route_key.split('_')

            # Validate nodes exist
            if source_node not in self.node_map or destination_node not in self.node_map:
                click.secho(f"Skipping invalid route {route_key} - nodes not found", fg='yellow')
                continue

            if self.node_map[source_node].get('type') == 'destination':
                continue

            # Initialize route-specific mappings
            routers = {}
            links = {}
            output_path = f'/workspace/config/simgrid_configs/{route_key}_network.xml'
            parent_dir = os.path.dirname(output_path)
            os.makedirs(parent_dir, exist_ok=True)
            self.node_network_xml_paths.append(f'{route_key}_network.xml')

            platform = ET.Element("platform", version="4.1")
            doctype = '<!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">\n'
            zone = ET.SubElement(platform, "zone", id="AS0", routing="Full")

            # Determine if this is a backbone path
            is_backbone = any(
                hop.node_id in [n for n, data in self.node_map.items() if data.get('type') == 'dtn']
                for hop in traceroute
            )

            # Process each hop
            xml_links = []
            for i, hop in enumerate(traceroute):
                # Handle source, destination, and routers differently
                if i == 0:  # Source node
                    router_id = source_node
                    node_data = self.node_map[source_node]
                    profile = {
                        'speed': f"{float(node_data['gf']) * 1e9:.0f}f",
                        'cores': str(node_data['CPU']),
                        'wattage_per_state': f"{node_data['power']['min']}:{node_data['power']['max']}",
                        'wattage_off': "5.0"
                    }
                elif i == len(traceroute) - 1:  # Destination node
                    router_id = destination_node
                    node_data = self.node_map[destination_node]
                    profile = {
                        'speed': f"{float(node_data['gf']) * 1e9:.0f}f",
                        'cores': str(node_data['CPU']),
                        'wattage_per_state': f"{node_data['power']['min']}:{node_data['power']['max']}",
                        'wattage_off': "5.0"
                    }
                else:  # Intermediate router
                    router_id = f"router_{route_key}_{i}"
                    profile = ENERGY_PROFILES['core_router' if is_backbone else 'edge_router']
                    routers[hop.ip] = router_id

                # Create router element
                router = ET.SubElement(
                    zone, "host",
                    id=router_id,
                    speed=profile['speed'],
                    core=profile['cores']
                )
                ET.SubElement(router, "prop", id="wattage_per_state", value=profile['wattage_per_state'])
                ET.SubElement(router, "prop", id="wattage_off", value=profile['wattage_off'])

                if i not in [0, len(traceroute) - 1]:
                    ET.SubElement(router, "prop", id="is_router", value="true")

                # Create links (skip for first hop)
                if i > 0:
                    link_id = f"link_{route_key}_{i}"
                    links[hop.ip] = link_id
                    prev_router_id = destination_node if i == 1 else f"router_{route_key}_{i - 1}"
                    xml_links.append((prev_router_id, router_id, link_id))

                    # Calculate per-hop latency
                    hop_latency = max(0.1, (hop.rtt - traceroute[i - 1].rtt) * 1000)  # ms

                    # Determine bandwidth
                    if i == 1:  # First hop from source
                        bandwidth = self.node_map[source_node]['NIC_SPEED']
                    elif i == len(traceroute) - 1:  # Last hop to destination
                        bandwidth = self.node_map[destination_node]['NIC_SPEED']
                    else:  # Intermediate links
                        bandwidth = "100Gbps" if is_backbone else "10Gbps"

                    link_profile = ENERGY_PROFILES['backbone_link' if is_backbone else 'standard_link']
                    link = ET.SubElement(
                        zone, "link",
                        id=link_id,
                        bandwidth=bandwidth,
                        latency=f"{hop_latency:.2f}ms",
                        sharing_policy="SHARED"
                    )
                    ET.SubElement(link, "prop", id="wattage_range", value=link_profile['wattage_range'])
                    ET.SubElement(link, "prop", id="wattage_off", value=link_profile['wattage_off'])

            # Create route in XML
            route = ET.SubElement(zone, "route", src=source_node, dst=destination_node)
            for src, dst, link_id in xml_links:
                ET.SubElement(route, "link_ctn", id=link_id)

            # Save XML file
            xml_str = ET.tostring(platform, encoding='unicode')
            with open(output_path, 'w') as f:
                f.write(f'<?xml version="1.0"?>\n{doctype}{xml_str}')

            # Store route mappings
            route_routers[route_key] = routers
            route_links[route_key] = links

            click.secho(f"Generated: {output_path} ({'backbone' if is_backbone else 'standard'} path)", fg='green')

        return route_routers, route_links

    def _determine_bandwidth(self, hop_index, total_hops, source_node, is_backbone):
        """Determine appropriate bandwidth for each link segment."""
        # First hop from source
        if hop_index == 1:
            return self.node_map[source_node]['NIC_SPEED']

        # Last hop to destination (handled in main method)
        elif hop_index == total_hops - 1:
            return self.node_map[self.destination_node]['NIC_SPEED']

        # Intermediate links
        else:
            return "100Gbps" if is_backbone else "10Gbps"

    def run_simulation(self, node_name="jgoldverg@gmail.com-ccuc", flows=1, job_size=1000000000000, job_id=1):
        xml_file_path = "/workspace/config/simgrid_configs/" + node_name + "_network.xml"
        params = ["/workspace/simgrid_environment/run.sh", xml_file_path, str(flows), str(job_size), str(job_id),
                  str(self.destination_node)]
        # click.secho(f"Running Simgrid with parameters: {params}")
        result = subprocess.run(params, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # result = subprocess.run(params)

    def parse_simulation_output(self, node_name, job_id):
        file_path = f"/workspace/data/energy_consumption_{node_name}_{job_id}_.json"
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                return json.load(file)
        return None
