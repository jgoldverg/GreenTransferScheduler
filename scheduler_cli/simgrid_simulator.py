import json
import subprocess
import xml.etree.ElementTree as ET
from typing import List

import click
import pandas as pd


class SimGridSimulator:

    def __init__(self, traceroute_data: List[List[dict]], job_list: List[dict], node_list: List[dict]):
        self.traceroute_data = traceroute_data
        self.network_file = '../config/network.xml'
        self.job_list = job_list
        self.node_list = node_list
        self.node_network_xml_paths = []
        self.node_name = ""

    def create_xml_for_traceroute(self):
        for node in self.node_list:
            node_network_file_path = f'../config/simgrid_configs/{node["name"]}_network.xml'
            self.node_network_xml_paths.append(f'{node["name"]}_network.xml')

            platform = ET.Element("platform", version="4.1")
            doctype = '<!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">\n'
            zone = ET.SubElement(platform, "zone", id="AS0", routing="Full")

            trace_route = self.traceroute_data[0]
            routers = {}
            links = []

            # Create the first router (source) and last router (destination)
            source_router = node['name']
            destination_router = "destination"

            # Create routers with energy properties
            for i, hop in enumerate(trace_route):
                if i == 0:
                    # First node should be named based on the provided node_name (source)
                    router_id = source_router
                    cores = str(node['CPU'])  # Source has the node's CPU cores
                    speed = "1Gf"
                elif i == len(trace_route) - 1:
                    # Last node should be named "destination"
                    router_id = destination_router
                    cores = str(node['CPU'])  # Set default cores for destination
                    speed = "1Gf"
                else:
                    # Intermediate routers (router1, router2, etc.)
                    router_id = f"router{i}"
                    cores = "4"  # Set default cores for routers
                    speed = "1Gf"

                # Create the router element
                router_element = ET.SubElement(
                    zone, "host", id=router_id, speed=speed, core=cores
                )

                # Add energy consumption properties
                power_profile = "100.0:120.0:200.0" if i == 0 or i == len(trace_route) - 1 else "30.0:50.0:80.0"
                ET.SubElement(router_element, "prop", id="wattage_per_state", value=power_profile)
                ET.SubElement(router_element, "prop", id="wattage_off", value="5")

                # Mark as router (for intermediate nodes)
                if i != 0 and i != len(trace_route) - 1:
                    ET.SubElement(router_element, "prop", id="is_router", value="true")

                # Store the router IP mapping
                routers[hop.ip] = router_id

                if i != 0:  # Skip the source router for links
                    prev_ip = trace_route[i - 1].ip
                    link_id = f"link{i}"
                    links.append((routers[prev_ip], router_id, link_id))

                    link_element = ET.SubElement(
                        zone, "link", id=link_id, bandwidth="10Gbps", latency=f"{hop.rtt}ms", sharing_policy="SHARED"
                    )
                    ET.SubElement(link_element, "prop", id="wattage_range", value="80.0:130.0")
                    ET.SubElement(link_element, "prop", id="wattage_off", value="10")

            # Define the route from source to destination
            route = ET.SubElement(zone, "route", src=source_router, dst=destination_router)
            for src, dst, link_id in links:
                ET.SubElement(route, "link_ctn", id=link_id)

            # Convert to string and save
            xml_string = ET.tostring(platform, encoding="utf-8").decode("utf-8")
            xml_string = f'<?xml version="1.0"?>\n{doctype}{xml_string}'

            with open(node_network_file_path, "w", encoding="utf-8") as f:
                f.write(xml_string)

            print(f"SimGrid XML file generated: {node_network_file_path}")



    def run_simulation(self, node_name="jgoldverg@gmail.com_ccuc", flows=1, job_size=1000000000000, job_id=1):
        xml_file_path = "/workspace/config/simgrid_configs/"+node_name+"_network.xml"
        params = ["/workspace/simgrid_environment/run.sh", xml_file_path, str(flows), str(job_size), str(job_id)]
        click.secho(f"Running Simgrid with parameters: {params}")
        # result = subprocess.run(params,stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        result = subprocess.run(params)
        # print(result.stdout)
        # print(result.stderr)

    def parse_simulation_output(self, node_name, job_id):
        file_path = "/workspace/data/energy_consumption_" + str(node_name) + "_" + str(job_id) + "_.json"
        with open(file_path, 'r') as file:
            return json.load(file)
        # with open(file_path, 'r') as file:
        #     data = json.load(file)
        #     return pd.read_json(data)