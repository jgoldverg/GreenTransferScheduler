import json
from typing import List
import xml.etree.ElementTree as ET

import click
from models import IpToLonAndLat, parse_speed_to_bps
import sys


import xml.etree.ElementTree as ET
from typing import List

import xml.etree.ElementTree as ET
from typing import List

class SimGridSimulator:

    def __init__(self, traceroute_data: List[List[dict]], job_list: List[dict], node_list: List[dict]):
        self.traceroute_data = traceroute_data
        self.network_file = '../config/network.xml'
        self.job_list = job_list
        self.node_list = node_list
        self.node_network_list = []
        self.node_name = ""

    def create_xml_for_traceroute(self):
        # Create a dictionary to store the file paths for each node
        self.node_file_path_dict = {}

        for node in self.node_list:
            node_network_file_path = '../config/simgrid_configs/' + node['name'] + '_network.xml'
            platform = ET.Element("platform", version="4.1")

            # Create DOCTYPE declaration (SimGrid requires this)
            doctype = '<!DOCTYPE platform SYSTEM "https://simgrid.org/simgrid.dtd">\n'

            # Create a zone for better routing
            zone = ET.SubElement(platform, "zone", id="AS0", routing="Full")

            trace_route = self.traceroute_data[0]
            hosts = {}
            links = []

            for i, hop in enumerate(trace_route):
                host_id = f"host{i}"
                hosts[hop.ip] = host_id

                # Define host with energy properties
                if i == 0:
                    # Configure the source node (e.g., the first node in the list)
                    host_element = ET.SubElement(
                        zone, "host",
                        id=node['name'],
                        speed="1Gf",
                        core=str(node['CPU'])  # Assuming 4 cores
                    )
                    self.node_file_path_dict[node['name']] = node_network_file_path
                    hosts[hop.ip] = node['name']
                else:
                    host_element = ET.SubElement(
                        zone, "host",
                        id=host_id,
                        speed="1Gf",
                        core="4"  # Assuming 4 cores for each host
                    )

                # Add wattage properties to each host
                ET.SubElement(host_element, "prop", id="wattage_per_state", value="100.0:120.0:200.0")
                ET.SubElement(host_element, "prop", id="wattage_off", value="10")

                # Create network links with energy properties
                if i > 0:
                    prev_ip = trace_route[i - 1].ip
                    link_id = f"link{i}"
                    links.append((hosts[prev_ip], hosts[hop.ip], link_id))

                    link_element = ET.SubElement(
                        zone, "link",
                        id=link_id,
                        bandwidth="10GBps",
                        latency=f"{hop.rtt}ms",
                        sharing_policy="SHARED"
                    )

                    # Energy properties for the link
                    ET.SubElement(link_element, "prop", id="wattage_range", value="100.0:200.0")
                    ET.SubElement(link_element, "prop", id="wattage_off", value="10")

            # Create the full route by sequentially adding the links
            # We need to create the route from host1 to host9
            route = ET.SubElement(zone, "route", src=hosts[trace_route[0].ip], dst=hosts[trace_route[-1].ip])

            # Add each link to the route in sequence
            for src, dst, link_id in links:
                ET.SubElement(route, "link_ctn", id=link_id)

            # Convert the XML tree to a string and add the DOCTYPE declaration manually
            xml_string = ET.tostring(platform, encoding="utf-8").decode("utf-8")
            xml_string = f'<?xml version="1.0"?>\n{doctype}{xml_string}'

            # Save the generated XML to the corresponding file path for the node
            with open(node_network_file_path, "w", encoding="utf-8") as f:
                f.write(xml_string)

            print(f"SimGrid XML file generated: {node_network_file_path}")
            print(f"Node list created: {self.node_list}")



    def run_simulation(self, simgrid_options):
        pass
        # sg.sg_host_load_plugin_init()
        # engine = sg.Engine(sys.argv)  # Use actual CLI args
        # for node in self.node_list:
        #     try:
        #         click.echo(f"Loading in: {node['name']} xml file")
        #         engine.load_platform(self.node_file_path_dict[node['name']])  # Ensure the file is correctly generated
        #     except Exception as e:
        #         print(f"Error loading platform: {e}")
        #         return
        #     break
        #
        # first_job = self.job_list[len(self.job_list) - 1]
        # # Assign jobs to nodes
        # click.secho(f"Job we are going to run is: {first_job}, type: {type(first_job)}")
        # for node in self.node_list:
        #     click.secho(f"The current Node we are running {node}, type: {type(node)}")
        #     try:
        #         # Correct access to node name
        #         host = engine.host_by_name(node['name'])  # Here node_name is already a string, no need for ['name']
        #         if host is None:
        #             print(f"Warning: Host {node['name']} not found in XML")
        #             continue
        #
        #         # engine.add_actor(f"job_{first_job['id']}", host, job_processor, first_job)
        #         sg.Actor.create(f"job_{first_job['id']}", host, job_processor, (first_job, node))
        #     except Exception as e:
        #         print(f"Error creating job {first_job['id']}: {e}")
        #
        # print("Starting simulation...")
        # try:
        #     engine.run()
        #     print("Simulation complete.")
        # except Exception as e:
        #     print(f"Error running simulation: {e}")


# def job_processor(args):
#     """ Simulates file transfer and tracks energy consumption """
#     job, node = args
#
#     actor = sg.this_actor
#     # node = sg.this_actor.get_host()
#     print(job, node)
#     # Get file size in bytes and network bandwidth in bytes per second
#     file_size_bytes = int(job.get("bytes", 0))
#     bandwidth_bits_per_second = parse_speed_to_bps(node['NIC_SPEED'])
#
#     # Calculate transfer time in seconds
#     transfer_time_seconds = file_size_bytes / bandwidth_bits_per_second
#
#     click.echo(f"File transfer for job {job['id']} started on {node['name']}")
#     click.echo(f"File size: {file_size_bytes} bytes, Bandwidth: {bandwidth_bits_per_second} bits/s")
#     click.echo(f"Estimated transfer time: {transfer_time_seconds:.2f} seconds")
#
#     try:
#         # Simulate the file transfer by sleeping for the calculated time
#         actor.sleep_for(transfer_time_seconds)
#         # Optionally, track energy consumption during the transfer
#
#         energy_joules = sg.this_actor.get_host().get_consumed_energy()
#         # energy_joules = node.get_consumed_energy()
#         click.echo(f"File transfer for job {job['id']} completed. Energy consumed: {energy_joules:.2f} Joules")
#     except Exception as e:
#         click.echo(f"Error during file transfer for job {job['id']}: {e}")
