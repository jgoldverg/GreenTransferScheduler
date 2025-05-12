# Carbon-Aware Network Job Scheduling: MILP Optimization for Green Computing

[![IEEE](https://img.shields.io/badge/IEEE-Conference-blue.svg)](https://www.ieee.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Authors**: Jacob Goldverg  
**Institution**: University at Buffalo (SEAS)  

## Abstract
This repository contains the implementation and experimental framework for my research on carbon-aware data transmission scheduling in a distributed setting. 
Im planning to present a Mixed Integer Linear Programming (MILP) approach that reduces carbon emissions by 89.7% compared to traditional scheduling algorithms while meeting all job deadlines.
**Gotta still do way more testing and proper visualization**

## Key Features
- 🍃 MILP formulation for carbon-minimizing file transfer scheduling or data transmission
- ⚡ Comparative analysis of 6 scheduling algorithms:
  - EDF (Earliest Deadline First)
  - SJF (Shortest Job First)
  - Round Robin
  - Greedy Carbon-Aware
  - Worst-Fit
  - Our MILP Optimizer
- 📊 Reproducible experimental framework
- 📈 Carbon/Throughput/Deadline compliance metrics

## Installation
Please have docker installed as Simgrid is very bulky to install locally, docker simplifies this dramatically and makes it highly portable.
Important! Run the Em2024DatasetBuilder.ipynb first to make sure you have the 2024 dataset locally downloaded. The path used is the default path in the cli.
Following you can do the bash installation below.

```bash
git clone https://github.com/yourusername/carbon-aware-scheduling.git
docker build -t simgrid-container .
docker run -it --rm -v $(pwd):/workspace simgrid-container
cd scheduler_cli

```

## How to use?
First you need to have a forecast dataset, this dataset can represent anytime period you would like but should cover the entire geographic region that you wish to study.
The default is as follows:
- `config/geojson/world.geojson` is a geojson file that draws effectively polygons of the entire world. Pretty straightforward as we use this with GeoPandas.
- `config/jobs_config/*` contains jobs that you wish to simulate via Simgrid and then schedule to run at some point in the future. There is a command in main.py that covers how to generate example job configuration
- `config/node_configs/` contains configurations of the nodes in the path. You must include a Source node, destination node, each is named as either the key in the traceroute you are supplying ex `chi_to_buff.json` and our nodes_space_3_config.json contains `chi` and `buff` nodes defined.
- `config/traceroutes/pmeter_tr` this is the directory to put in traceroutes named from source_to_destination.json with each node being supplied via node_configs.
- `config/simgrid_configs` pretty simple this directory needs to be created and contains configurations generated via your traceroute and node configurations. Mirroring the WAN setup.

### Commands
`python3 main.py` will output supported commands with information per command. The defaults are sufficient for certain cases and I will document command examples below as I progress.