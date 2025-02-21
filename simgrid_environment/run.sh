#!/bin/bash

set -e  # Exit immediately if a command fails

# Create and navigate to the build directory
mkdir -p /workspace/simgrid_environment/build
cd /workspace/simgrid_environment/build

# Run CMake and build the project
cmake ..
make -j$(nproc)  # Uses multiple cores for faster compilation

# Run the compiled program (modify 'my_program' to match your executable)
./my_simgrid_app "$@"
