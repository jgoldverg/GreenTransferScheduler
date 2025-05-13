# Use an official Ubuntu as a base image
FROM ubuntu:24.04

# Set environment variables to avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install dependencies for SimGrid and C++ development
RUN apt-get update -y && apt-get upgrade -y && apt-get install -y \
    build-essential \
    python3-dev \
    libc6-dev \
    simgrid \
    libsimgrid-dev \
    python3-simgrid \
    python3-pip \
    pybind11-dev \
    libboost-dev \
    nlohmann-json3-dev \
    cmake \
    g++ \
    gdb \
    ninja-build \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /workspace

# Copy requirements.txt and install dependencies
COPY requirements.txt /workspace/requirements.txt
RUN pip3 install -r /workspace/requirements.txt --break-system-packages

# Default command
CMD ["bash"]
