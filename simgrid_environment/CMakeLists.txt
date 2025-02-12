cmake_minimum_required(VERSION 3.10)
project(SimGridExample)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Manually set include and library paths for SimGrid
set(SIMGRID_ROOT "/opt/homebrew/opt/simgrid") # Change to /usr/local for Intel Macs
include_directories("${SIMGRID_ROOT}/include")
link_directories("${SIMGRID_ROOT}/lib")

# Find and link nlohmann_json
find_package(nlohmann_json REQUIRED)

# Create executable
add_executable(my_simgrid_app main.cpp)

# Link SimGrid and nlohmann_json
target_link_libraries(my_simgrid_app PRIVATE simgrid nlohmann_json::nlohmann_json)
