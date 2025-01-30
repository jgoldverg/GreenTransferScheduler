cmake_minimum_required(VERSION 3.13)

project(MyNs3Project)

# Manually specify the path to the ns-3 installation
set(NS3_INSTALL_DIR /usr/local)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add ns3 include directory
include_directories(${NS3_INSTALL_DIR}/include)

# Add the ns-3 library directory
link_directories(${NS3_INSTALL_DIR}/lib)

# Add executable for your project
add_executable(my_ns3_app my_simulation.cc)

# Link your executable with the ns-3 libraries
target_link_libraries(my_ns3_app ns3-core ns3-network ns3-internet)  # Add other libraries as needed

# Link the dynamic libraries (.dylib)
target_link_libraries(my_ns3_app ${LIBS})
