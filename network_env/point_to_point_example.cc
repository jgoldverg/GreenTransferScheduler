/*
* SPDX-License-Identifier: GPL-2.0-only
 */

#include "ns3/applications-module.h"
#include "ns3/core-module.h"
#include "ns3/internet-module.h"
#include "ns3/network-module.h"
#include "ns3/point-to-point-module.h"

// Default Network Topology
//
//       10.1.1.0
// n0 -------------- n1
//    point-to-point
//

using namespace ns3;

NS_LOG_COMPONENT_DEFINE("FirstScriptExample");

int
main(int argc, char* argv[])
{

    //Should be a list of lists. In the first list we we have the forecast for 1 node, every entry in the 2nd list is the forecasted data
    std::string electricity_maps_forecast_json;
    std::string pmeter_ip_data_json;
    std::string node_spec_json;

    CommandLine cmd(__FILE__);
    cmd.AddValue("electricity_maps_forecast.json", "Forecast json file", electricity_maps_forecast_json);
    cmd.AddValue("pmeter_ip_data_json", "Pmeter measurement to base simulation on", pmeter_ip_data_json);
    cmd.AddValue("node_spec_json", "The node specification for source, transfer node, and destination", node_spec_json);
    cmd.Parse(argc, argv);

    Time::SetResolution(Time::NS);
    LogComponentEnable("FileTransferNodeApplication", LOG_LEVEL_INFO);
    LogComponentEnable("ServerNode", LOG_LEVEL_INFO);
    LogComponentEnable("DestinationNode", LOG_LEVEL_INFO);


    NodeContainer nodes;
    nodes.Create(3);

    PointToPointHelper pointToPoint;
    pointToPoint.SetDeviceAttribute("DataRate", StringValue("10Gbps"));
    pointToPoint.SetChannelAttribute("Delay", StringValue("2ms"));

    NetDeviceContainer devices;
    devices = pointToPoint.Install(nodes);

    InternetStackHelper stack;
    stack.Install(nodes);

    Ipv4AddressHelper address;
    address.SetBase("10.1.1.0", "255.255.255.0");

    Ipv4InterfaceContainer interfaces = address.Assign(devices);

    UdpEchoServerHelper echoServer(9);

    ApplicationContainer serverApps = echoServer.Install(nodes.Get(1));
    serverApps.Start(Seconds(1));
    serverApps.Stop(Seconds(10));



    Simulator::Run();
    Simulator::Destroy();
    return 0;
}

int compute_points_from_traceroute(std::string pmeter_ip_data_json) {

}