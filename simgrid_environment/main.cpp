#include "simgrid/plugins/energy.h"
#include "xbt/log.h"
#include "xbt/random.hpp"
#include <simgrid/s4u.hpp>
#include <vector>
#include <string>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;
static const int min_size = 1e6;
static const int max_size = 1e9;

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_energyconsumption, "Messages specific for this s4u example");
namespace sg4 = simgrid::s4u;

static void sender(std::vector<std::string> args) {
    xbt_assert(args.size() == 2, "The sender function expects 2 arguments.");
    int flow_amount = std::stoi(args.at(0));
    long comm_size = std::stol(args.at(1));

    XBT_INFO("Sending %ld bytes in %d flows", comm_size, flow_amount);
    sg4::Mailbox *mailbox = sg4::Mailbox::by_name("message");

    sg4::this_actor::sleep_for(10); // Initial delay

    long chunk_size = comm_size / flow_amount;
    long remainder = comm_size % flow_amount;

    // Simulate buffer preparation
//    double buffer_prep_work = comm_size * 1;
//    sg4::this_actor::execute(buffer_prep_work);
//    XBT_INFO("Buffer preparation completed");

    sg4::ActivitySet comms;
    for (int i = 0; i < flow_amount; i++) {
        long this_chunk_size = (i == flow_amount - 1) ? chunk_size + remainder : chunk_size;
//        double buffer_copy_work = this_chunk_size * 1;
//        sg4::this_actor::execute(buffer_copy_work);

        XBT_INFO("Flow %d sending %ld bytes", i, this_chunk_size);
        comms.push(mailbox->put_async(bprintf("%d", i), this_chunk_size));
    }
    comms.wait_all();
    XBT_INFO("Sender finished sending all flows.");
}

static void receiver(std::vector<std::string> args) {
    xbt_assert(args.size() == 1, "The receiver function expects 1 argument.");
    int flow_amount = std::stoi(args.at(0));

    XBT_INFO("Receiving %d flows...", flow_amount);
    sg4::Mailbox *mailbox = sg4::Mailbox::by_name("message");

    std::vector<char *> data(flow_amount);
    sg4::ActivitySet comms;

    for (int i = 0; i < flow_amount; i++) {
        comms.push(mailbox->get_async<char>(&data[i]));
    }
    comms.wait_all();

    for (int i = 0; i < flow_amount; i++) {
        long this_chunk_size = strlen(data[i]) + 1;
//        double buffer_process_work = this_chunk_size * 1;
//        sg4::this_actor::execute(buffer_process_work);
        XBT_INFO("Flow %d received %ld bytes", i, sizeof(data[i]));
        xbt_free(data[i]);
    }
    XBT_INFO("Receiver finished receiving all flows.");
}

int main(int argc, char *argv[]) {
    sg4::Engine e(&argc, argv);
    sg_link_energy_plugin_init();
    sg_host_energy_plugin_init();

    xbt_assert(argc > 1, "\nUsage: %s platform_file [flowCount [datasize [job_id]]]\n"
               "\tExample: %s s4uplatform.xml \n", argv[0], argv[0]);
    e.load_platform(argv[1]);

    // Extract route key from platform file path (format: source_destination)
    std::string platform_file = argv[1];
    size_t start_pos = platform_file.find("simgrid_configs/") + std::string("simgrid_configs/").size();
    std::string route_key = platform_file.substr(start_pos);
    route_key = route_key.substr(0, route_key.find("_network.xml"));

    // Split route key into source and destination
    size_t underscore_pos = route_key.find('_');
    std::string source_node = route_key.substr(0, underscore_pos);
    std::string destination_node = route_key.substr(underscore_pos + 1);

    XBT_INFO("Route: %s -> %s", source_node.c_str(), destination_node.c_str());

    // Prepare arguments
    std::vector<std::string> argSender;
    std::vector<std::string> argReceiver;

    argSender.push_back(argc > 2 ? argv[2] : "1"); // flow count
    argReceiver.push_back(argc > 2 ? argv[2] : "1");

    if (argc > 3) {
        if (strcmp(argv[3], "random") == 0) {
            argSender.push_back(std::to_string(simgrid::xbt::random::uniform_int(min_size, max_size)));
        } else {
            argSender.push_back(argv[3]); // datasize
        }
    } else {
        argSender.push_back("25000"); // default datasize
    }

    std::string job_id = argc > 4 ? argv[4] : "0";

    // Create actors
    sg4::Actor::create("sender", e.host_by_name(source_node), sender, argSender);
    sg4::Actor::create("receiver", e.host_by_name(destination_node), receiver, argReceiver);

    long job_size = std::stol(argSender[1]);
    e.run();

    // Collect and save energy data
    json output;
    output["transfer_duration"] = sg4::Engine::get_clock();
    output["job_size_bytes"] = job_size;
    output["flow_count"] = std::stoi(argSender[0]);
    output["route_key"] = route_key;
    output["source_node"] = source_node;
    output["destination_node"] = destination_node;

    json host_energy;
    json link_energy;
    double total_energy_hosts = 0.0;
    for (sg4::Host *host : e.get_all_hosts()) {
        total_energy_hosts += sg_host_get_consumed_energy(host);
        host_energy[host->get_name()] = sg_host_get_consumed_energy(host);
    }
    double total_link_energy = 0.0;
    for (sg4::Link *link : e.get_all_links()) {
        total_link_energy += sg_link_get_consumed_energy(link);
        link_energy[link->get_name()] = sg_link_get_consumed_energy(link);
    }

    output["hosts"] = host_energy;
    output["links"] = link_energy;
    output["total_energy_hosts"] = total_energy_hosts;
    output["total_link_energy"] = total_link_energy;

    std::string output_file = "/workspace/data/energy_consumption_" + route_key + "_" + job_id + "_.json";
    std::ofstream file(output_file);
    file << output.dump(4);
    file.close();

    XBT_INFO("Energy data saved to %s", output_file.c_str());
    return 0;
}