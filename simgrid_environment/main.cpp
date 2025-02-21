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
/* Parameters of the random generation of the flow size */
static const int min_size = 1e6;
static const int max_size = 1e9;

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_energyconsumption, "Messages specific for this s4u example");
namespace sg4 = simgrid::s4u;

static void sender(std::vector<std::string> args) {
    xbt_assert(args.size() == 2, "The sender function expects 2 arguments.");
    int flow_amount = std::stoi(args.at(0));  // Number of flows
    long comm_size = std::stol(args.at(1));   // Total data size

    XBT_INFO("Sending %ld bytes in %d flows", comm_size, flow_amount);
    sg4::Mailbox *mailbox = sg4::Mailbox::by_name("message");

    // Sleep before starting the transfer
    sg4::this_actor::sleep_for(10);

    long chunk_size = comm_size / flow_amount;
    long remainder = comm_size % flow_amount;

    sg4::ActivitySet comms;  // Activity set to track parallel transfers

    for (int i = 0; i < flow_amount; i++) {
        long this_chunk_size = (i == flow_amount - 1) ? chunk_size + remainder : chunk_size;
        XBT_INFO("Flow %d sending %ld bytes", i, this_chunk_size);

        // Asynchronous put to enable parallel flow transfers
        comms.push(mailbox->put_async(bprintf("%d", i), this_chunk_size));
    }

    comms.wait_all();  // Wait for all chunks to be transferred
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
        comms.push(mailbox->get_async<char>(&data[i]));  // Async receive
    }

    comms.wait_all();  // Ensure all flows are received

    for (int i = 0; i < flow_amount; i++) {
        XBT_INFO("Flow %d received %ld bytes", i, sizeof(data[i]));
        xbt_free(data[i]);  // Free allocated memory
    }

    XBT_INFO("Receiver finished receiving all flows.");
}



int main(int argc, char *argv[]) {
    sg4::Engine e(&argc, argv);

    XBT_INFO("Activating the SimGrid link energy plugin");
    sg_link_energy_plugin_init();
    sg_host_energy_plugin_init();

    xbt_assert(argc > 1, "\nUsage: %s platform_file [flowCount [datasize]]\n"
               "\tExample: %s s4uplatform.xml \n",
               argv[0], argv[0]);
    e.load_platform(argv[1]);
    std::string platform_file = argv[1];

    size_t start_pos = platform_file.find("simgrid_configs/") + std::string("simgrid_configs/").size();
    size_t end_pos = platform_file.find("/", start_pos);
    std::string node_name = platform_file.substr(start_pos, end_pos - start_pos);
    node_name = node_name.substr(0, node_name.find("_network.xml"));
    std::cout << "Node name: " << node_name << std::endl;    XBT_INFO("Node name: %s", node_name.c_str());
    /* prepare to launch the actors */

    std::vector<std::string> argSender;
    std::vector<std::string> argReceiver;
    if (argc > 2) {
        argSender.emplace_back(argv[2]); // Take the amount of flows from the command line
        argReceiver.emplace_back(argv[2]);
    } else {
        argSender.emplace_back("1"); // Default value
        argReceiver.emplace_back("1");
    }

    if (argc > 3) {
        if (strcmp(argv[3], "random") == 0) {
            // We're asked to get a random size
            std::string size = std::to_string(simgrid::xbt::random::uniform_int(min_size, max_size));
            argSender.push_back(size);
        } else {
            // Not "random" ? Then it should be the size to use
            argSender.emplace_back(argv[3]); // Take the datasize from the command line
        }
    } else {
        // No parameter at all? Then use the default value
        argSender.emplace_back("25000");
    }

    std::string job_id = "";
    if(argc > 4) {
        job_id = argv[4];
    }

    sg4::Actor::create("sender", e.host_by_name(node_name), sender, argSender);
    sg4::Actor::create("receiver", e.host_by_name("destination"), receiver, argReceiver);

    /* And now, launch the simulation */
    double start_time = sg4::Engine::get_clock();
    e.run();

    double end_time = sg4::Engine::get_clock();
    XBT_INFO("Simulation time: %f s", end_time - start_time);

    json output;
    output["transfer_duration"] = sg4::Engine::get_clock();
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

    std::ofstream file("/workspace/data/energy_consumption_" + node_name + "_"+ job_id+"_.json");
    file << output.dump(4);
    file.close();
    return 0;
}
