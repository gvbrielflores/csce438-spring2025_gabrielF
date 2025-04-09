#include <algorithm>
#include <cstdio>
#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <utility>
#include <map>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <glog/logging.h>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool isMaster;
    bool missed_heartbeat;
    zNode( // Constructor with defaults to work with resize
        int sid = -1,
        std::string host = "null", 
        std::string port = "null",
        std::string type = "null",
        std::time_t lasthb = 0,
        bool master = false,
        bool missedhb = false
    ): serverID(sid), hostname(host), port(port), type(type), last_heartbeat(lasthb), isMaster(master), missed_heartbeat(missedhb) {}
    bool isActive();
};


//potentially thread safe 
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;

// creating a vector of vectors containing znodes
std::vector<std::vector<zNode*>> clusters = {cluster1, cluster2, cluster3};


//func declarations
int findServer(std::vector<zNode*> v, int id); 
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}


class CoordServiceImpl final : public CoordService::Service {

    Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
        // Your code here
        // See if the heartbeat is registration or normal
        zNode *heartbeatSender;
        auto clientMetadata = context->client_metadata();
        auto isRegistration = clientMetadata.find("registration-heartbeat");
        int clusterId = std::stoi(clientMetadata.find("clusterid")->second.data());
        if (isRegistration != clientMetadata.end()) { // Registration heartbeat
            v_mutex.lock();

            // Make a new zNode for the cluster
            log(INFO, "Got a registration request from "+ std::to_string(clusterId) +"/"+std::to_string(serverinfo->serverid())+"\n");

            std::vector<zNode *>* senderCluster = &clusters.at(clusterId - 1);
            // If there are no other servers on the cluster, the registering server is the master of this cluster now
            // Otherwise, it is a slave
            bool master = (senderCluster->size() == 0) ? true : false;

            if (master) {
                log(INFO, "New master "+std::to_string(serverinfo->serverid())+" for cluster "+std::to_string(clusterId)+"\n");
            }

            heartbeatSender = new zNode(
                serverinfo->serverid(),
                serverinfo->hostname(),
                serverinfo->port(),
                serverinfo->type(),
                getTimeNow(),
                master,
                false
            );

            // Place in correct spot by id (this is assuming servers can have whatever id they want)
            if (heartbeatSender->serverID - 1 >= senderCluster->size()) { // Server id is not contained in the cluster
                zNode *dummy = new zNode();
                for (int i = heartbeatSender->serverID - 1; i < heartbeatSender->serverID; i++) {// Starting from end of the current array
                    senderCluster->push_back(dummy);
                }
            }
            senderCluster->at(heartbeatSender->serverID - 1) = heartbeatSender;

            // // Push a new server into the corresponding cluster vector
            // senderCluster->push_back(heartbeatSender);

            v_mutex.unlock();
        }
        else {
            v_mutex.lock();

            log(INFO, "Got a heartbeat from "+ std::to_string(clusterId) +"/"+std::to_string(serverinfo->serverid())+"\n");
            // Find the server
            heartbeatSender = clusters.at(clusterId - 1).at(serverinfo->serverid() - 1); // Grab based on server id
            // Update the heartbeat sender's zNode (re-write everything like Zookeeper does it)
            heartbeatSender->serverID = serverinfo->serverid();
            heartbeatSender->hostname = serverinfo->hostname();
            heartbeatSender->port = serverinfo->port();
            heartbeatSender->type = serverinfo->type();
            // heartbeatSender->isMaster = serverinfo->ismaster(); Master is only determined and kept track of coordinator side.
            heartbeatSender->last_heartbeat = getTimeNow();

            v_mutex.unlock();
        }

        return Status::OK;
    }

    //function returns the server information for requested client id
    //this function assumes there are always 3 clusters and has math
    //hardcoded to represent this.
    Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
        // Your code here
        int clientId = id->id();
        int clusterId = ((clientId - 1) % 3) + 1;
        
        // Build the server info for the client
        // First get the client's assigned cluster
        std::vector<zNode *> *assignedCluster = &clusters.at(clusterId - 1);

        // Grab the Master server in the cluster
        zNode *server;
        for (int i = 0; i < assignedCluster->size(); i++) {
            if ((assignedCluster->at(i))->isMaster) {
                server = assignedCluster->at(i);
            }
        }

        // Fill in server info for the client
        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
        serverinfo->set_ismaster(server->isMaster);
        serverinfo->set_type(server->type);

        return Status::OK;
    }

    Status GetSlave(ServerContext *context, const ID *id, ServerInfo *serverinfo) {
        int clientId = id->id();
        int clusterId = ((clientId - 1) % 3) + 1;
        
        // Build the server info for the client
        // First get the client's assigned cluster
        std::vector<zNode *> *assignedCluster = &clusters.at(clusterId - 1);

        // Grab the Slave server in the cluster
        zNode *server;
        for (int i = 0; i < assignedCluster->size(); i++) {
            if (!(assignedCluster->at(i))->isMaster) {
                server = assignedCluster->at(i);
            }
        }

        // Fill in server info for the client
        serverinfo->set_serverid(server->serverID);
        serverinfo->set_hostname(server->hostname);
        serverinfo->set_port(server->port);
        serverinfo->set_ismaster(server->isMaster);
        serverinfo->set_type(server->type);

        return Status::OK;
    }

    Status GetAllFollowerServers(ServerContext *context, const ID *id, ServerList *serverlist) {
        return Status::OK;
    }

    Status GetFollowerServer(ServerContext *context, const ID *id, ServerInfo *serverinfo) {
        return Status::OK;
    }

};

void RunServer(std::string port_no){
    //start thread to check heartbeats
    std::thread hb(checkHeartbeat);
    //localhost = 127.0.0.1
    std::string server_address("127.0.0.1:"+port_no);
    CoordServiceImpl service;
    //grpc::EnableDefaultHealthCheckService(true);
    //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Wait for the server to shutdown. Note that some other thread must be
    // responsible for shutting down the server for this call to ever return.
    server->Wait();
}

int main(int argc, char** argv) {

    std::string port = "3010";
    int opt = 0;
    while ((opt = getopt(argc, argv, "p:")) != -1){
        switch(opt) {
            case 'p':
                port = optarg;
                break;
            default:
                std::cerr << "Invalid Command Line Argument\n";
        }
    }
    RunServer(port);
    return 0;
}



void checkHeartbeat(){
    while(true){
        //check servers for heartbeat > 10
        //if true turn missed heartbeat = true
        // Your code below

        v_mutex.lock();

        // iterating through the clusters vector of vectors of znodes
        for (auto& c : clusters){
            // Iterating through servers in a cluster
            for(auto& s : c){
                if (s->serverID != -1) { // Only check if the server "exists" (is active)
                    if(difftime(getTimeNow(),s->last_heartbeat)>10){
                        log(INFO, "missed heartbeat from server " + std::to_string(s->serverID) + "\n");
                        if (s->isMaster) { // If it was the master, must promote the slave to master
                            log(INFO, "Master "+std::to_string(s->serverID)+" has died"+ "\n");
                            s->serverID = -1; // Mark server as non-existent (failed)
                            for (auto& s : c) { // zNodes are not actually removed from the vector, so we need to find the active slave (there should only be one slave)
                                if (s->serverID != -1 && !s->isMaster) { // Active slave server
                                    log(INFO, "New master: "+std::to_string(s->serverID) + "\n");
                                    s->isMaster = true;
                                }
                            }
                        }
                        if(!s->missed_heartbeat){
                            s->missed_heartbeat = true;
                            s->last_heartbeat = getTimeNow();
                        }
                    }
                }
            }
        }

        v_mutex.unlock();

        sleep(3);
    }
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

