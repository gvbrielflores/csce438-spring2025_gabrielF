// NOTE: This starter code contains a primitive implementation using the default RabbitMQ protocol.
// You are recommended to look into how to make the communication more efficient,
// for example, modifying the type of exchange that publishes to one or more queues, or
// throttling how often a process consumes messages from a queue so other consumers are not starved for messages
// All the functions in this implementation are just suggestions and you can make reasonable changes as long as
// you continue to use the communication methods that the assignment requires between different processes

#include <bits/fs_fwd.h>
#include <ctime>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <semaphore.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unordered_map>
#include <vector>
#include <unordered_set>
#include <filesystem>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <stdio.h>
#include <cstdlib>
#include <unistd.h>
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include "sns.grpc.pb.h"
#include "sns.pb.h"
#include "coordinator.grpc.pb.h"
#include "coordinator.pb.h"

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <jsoncpp/json/json.h>

#define log(severity, msg) \
    LOG(severity) << msg;  \
    google::FlushLogFiles(google::severity);

namespace fs = std::filesystem;

using csce438::AllUsers;
using csce438::Confirmation;
using csce438::CoordService;
using csce438::ID;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::SynchronizerListReply;
using csce438::SynchService;
using google::protobuf::Duration;
using google::protobuf::Timestamp;
using grpc::ClientContext;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using grpc::ClientContext;
// tl = timeline, fl = follow list
using csce438::TLFL;

ServerInfo myInfo;
int synchID = 1;
int clusterID = 1;
bool isMaster = false;
int total_number_of_registered_synchronizers = 6; // update this by asking coordinator
std::string coordAddr;
std::string clusterSubdirectory;
std::vector<std::string> otherHosts;
std::unordered_map<std::string, int> timelineLengths;

std::vector<std::string> get_lines_from_file(std::string);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, int, bool);
std::vector<std::string> getFollowersOfUser(int);
bool file_contains_user(std::string filename, std::string user);

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID, bool registration, std::shared_ptr<CoordService::Stub> stub = nullptr);

class SynchronizerRabbitMQ
{
private:
    amqp_connection_state_t conn;
    amqp_channel_t channel;
    amqp_channel_t consumeChannel;
    std::string hostname;
    int port;

    void setupRabbitMQ()
    {
        amqp_rpc_reply_t reply;

        conn = amqp_new_connection();
        amqp_socket_t *socket = amqp_tcp_socket_new(conn);
        amqp_socket_open(socket, hostname.c_str(), port);
        amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, "guest", "guest");
        amqp_channel_open(conn, channel);
        reply = amqp_get_rpc_reply(conn);
        if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
            std::cerr << "Channel open succeeded for channel (channel) - " << channel << std::endl;
        }
        amqp_channel_open(conn, consumeChannel);
        reply = amqp_get_rpc_reply(conn);
        if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
            std::cerr << "Channel open succeeded for channel (consumeChannel) - " << channel << std::endl;
        }
    }

    void bindConsumerQueue(const std::string& queueName) {
        amqp_basic_consume(conn, consumeChannel, amqp_cstring_bytes(queueName.c_str()),
                           amqp_empty_bytes, 0, 0, 0, amqp_empty_table);
    }

    void declareQueue(const std::string &queueName)
    {
        amqp_queue_declare(conn, channel, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, 0, 0, amqp_empty_table);
    }

    int publishMessage(const std::string &queueName, const std::string &message)
    {
        return amqp_basic_publish(conn, channel, amqp_empty_bytes, amqp_cstring_bytes(queueName.c_str()),
                           0, 0, NULL, amqp_cstring_bytes(message.c_str()));
    }

public:
    // Had to move this to public to combat leaky queues
    std::string consumeMessage(const std::string &queueName, int timeout_ms = 5000)
    {
        // amqp_basic_consume_ok_t *consumeOk = amqp_basic_consume(conn, consumeChannel, amqp_cstring_bytes(queueName.c_str()),
        //                 amqp_empty_bytes, 0, 0, 0, amqp_empty_table);

        amqp_envelope_t envelope;
        amqp_maybe_release_buffers(conn);

        struct timeval timeout;
        timeout.tv_sec = timeout_ms / 1000;
        timeout.tv_usec = (timeout_ms % 1000) * 1000;

        amqp_rpc_reply_t res = amqp_consume_message(conn, &envelope, &timeout, 0);

        if (res.reply_type != AMQP_RESPONSE_NORMAL)
        {
            return "";
        }

        std::string message(static_cast<char *>(envelope.message.body.bytes), envelope.message.body.len);
        amqp_basic_ack(conn, consumeChannel, envelope.delivery_tag, false); // Manual ack - long shot
        std::cout << "[ACKED] msg: " << message << std::endl;
        amqp_destroy_envelope(&envelope);
        return message;
    }

    int synchID;
    // SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname(host), port(p), channel(1), synchID(id)
    SynchronizerRabbitMQ(const std::string &host, int p, int id) : hostname("rabbitmq"), port(p), channel(1), consumeChannel(id+1), synchID(id)
    {
        setupRabbitMQ();
        // declareQueue("synch" + std::to_string(synchID) + "_users_queue");
        // declareQueue("synch" + std::to_string(synchID) + "_clients_relations_queue");
        // declareQueue("synch" + std::to_string(synchID) + "_timeline_queue");
        // TODO: add or modify what kind of queues exist in your clusters based on your needs

        // Leaky queues fix
        declareQueue("synch" + std::to_string(synchID) + "_queue");
        bindConsumerQueue("synch" + std::to_string(synchID) + "_queue");
    }

    void publishUserList()
    {
        // Open a semaphore to protect reading from all users file
        std::string allUsersSemName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
        if (allUsersSem == SEM_FAILED) {
            perror("sem_open failed");
            std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
            log(ERROR, "failed to open semaphore");
            return;
        }
        sem_wait(allUsersSem);
        std::vector<std::string> users = get_all_users_func(synchID);
        sem_post(allUsersSem);
        sem_close(allUsersSem);

        std::sort(users.begin(), users.end());
        Json::Value userList;
        for (const auto &user : users)
        {
            userList["users"].append(user);
        }
        Json::FastWriter writer;
        std::string message = writer.write(userList);
        std::cout << "publishing these users: " + message << std::endl;
        for (int i = 1; i <= 6; i++) { // Publish this message to all OTHER synch queues
            if (i != synchID) {
                int status = publishMessage("synch" + std::to_string(i) + "_queue", message);
                if (status != 0) std::cout << "user list publish failed" << std::endl;
            }
        }
    }

    void consumeUserLists(std::string message)
    {
        std::vector<std::string> allUsers;
        // YOUR CODE HERE
        
        // TODO: while the number of synchronizers is harcorded as 6 right now, you need to change this
        // to use the correct number of follower synchronizers that exist overall
        // accomplish this by making a gRPC request to the coordinator asking for the list of all follower synchronizers registered with it
        // std::string queueName = "synch" + std::to_string(synchID) + "_users_queue";
        // std::string message = consumeMessage(queueName, 1000); // 1 second timeout
        if (!message.empty())
        {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &user : root["users"])
                {
                    allUsers.push_back(user.asString());
                }
            }
        }
        updateAllUsersFile(allUsers);
    }

    void publishClientRelations()
    {
        Json::Value relations;

        // Open a semaphore to protect reading from all users file
        std::string allUsersSemName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
        if (allUsersSem == SEM_FAILED) {
            perror("sem_open failed");
            std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
            log(ERROR, "failed to open semaphore");
            return;
        }
        sem_wait(allUsersSem);
        std::vector<std::string> users = get_all_users_func(synchID);
        sem_post(allUsersSem);
        sem_close(allUsersSem);

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            Json::Value followerList(Json::arrayValue);
            for (const auto &follower : followers)
            {
                followerList.append(follower);
            }

            if (!followerList.empty())
            {
                relations[client] = followerList;
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(relations);
        std::cout << "publishing these relations: " + message << std::endl;
        for (int i = 1; i <= 6; i++) {
            if (i != synchID) {
                int status = publishMessage("synch" + std::to_string(i) + "_queue", message);
                if (status != 0) std::cout << "client relations publish failed" << std::endl;
            }
        }
    }

    void consumeClientRelations(std::string message)
    {
        // Open a semaphore to protect reading from all users file
        std::string allUsersSemName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
        if (allUsersSem == SEM_FAILED) {
            perror("sem_open failed");
            std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
            log(ERROR, "failed to open semaphore");
            return;
        }
        sem_wait(allUsersSem);
        std::vector<std::string> allUsers = get_all_users_func(synchID);
        sem_post(allUsersSem);
        sem_close(allUsersSem);

        // YOUR CODE HERE

        // TODO: hardcoding 6 here, but you need to get list of all synchronizers from coordinator as before
        // std::string queueName = "synch" + std::to_string(synchID) + "_clients_relations_queue";
        // std::string message = consumeMessage(queueName, 1000); // 1 second timeout

        if (!message.empty())
        {
            Json::Value root;
            Json::Reader reader;
            if (reader.parse(message, root))
            {
                for (const auto &client : allUsers)
                {
                    std::string followerFile = client + "_followers.txt";
                    std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + followerFile;
                    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
                    if (fileSem == SEM_FAILED) {
                        perror("sem_open failed");
                        std::cout << "sem: " << " -> " << semName << std::endl;
                        log(ERROR, "failed to open semaphore");
                        return;
                    }
                    sem_wait(fileSem);
                    std::string followerFilePath = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+followerFile;
                    std::ofstream followerStream(followerFilePath, std::ios::app | std::ios::out | std::ios::in);
                    if (root.isMember(client))
                    {
                        for (const auto &follower : root[client])
                        {
                            std::string cleanFollower = follower.asString().substr(1); // Need to strip the r from the front
                            if (!file_contains_user(followerFile, follower.asString()))
                            {
                                followerStream << follower.asString() << std::endl;
                            }
                        }
                    }
                    followerStream.close();
                    sem_post(fileSem);
                    sem_close(fileSem);
                }
            }
        }
    }

    // for every client in your cluster, update all their followers' timeline files
    // by publishing your user's timeline file (or just the new updates in them)
    //  periodically to the message queue of the synchronizer responsible for that client

    // brando - 
    void publishTimelines()
    {
        std::vector<std::string> users = get_all_users_func(synchID);
        Json::Value timelines;

        for (const auto &client : users)
        {
            int clientId = std::stoi(client);
            int client_cluster = ((clientId - 1) % 3) + 1;
            // only do this for clients in your own cluster
            if (client_cluster != clusterID)
            {
                continue;
            }
            std::cout << "beginning to publish "+client+"'s timeline" << std::endl;

            std::vector<std::string> tl = get_tl_or_fl(synchID, clientId, true);
            std::vector<std::string> followers = getFollowersOfUser(clientId);

            std::cout << "got followers and tl" << std::endl;
            // Build the timeline vector that holds each post that will be published from this client
            std::vector<std::string> posts;
            for (int i = 0; i + 2 < tl.size(); i+=3) {
                std::string post = tl[i]+"\n"+tl[i+1]+"\n"+tl[i+2]+"\n\n";
                std::cout << "POST - "+post << std::endl;
                posts.push_back(post);
            }

            std::cout << "before the loop" << std::endl;
            for (const auto &follower : followers) // Send the timeline updates of your current user to all its followers
            {
                std::cout << "follower "+follower << std::endl;
                if (((std::stoi(follower) - 1) % 3) + 1 != clusterID) {// ONLY PUBLISH TO FOLLOWERS OUTSIDE OF THE CLUSTER
                    std::cout << "publishing to follower: "+follower << std::endl;
                }
                else{
                    continue;
                }
                // Build the post list that the follower will recieve
                Json::Value postList(Json::arrayValue);
                for (const auto& post : posts) {
                    postList.append(post);
                }

                // If there are posts from the client, add them to update destined for this follower
                if (!postList.empty()) {
                    if (!timelines.isMember("t"+follower)) { // If this follower is not in the list of clients recieving updates yet, make them an entry
                        timelines["t"+follower] = Json::Value(Json::arrayValue);
                    }
                    for (const auto& post : postList) {
                        timelines["t"+follower].append(post);
                    }   
                }
            }
        }

        Json::FastWriter writer;
        std::string message = writer.write(timelines);
        std::cout << "publishing these timelines: " + message << std::endl;
        for (int i = 1; i <= 6; i++) {
            if (i != synchID) {
                int status = publishMessage("synch" + std::to_string(i) + "_queue", message);
                if (status != 0) std::cout << "timeline publish failed" << std::endl;
            }
        }
    }

    // For each client in your cluster, consume messages from your timeline queue and modify your client's timeline files based on what the users they follow posted to their timeline
    void consumeTimelines(std::string message)
    {
        // std::string queueName = "synch" + std::to_string(synchID) + "_timeline_queue";
        // std::string message = consumeMessage(queueName, 1000); // 1 second timeout

        if (!message.empty())
        {
            // consume the message from the queue and update the timeline file of the appropriate client with
            // the new updates to the timeline of the user it follows
            Json::Value root;
            Json::Reader reader;

            if (reader.parse(message, root)) {
                for (const auto& user : root.getMemberNames()) { // Iterate through the keys of the recieved object
                    std::string cleanUser = user.substr(1); // Need to strip the t from the front
                    std::string followingPath = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+cleanUser+"_following.txt";
                    std::string followingSemName = "/"+std::to_string(clusterID)+"_"+clusterSubdirectory+"_"+cleanUser+"_following.txt";
                    std::fstream followingFile;
                    sem_t *followingSem = sem_open(followingSemName.c_str(), O_CREAT, 0777, 1);
                    if (followingSem == SEM_FAILED) {
                        perror("sem open failed");
                        std::cout << "sem -> " << followingSem << std::endl;
                        log(ERROR, "Failed to open semaphore to cosume new timeline posts");
                        continue;
                    }
                    sem_wait(followingSem);
                    followingFile.open(followingPath, std::ios::app);
                    if (!followingFile.is_open()) {
                        perror("file open failed");
                        std::cout << followingPath << std::endl;
                        log(ERROR, "Failed to open file while trying to conusme new timeline posts");
                        sem_post(followingSem);
                        sem_close(followingSem);
                        continue;
                    }
                    for (const auto& post : root[user]) { // Append each new post to following file
                        followingFile << post.asString();
                    }
                    followingFile.close();
                    sem_post(followingSem);
                    sem_close(followingSem);
                }
            }
            else {
                log(ERROR, "Failed to parse timeline JSON object");
                return;
            }
        }
        else {
            log(ERROR, "Received empty message");
            return;
        }
    }

private:
    void updateAllUsersFile(const std::vector<std::string> &users)
    {
        std::string usersFile = "all_users.txt";
        std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
        sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
        if (fileSem == SEM_FAILED) {
            perror("sem_open failed");
            std::cout << "sem: " << " -> " << semName << std::endl;
            log(ERROR, "failed to open semaphore while updating all users");
            return;
        }
        // std::cout << "trying to get semaphore: "+semName << std::endl;
        sem_wait(fileSem);

        // Make full path
        std::string usersFullFile = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+usersFile;
        std::ofstream userStream(usersFullFile, std::ios::app | std::ios::out | std::ios::in);
        if (!userStream.is_open()) {
            perror("userStream open failed");
            std::cout << usersFullFile << std::endl;
            log(ERROR, "failed to open file while updating all users");
            sem_post(fileSem);
            sem_close(fileSem);
            return;
        }
        for (std::string user : users)
        {
            // std::cout << "checking if "+user+" is in "+usersFullFile << std::endl;
            if (!file_contains_user(usersFile, user))
            {
                // std::cout << "adding user "+user+" to "+usersFullFile << std::endl;
                userStream << user << std::endl;
            }
        }
        sem_post(fileSem);
        sem_close(fileSem);
        // std::cout << "successfully updated user file" << std::endl;
    }
};

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ);

class SynchServiceImpl final : public SynchService::Service
{
    // You do not need to modify this in any way
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID)
{
    // localhost = 127.0.0.1
    std::string server_address("127.0.0.1:" + port_no);
    log(INFO, "Starting synchronizer server at " + server_address);
    SynchServiceImpl service;
    // grpc::EnableDefaultHealthCheckService(true);
    // grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << server_address << std::endl;

    // Initialize RabbitMQ connection
    // SynchronizerRabbitMQ rabbitMQ("localhost", 5672, synchID);
    SynchronizerRabbitMQ rabbitMQ("rabbitmq", 5672, synchID);

    std::thread t1(run_synchronizer, coordIP, coordPort, port_no, synchID, std::ref(rabbitMQ));

    // Create a consumer thread
    std::thread consumerThread([&rabbitMQ]()
                               {
        while (true) {
            while (true) {
                std::string msg = rabbitMQ.consumeMessage("synch"+std::to_string(rabbitMQ.synchID)+"_queue", 1000);
                if (!msg.empty()) {
                    std::cout << "got message: " << msg << std::endl;
                    Json::Value root;
                    Json::Reader reader;
                    if (reader.parse(msg, root)) {
                        const auto& keys = root.getMemberNames();
                        if (!keys.empty()) {
                            std::string firstKey = keys.at(0);
                            std::cout << "first key: "+firstKey << std::endl;
                            if (firstKey == "users") { // Update all users message
                                std::cout << "go to consumeUserLists" << std::endl;
                                rabbitMQ.consumeUserLists(msg);
                            }
                            else if (firstKey.at(0) == 't') { // Timeline message
                                std::cout << "go to consumeTimelines" << std::endl;
                                rabbitMQ.consumeTimelines(msg);
                            }
                            else { // Client relations message
                                std::cout << "go to consumeClientRelations" << std::endl;
                                rabbitMQ.consumeClientRelations(msg);
                            }
                        }
                    }
                }
                else {
                    break;
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(5));
            // you can modify this sleep period as per your choice
        } });

    server->Wait();

    //   t1.join();
    //   consumerThread.join();
}

int main(int argc, char **argv)
{
    int opt = 0;
    std::string coordIP = "127.0.0.1";
    std::string coordPort = "9000";
    std::string port = "3029";

    while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1)
    {
        switch (opt)
        {
        case 'h':
            coordIP = optarg;
            break;
        case 'k':
            coordPort = optarg;
            break;
        case 'p':
            port = optarg;
            break;
        case 'i':
            synchID = std::stoi(optarg);
            break;
        default:
            std::cerr << "Invalid Command Line Argument\n";
        }
    }
    if (port == coordPort) {
        std::cerr << "FATAL: Synchronizer and Coordinator cannot use the same port!" << std::endl;
        exit(1);
    }

    std::string log_file_name = std::string("synchronizer-") + port;
    google::InitGoogleLogging(log_file_name.c_str());
    log(INFO, "Logging Initialized. Server starting...");

    coordAddr = coordIP + ":" + coordPort;
    clusterID = ((synchID - 1) % 3) + 1;
    ServerInfo serverInfo;
    serverInfo.set_hostname("localhost");
    serverInfo.set_port(port);
    serverInfo.set_type("synchronizer");
    serverInfo.set_serverid(synchID);
    serverInfo.set_clusterid(clusterID);
    bool registration = true;
    Heartbeat(coordIP, coordPort, serverInfo, synchID, registration);

    RunServer(coordIP, coordPort, port, synchID);
    return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID, SynchronizerRabbitMQ &rabbitMQ)
{
    // setup coordinator stub
    std::string target_str = coordIP + ":" + coordPort;
    std::shared_ptr<CoordService::Stub> coord_stub_;
    coord_stub_ = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials())));

    ServerInfo msg;
    Confirmation c;

    msg.set_serverid(synchID);
    msg.set_hostname("127.0.0.1");
    msg.set_port(port);
    msg.set_type("follower");

    // TODO: begin synchronization process
    while (true)
    {
        // the synchronizers sync files every 5 seconds
        sleep(5);

        grpc::ClientContext context;
        ServerList followerServers;
        ID id;
        id.set_id(synchID);

        // making a request to the coordinator to see count of follower synchronizers
        coord_stub_->GetAllFollowerServers(&context, id, &followerServers);

        std::vector<int> server_ids;
        std::vector<std::string> hosts, ports;
        for (std::string host : followerServers.hostname())
        {
            hosts.push_back(host);
        }
        for (std::string port : followerServers.port())
        {
            ports.push_back(port);
        }
        for (int serverid : followerServers.serverid())
        {
            server_ids.push_back(serverid);
        }

        // update the count of how many follower sychronizer processes the coordinator has registered

        // below here, you run all the update functions that synchronize the state across all the clusters
        // make any modifications as necessary to satisfy the assignments requirements

        // Hearbeats to know if this synchronizer's master has died
        bool registration = false;
        Heartbeat(coordIP, coordPort, myInfo, synchID, registration, coord_stub_);

        if (myInfo.ismaster()) { // Only publish if master
            // Publish user list
            std::cout<< "starting to publish" << std::endl;
            rabbitMQ.publishUserList();

            // Publish client relations
            rabbitMQ.publishClientRelations();

            // Publish timelines
            rabbitMQ.publishTimelines();
        }
    }
    return;
}

std::vector<std::string> get_lines_from_file(std::string filename)
{
    std::vector<std::string> users;
    std::string user;
    std::ifstream file;
    // std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    
    // // If not alr created, create the semaphore and allow the process to write to it
    // sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
    // if (fileSem == SEM_FAILED) {
    //     perror("sem_open failed");
    //     std::cout << "sem: " << " -> " << semName << std::endl;
    //     log(ERROR, "failed to open semaphore");
    //     return users;
    // }

    // Wait until lock is available 
    // std::cout << "waiting for semaphore "+semName << std::endl;
    // sem_wait(fileSem);

    // Try to open the file by adding full path, if it fails then release the semaphore
    filename = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+filename;
    file.open(filename);
    if (!file.is_open()) {
        log(ERROR, "failed to open file "+filename);
        // sem_post(fileSem); // Give up the lock
        // sem_close(fileSem); // Deallocate resources
        return users;
    }

    if (file.peek() == std::ifstream::traits_type::eof()) // Check if the file is empty
    {
        // return empty vector if empty file
        // std::cout<<"returned empty vector bc empty file"<<std::endl;
        std::cout << "empty file" << std::endl;
        file.close();
        // sem_post(fileSem); 
        // sem_close(fileSem);
        return users;
    }

    while (getline(file, user)) // Grab next user
    {
        // std::cout << "grabbed "+user << std::endl;
        if (!user.empty()) // If there is no user grabbed, we have reached eof
            users.push_back(user);
    }

    file.close();
    // sem_post(fileSem);
    // sem_close(fileSem);

    return users;
}

void Heartbeat(std::string coordinatorIp, std::string coordinatorPort, ServerInfo serverInfo, int syncID, bool registration,
    std::shared_ptr<CoordService::Stub> stub)
{
    // For the synchronizer, a single initial heartbeat RPC acts as an initialization method which
    // servers to register the synchronizer with the coordinator and determine whether it is a master

    if (!stub) { // If there was no stub passed in, make a new one. otherwise, use the one you were given
        // std::cout << "made a new stub" << std::endl;
        std::string coordinatorInfo = coordinatorIp + ":" + coordinatorPort;
        stub = std::shared_ptr<CoordService::Stub>(CoordService::NewStub(grpc::CreateChannel(coordinatorInfo, grpc::InsecureChannelCredentials())));
    }
    else {
        // std::cout << "stub passed in" << std::endl;
    }
    // send a heartbeat to the coordinator, which registers your follower synchronizer as either a master or a slave
    ClientContext registrationContext;
    Confirmation confirmation;
    registrationContext.AddMetadata("synchronizer", "yes");
    if (registration) registrationContext.AddMetadata("registration-heartbeat", "yes");
    myInfo.set_clusterid(clusterID);
    myInfo.set_serverid(syncID);
    if (registration) std::cout << "registration type" << std::endl;
    if (!stub) std::cout << "stub died" << std::endl;
    Status registrationStatus = stub->Heartbeat(&registrationContext, std::ref(myInfo), &confirmation);
    if (!registrationStatus.ok()) {
        std::cerr << "Heartbeat failed: " << registrationStatus.error_message()
              << " (code " << registrationStatus.error_code() << ")" << std::endl;
    }
    // else {
    //     std::cout << "good hb from coord" << std::endl;
    // }

    // Determine synchronizer's role
    bool amMaster = confirmation.status();
    if (amMaster) {
        myInfo.set_ismaster(amMaster);
        if (registration) clusterSubdirectory = "1"; // If I am master synchronizer @ registration
        // std::cout << std::to_string(myInfo.serverid())+" is the master SYNCH of cluster "+std::to_string(myInfo.clusterid())+"\n";
    }
    else {
        myInfo.set_ismaster(amMaster);
        if (registration) clusterSubdirectory = "2";
        // std::cout << "i am not the master of my cluster" << std::endl;
    }
    // YOUR CODE HERE
}

bool file_contains_user(std::string filename, std::string user)
{
    // std::cout << "in top level file contains user func" << std::endl;
    std::vector<std::string> users;
    // check username is valid
    // std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + filename;
    // sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
    // if (fileSem == SEM_FAILED) {
    //     perror("sem_open failed");
    //     std::cout << "sem: " << " -> " << semName << std::endl;
    //     log(ERROR, "failed to open semaphore");
    //     return false;
    // }
    // std::cout << "trying to get semaphore: " << semName << std::endl;
    // sem_wait(fileSem);
    // std:: cout << "get the users list" << std::endl;
    users = get_lines_from_file(filename);
    // std::cout << "got em" << std::endl;
    for (int i = 0; i < users.size(); i++)
    {
        // std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
        if (user == users[i])
        {
            // std::cout<<"found"<<std::endl;
            // sem_post(fileSem);
            // sem_close(fileSem);
            return true;
        }
    }
    // std::cout << "user "+user+" is not there" << std::endl;
    // sem_post(fileSem);
    // sem_close(fileSem);
    return false;
}

std::vector<std::string> get_all_users_func(int synchID)
{
    std::string users_file = "all_users.txt";
    std::vector<std::string> user_list = get_lines_from_file(users_file);
    
    return user_list;
}

std::vector<std::string> get_tl_or_fl(int synchID, int clientID, bool tl)
{
    // std::cout << "grabbing "+std::to_string(clientID)+"'s timeline" << std::endl;
    std::vector<std::string> timeline;
    std::string file = std::to_string(clientID);
    if (tl)
    {
        file.append(".txt");
    }
    else
    {
        file.append("_followers.txt");
    }
    std::string filePath = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+file;
    std::string semName = "/"+std::to_string(clusterID)+"_"+clusterSubdirectory+"_"+file;
    std::ifstream fileStream;
    std::fstream truncator;
    sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
    if (fileSem == SEM_FAILED) {
        perror("sem_open failed");
        std::cout << "sem: " << " -> " << semName << std::endl;
        log(ERROR, "failed to open semaphore while getting a timeline");
        return timeline;
    }
    else {
        // std::cout << "grabbed semaphore " << semName << std::endl;
    }
    sem_wait(fileSem);
    fileStream.open(filePath);
    if (!fileStream.is_open()) {
        log(ERROR, "failed to open file "+filePath)
        sem_post(fileSem);
        sem_close(fileSem);
        return timeline;
    }
    else {
        // std::cout << "opened file " + filePath << std::endl;
    }
    // Get all non-whitespace lines from the specified file
    std::string line;
    while(getline(fileStream, line)) {
        if (!line.empty()) { // Skip whitespace
            // std::cout << line << std::endl;
            timeline.push_back(line);
        }
    }
    fileStream.close();
    // Truncate the timeline file so that only new posts will be there on next check
    truncator.open(filePath, std::ios::out | std::ios::trunc);
    if (!truncator.is_open()) {
        log(ERROR, "failed to truncate file "+filePath);
        sem_post(fileSem);
        sem_close(fileSem);
        return timeline;
    }
    truncator.close();
    sem_post(fileSem);
    sem_close(fileSem);


    return timeline;
}

std::vector<std::string> getFollowersOfUser(int ID)
{
    std::vector<std::string> followers;
    std::string clientID = std::to_string(ID);
    // // Open a semaphore to protect reading from all users file
    // std::string allUsersSemName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_all_users.txt";
    // sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
    // if (allUsersSem == SEM_FAILED) {
    //     perror("sem_open failed");
    //     std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
    //     log(ERROR, "failed to open semaphore");
    //     return followers;
    // }
    // // std::cout << "trying to get semaphore: "+allUsersSemName << std::endl;
    // sem_wait(allUsersSem);
    // std::vector<std::string> usersInCluster = get_all_users_func(synchID);
    // sem_post(allUsersSem);
    // sem_close(allUsersSem);

    // Literally just read in the followers folder
    std::string followersPath = "./cluster"+std::to_string(clusterID)+"/"+clusterSubdirectory+"/"+clientID+"_followers.txt";
    std::string followersSemName = "/"+std::to_string(clusterID)+"_"+clusterSubdirectory+"_"+clientID+"_followers.txt";
    std::fstream followersFile;
    sem_t *followersSem = sem_open(followersSemName.c_str(), O_CREAT, 0777, 1);
    if (followersSem == SEM_FAILED) {
        perror("sem_open failed");
        std::cout << "sem: " << " -> " << followersSemName << std::endl;
        log(ERROR, "failed to open semaphore while getting followers");
        return followers;
    }
    sem_wait(followersSem);
    followersFile.open(followersPath, std::ios::in);
    if (!followersFile.is_open()) {
        log(ERROR, "Failed to open file"+followersPath);
        sem_post(followersSem);
        sem_close(followersSem);
        return followers;
    }
    std::string line;
    while (getline(followersFile, line)) {
        if (!line.empty()) {
            followers.push_back(line);
        }
    }
    followersFile.close();
    sem_post(followersSem);
    sem_close(followersSem);

    // for (auto userID : usersInCluster)
    // { // Examine each user's following file
    //     std::string file = userID + "_follow_list.txt";
    //     std::string semName = "/" + std::to_string(clusterID) + "_" + clusterSubdirectory + "_" + file;
    //     sem_t *fileSem = sem_open(semName.c_str(), O_CREAT, 0777, 1);
    //     if (fileSem == SEM_FAILED) {
    //         perror("sem_open failed");
    //         std::cout << "sem: " << " -> " << semName << std::endl;
    //         log(ERROR, "failed to open semaphore");
    //         return followers;
    //     }
    //     sem_wait(fileSem);
    //     // std::cout << "Reading file " << file << std::endl;
    //     if (file_contains_user(file, clientID))
    //     {
    //         followers.push_back(userID);
    //     }
    //     sem_post(fileSem);
    //     sem_close(fileSem);
    // }

    return followers;
}
