/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <chrono>
#include <thread>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include <glog/logging.h>
#include <filesystem>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"
#include "coordinator.grpc.pb.h"

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ClientContext;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;


struct Client {
  std::string username;
  bool connected = true;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;

// Stub to communicate with coordinator
std::shared_ptr<CoordService::Stub> coordStub_;
// Holding the information for this server instance
ServerInfo myInfo;

class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override {
    Client* client;
    for (int i = 0; i < client_db.size(); i++) {
      // Find the client that made the request
      if (client_db.at(i)->username == request->username()) {
        client = client_db.at(i);
        break;
      }
      // If the requested username is not in the db, return error status
      if (i == client_db.size() - 1) { // Reached last index and still no match
        Status no(grpc::StatusCode::CANCELLED, "Requestor does not exist");
        return no; // Would make this more accurate error message in full imlpementation
      }
    }

    // Add all users in db to list reply
    for (int i = 0; i < client_db.size(); i++) {
      list_reply->add_all_users(client_db.at(i)->username);
    } 

    // Add all followers of client to list reply
    for (int i = 0; i < client->client_followers.size(); i++) {
      list_reply->add_followers(client->client_followers.at(i)->username);
    }

    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string requestorName = request->username();
    std::string targetName = request->arguments(0);

    // You cannot follow yourself
    if (targetName == requestorName) {
      reply->set_msg("You cannot follow yourself");
      Status no(grpc::StatusCode::OK, "You cannot follow yourself");
      return no;
    }

    // Find the client that made the request
    Client* requestor;
    for (int i = 0; i < client_db.size(); i++) {
      if (client_db.at(i)->username == requestorName) {
        requestor = client_db.at(i);
        break;
      }
      // If the requestor's username is not in the db, return error status
      if (i == client_db.size() - 1) { // Reached last index and still no match
        reply->set_msg("Requestor does not exist");
        Status no(grpc::StatusCode::OK, "Requestor does not exist");
        return no; // Would make this more accurate error message in full imlpementation
      }
    }

    // Find the client that is the target of the follow request
    Client* target;
    for (int i = 0; i < client_db.size(); i++) {
      if (client_db.at(i)->username == targetName) {
        target = client_db.at(i);
        break;
      }
      // If the target's username is not in the db, return error status
      if (i == client_db.size() - 1) { // Reached last index and still no match
        std::stringstream errmsg;
        errmsg << "Target does not exist " << targetName;
        reply->set_msg("Target does not exist");
        Status no(grpc::StatusCode::OK, errmsg.str());
        return no; // Would make this more accurate error message in full imlpementation
      }
    }

    // Add target to requestor's following list if not already following
    std::vector<Client*>::iterator finder;
    if (std::find(requestor->client_following.begin(), requestor->client_following.end(), target) 
    == requestor->client_following.end()) {
      requestor->client_following.push_back(target);
    }
    else {
      reply->set_msg("You already follow the target");
      Status no(grpc::StatusCode::OK, "You already follow the target");
      return no;
    }
    
    // Add requestor to target's followers list if not already a follower
    if (std::find(target->client_followers.begin(), target->client_followers.end(), requestor)
    == target->client_followers.end()) {
      target->client_followers.push_back(requestor);
    }
    else {
      reply->set_msg("Requestor already follows you");
      Status no(grpc::StatusCode::OK, "Requestor already follows you");
      return no;
    }

    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string requestorName = request->username();
    std::string targetName = request->arguments(0);

    // You cannot unfollow yourself
    if (targetName == requestorName) {
      reply->set_msg("You cannot unfollow yourself");
      Status no(grpc::StatusCode::OK, "You cannot unfollow yourself");
      return no;
    }

    // Find the client that made the request
    Client* requestor;
    for (int i = 0; i < client_db.size(); i++) {
      if (client_db.at(i)->username == requestorName) {
        requestor = client_db.at(i);
        break;
      }
      // If the requestor's username is not in the db, return error status
      if (i == client_db.size() - 1) { // Reached last index and still no match
        reply->set_msg("Requestor does not exist");
        Status no(grpc::StatusCode::OK, "Requestor does not exist");
        return no; // Would make this more accurate error message in full imlpementation
      }
    }

    // Find the client that is the target of the follow request
    Client* target;
    for (int i = 0; i < client_db.size(); i++) {
      if (client_db.at(i)->username == targetName) {
        target = client_db.at(i);
        break;
      }
      // If the target's username is not in the db, return error status
      if (i == client_db.size() - 1) { // Reached last index and still no match
        reply->set_msg("Target does not exist");
        Status no(grpc::StatusCode::OK, "Target does not exist");
        return no; // Would make this more accurate error message in full imlpementation
      }
    }


    // Remove target from requestor's following if they are there
    if (std::find(requestor->client_following.begin(), requestor->client_following.end(), target) == 
    requestor->client_following.end()) {
      reply->set_msg("You were never following the target");
      Status no(grpc::StatusCode::OK, "You were never following the target");
      return no;
    }
    else {
      requestor->client_following.erase(
        std::remove(requestor->client_following.begin(), requestor->client_following.end(), target),
        requestor->client_following.end()
      );
    }

    // Remove requestor from target's followers if they are there
    if (std::find(target->client_followers.begin(), target->client_followers.end(), requestor) 
    == target->client_followers.end()) {
      reply->set_msg("Requestor was never your follower");
      Status no(grpc::StatusCode::OK, "Requestor was never your follower");
    }
    else {
      target->client_followers.erase(
        std::remove(target->client_followers.begin(), target->client_followers.end(), requestor),
        target->client_followers.end()
      );
    }

    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {
    std::string clientName = request->username();
    Client* client;
    // Check if the requested username is already logged in
    for (int i = 0; i < client_db.size(); i++) {
      // If so, return login failed
      if (client_db.at(i)->username == clientName) {
        reply->set_msg("You are already logged in");
        Status no(grpc::StatusCode::CANCELLED, "You are already logged in");
        return no; // Can make this a better error/failure message
      }
    }

    // Log new client in
    client = new Client();
    client->username = clientName;
    client_db.push_back(client);
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message message;
    while (stream->Read(&message)) {
      std::string username = message.username();
      std::string userFilePath = username+".txt";
      std::string userFollowingPath = username+"_following.txt";
      google::protobuf::Timestamp timestamp = message.timestamp();
      std::string timeString = google::protobuf::util::TimeUtil::ToString(timestamp);
      std::stringstream formattedBuf;
      // std::cout << username+" called timeline\n";

      // Find the message author in db
      Client* userClient = nullptr;
      for (int i = 0; i < client_db.size(); i++) {
        if (client_db.at(i)->username == username) {
          userClient = client_db.at(i);
          userClient->stream = stream;
          break;
        }
        // Honestly not sure when this would happen, but for redundancy
        if (i == client_db.size() - 1) {
          Status no(grpc::StatusCode::CANCELLED, "The author of read-in message does not exist");
          return no;
        }
      }
        // Make formatted post
        formattedBuf << "T "+timeString+"\n" << "U "+username+"\n" << "W "+message.msg()+"\n\n";

        // Check if the user has previously open timeline stream
        if (message.msg() != "Hi server thismostlikelywouldnotbewrittenbyaperson") { // Not hello message, append message to user post history
          std::cout<< username << " posted\n";
          std::fstream userFile;
          // Open user file to append
          userFile.open(userFilePath, std::ios::app);
          if (!userFile.is_open()) {
            Status no(grpc::StatusCode::CANCELLED, "Failed to open user's post history file");
            return no;
          }
          userFile << formattedBuf.str();
          userFile.close();
        }
        else { // User just used timeline command for the first time, show last 20 following posts
          std::cout<<username<< " entered timeline\n";
          std::vector<std::string> post;
          std::fstream userFollowing;
          std::string line;
          std::vector<std::vector<std::string>> posts;
          userFollowing.open(userFollowingPath, std::ios::app);
          if (!userFollowing) {
            Status no(grpc::StatusCode::CANCELLED, "Couldn't open/create user following file");
          }
          userFollowing.close();
          userFollowing.open(userFollowingPath, std::ios::in);
          if (userFollowing.is_open()) {
            int counter = 0;
            while (std::getline(userFollowing, line)) {
              // Skip the empty lines to maintain counting logic
              if (line.empty()) continue; 

              // Skip the beginning T or U or wtv, just get the data
              line = line.substr(2);
              post.push_back(line);
              counter++;
              if (counter == 3) {
                // Once we have all parts of a post  
                posts.push_back(post); // push it to posts
                post.clear(); // clear the post vector to get the rest
                counter = 0; // and reset the counter
              }
            }
          }
          userFollowing.close();

          // Now that we have all the posts that could be in user's feed, write them in backwards
          // First reverse the vector so we can read it in backwards
          std::reverse(posts.begin(), posts.end());
          // Write the latest 20 to client's stream
          size_t numRead = std::min(posts.size(), size_t(20));
          for (size_t i = 0; i < numRead; i++) {
            grpc::WriteOptions options;
            Message messageWrite;
            google::protobuf::Timestamp* stamp = new google::protobuf::Timestamp();
            std::string rfcString = posts.at(i).at(0);
            rfcString.replace(10, 1, "T");
            rfcString.append("Z");
            google::protobuf::util::TimeUtil::FromString(rfcString, stamp);
            messageWrite.set_msg(posts.at(i).at(2));
            messageWrite.set_username(posts.at(i).at(1));
            messageWrite.set_allocated_timestamp(stamp);
            stream->Write(messageWrite, options);
          }
          
        } 
        
        // Write post to followers' stream and append to following file(s) if it is not the hello message
        if (message.msg() != "Hi server thismostlikelywouldnotbewrittenbyaperson") {
          for (Client* follower : userClient->client_followers) {
            grpc::WriteOptions options;
            Message messageWrite;
            messageWrite.set_username(username);
            auto *followerTimestamp = new google::protobuf::Timestamp(timestamp);
            messageWrite.set_allocated_timestamp(followerTimestamp);
            messageWrite.set_msg(message.msg());            
            if (follower->stream != 0) {
              std::cout<<"Writing to "<< follower->username <<"'s stream\n";
              follower->stream->Write(messageWrite, options);
              std::cout<<"Succesfully wrote to "<< follower->username <<"'s stream\n";
            }

            // Append to following files
            std::string followerFollowingPath = follower->username+"_following.txt";
            std::fstream followerFollowing(followerFollowingPath, std::ios::app);
            if (!followerFollowing.is_open()) {
              Status no(grpc::StatusCode::CANCELLED, "A follower's following file was not able to be opened");
              return no;
            }
            followerFollowing << formattedBuf.str();
            followerFollowing.close();
            std::cout << username+" is writing to "<<follower->username<< std::endl;
          }
        }
    }
    
    return Status::OK;
  }

};

void sendHeartbeat(ServerInfo& info, std::shared_ptr<CoordService::Stub> coordStub, 
  const std::string& cid) {
  // Initilaize arguments for registration
  ClientContext registrationContext;
  Confirmation confirmation;
  registrationContext.AddMetadata("clusterid", cid);
  registrationContext.AddMetadata("registration-heartbeat", "yes");
  Status registrationStatus = coordStub->Heartbeat(&registrationContext, info, &confirmation);

  log(INFO, "Registration heartbeat from "+cid+"/"+std::to_string(info.serverid()));

  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Send heartbeat every 5 seconds
  while (1) {
    ClientContext context;
    context.AddMetadata("clusterid",cid);
    Status heartbeatStatus = coordStub->Heartbeat(&context, info, &confirmation);
    log(INFO, "Normal heartbeat from "+cid+"/"+std::to_string(info.serverid()));

    std::this_thread::sleep_for(std::chrono::seconds(5));
 }
}

void RunServer(std::string port_no, std::string clusterId, std::string serverId,
  std::string coordIp, std::string coordPort) {
  std::thread hb;
  if (coordIp != "-1") {
    // Make stub to communicate with coordinator
    std::string coordinatorConnection = coordIp + ":" + coordPort;
    auto coordinatorChannel = grpc::CreateChannel(coordinatorConnection, grpc::InsecureChannelCredentials());
    coordStub_ = CoordService::NewStub(coordinatorChannel);

    // Build ServerInfo object with this server's info
    myInfo.set_hostname(coordIp);
    myInfo.set_port(port_no);
    myInfo.set_serverid(std::stoi(serverId));

    // Start heartbeat thread
    hb = std::thread(sendHeartbeat, std::ref(myInfo), std::move(coordStub_), std::ref(clusterId));
  }

  std::string server_address = "127.0.0.1:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();

  if (hb.joinable()) hb.join();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string coordIp = "-1";
  std::string coordPort = "9090";
  std::string clusterId = "null";
  std::string serverId = "null";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:c:s:h:k:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      case 'c':
          clusterId = optarg;break;
      case 's':
          serverId = optarg;break;
      case 'h': 
          coordIp = optarg;break;
      case 'k':
          coordPort = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }

  if (clusterId == "" || serverId == "") {
    std::cerr << "You must provide cluster and server id";
    exit(1);
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port, clusterId, serverId, coordIp, coordPort);

  return 0;
}
