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
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#include <filesystem>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 

#include "sns.grpc.pb.h"


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
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
      Status no(grpc::StatusCode::CANCELLED, "You cannot follow yourself");
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
        Status no(grpc::StatusCode::CANCELLED, "Requestor does not exist");
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
        Status no(grpc::StatusCode::CANCELLED, errmsg.str());
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
      Status no(grpc::StatusCode::CANCELLED, "You already follow the target");
      return no;
    }
    
    // Add requestor to target's followers list if not already a follower
    if (std::find(target->client_followers.begin(), target->client_followers.end(), requestor)
    == target->client_followers.end()) {
      target->client_followers.push_back(requestor);
    }
    else {
      reply->set_msg("You already follow the target");
      Status no(grpc::StatusCode::CANCELLED, "You already follow the target");
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
      Status no(grpc::StatusCode::CANCELLED, "You cannot unfollow yourself");
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
        Status no(grpc::StatusCode::CANCELLED, "Requestor does not exist");
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
        Status no(grpc::StatusCode::CANCELLED, "Target does not exist");
        return no; // Would make this more accurate error message in full imlpementation
      }
    }


    // Remove target from requestor's following
    requestor->client_following.erase(
      std::remove(requestor->client_following.begin(), requestor->client_following.end(), target),
      requestor->client_following.end()
    );

    // Remove requestor from target's followers
    target->client_followers.erase(
      std::remove(target->client_followers.begin(), target->client_followers.end(), requestor),
      target->client_followers.end()
    );

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
      std::string userFilePath = "./posts_db/"+username+".txt";
      std::string userFollowingPath = "./posts_db/"+username+".txt";
      google::protobuf::Timestamp timestamp = message.timestamp();
      std::string timeString = google::protobuf::util::TimeUtil::ToString(timestamp);
      std::stringstream formattedBuf;

      // Find the message author in db
      Client* userClient;
      for (int i = 0; i < client_db.size(); i++) {
        if (client_db.at(i)->username == username) {
          userClient = client_db.at(i);
          break;
        }
        // Honestly not sure when this would happen, but for redundancy
        if (i == client_db.size() - 1) {
          Status no(grpc::StatusCode::CANCELLED, "The author of read-in message does not exist");
          return no;
        }

        // Make formatted post
        formattedBuf << "T "+timeString+"\n" << "U "+username+"\n" << "W "+message.msg()+"\n\n";
        //Check if the user has previously open timeline stream
        if (std::filesystem::exists(userFilePath)) { // If file already exists, append message to user post history
          std::fstream userFile;
          userFile.open(userFilePath);
          if (!userFile.is_open()) {
            Status no(grpc::StatusCode::CANCELLED, "Failed to open user's post history file");
            return no;
          }
          userFile << formattedBuf.str();
        }
        else { // User just used timeline command for the first time, show last 20 following posts
          std::vector<std::string> post;
          std::fstream userFollowing;
          std::string line;
          std::vector<std::vector<std::string>> posts;
          userFollowing.open(userFollowingPath);
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
          // Now that we have all the posts that could be in user's feed, write them in backwards
          std::stringstream latest20;

          // First reverse the iterator so we can read it in backwards
          std::reverse(posts.begin(), posts.end());
          for (std::vector<std::string> post : posts) {
            std::stringstream postBuf;
            postBuf << post.at(1) << " (" << post.at(0) << ") >> "<< post.at(2) << "\n";
            latest20 << postBuf.str();
          }

          // Write the latest 20 to client's stream
          grpc::WriteOptions options;
          Message message;
          message.set_msg(latest20.str());
          stream->Write(message, options);
        } 
        
        // Write post to followers' stream
        for (Client* follower : userClient->client_followers) {
          grpc::WriteOptions options;
          Message message;
          message.set_msg(formattedBuf.str());
          follower->stream->Write(message, options);
          std::string followerFollowingPath = "./posts_db/"+follower->username+".txt";
          std::fstream follwerFollowing(followerFollowingPath);
        }
      }
    }
    
    return Status::OK;
  }

};

void RunServer(std::string port_no) {
  std::string server_address = "127.0.0.1:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  server->Wait();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port);

  return 0;
}
