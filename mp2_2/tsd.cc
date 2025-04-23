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
#include <semaphore.h>
#include <glog/logging.h>
#include <fcntl.h>   
#include <unistd.h>
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
using csce438::ID;

namespace fs = std::filesystem;


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
// Stub to communicate with slave
std::shared_ptr<SNSService::Stub> slaveStub_;
// Holding the information for this server instance
ServerInfo myInfo;

// Utility to check if a file has a string in it
bool file_contains_user(std::string filename, std::string user)
{
  std::string fullFile = "./cluster"+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"/"+filename;
  std::string tempUser;
  std::ifstream fileStream;
  fileStream.open(fullFile);
  if (!fileStream.is_open()) {
    log(ERROR, "Couldn't open all users file while checking if someone was already in there");
    return false;
  }
  while (getline(fileStream, tempUser)) {
    if (!user.empty()) {
      if (tempUser == user) { // Check if user in the file matches target
        return true;
      }
    }
  }
  return false;
}

// Utility to remove all client data (when becoming master so you have a clean slate for users to re-login to)
// void clearClientData() {
//   // Go through the db and erase disk (file) data
//   for (auto& c : client_db) {
//     std::string user = c->username;
//     std::string filePrefix = "./client"+std::to_string(myInfo.clusterid())+"/"+std::to_string(serverid())+"/";
//     std::string semPrefix = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_";
//     std::vector<std::string> paths;
//     paths.push_back(); // User following path
//     paths.push_back(); // User followers path
//     paths.push_back(); // 
//   }
//   client_db.clear(); // Finally remove all in-memory data
// }

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

    if (!myInfo.ismaster()) {
      std::cout << "im a slave getting a replicated follow request" << std::endl;
    }

    // Forward the RPC to your slave
    if (myInfo.ismaster()) {
      // Find your slave 
      ClientContext forCoord;
      ServerInfo slaveInfo;
      ID myId;
      myId.set_id(myInfo.clusterid());
      Status slaveReqStatus = coordStub_->GetSlave(&forCoord, myId, &slaveInfo);

      if (slaveInfo.serverid() != -1) {
        std::cout << "i have a slave" << std::endl;
        // Connect to your slave
        std::string slaveHostname = slaveInfo.hostname();
        std::string slavePort = slaveInfo.port();
        log(INFO, "Master of cluster "+std::to_string(myInfo.clusterid())+" has discovered slave "+
        std::to_string(slaveInfo.serverid())+"\n");
        std::cout << "Master of cluster "+std::to_string(myInfo.clusterid())+" has discovered slave "+
        std::to_string(slaveInfo.serverid())+"\n" << std::endl;

        std::string slaveConnection = slaveHostname+":"+slavePort;
        slaveStub_ = SNSService::NewStub(grpc::CreateChannel(slaveConnection, grpc::InsecureChannelCredentials()));

        // Send them this RPC
        ClientContext forSlave;
        Request req;
        Reply reply;
        req.set_username(requestorName);
        req.add_arguments(targetName);
        Status slaveResponseStatus = slaveStub_->Follow(&forSlave, req, &reply);
      }
    }

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

    std::string directory = "./cluster" + std::to_string(myInfo.clusterid()) + "/" + std::to_string(myInfo.serverid())+"/";
    std::string semPrefix = "/"+ std::to_string(myInfo.clusterid()) + "_" + std::to_string(myInfo.serverid())+"_";

    // Add target to requestor's following list & follow_list file if not already following
    if (std::find(requestor->client_following.begin(), requestor->client_following.end(), target) 
    == requestor->client_following.end()) {
      requestor->client_following.push_back(target);
      std::string followListPath = directory + requestor->username+"_follow_list.txt";
      std::string followListSemName = semPrefix + requestor->username+"_follow_list.txt";
      std::fstream followList;
      sem_t *followListSem = sem_open(followListSemName.c_str(), O_CREAT, 0777, 1);
      if (followListSem == SEM_FAILED) {
        perror("sem_open failed");
        std::cout << "sem: " << " -> " << followListSemName << std::endl;
        log(ERROR, "Couldn't open semaphore when writing to user's follow list file");
        Status no(grpc::StatusCode::CANCELLED, "Failed to open user's follow list semaphore");
        return no;
      }
      sem_wait(followListSem);
      followList.open(followListPath, std::ios::app);
      if (!followList.is_open()) {
        Status no(grpc::StatusCode::CANCELLED, "Failed to open user's follow list file");
        sem_post(followListSem);
        sem_close(followListSem);
        return no;
      }
      followList << target->username << std::endl; // Add the target to the requestor's follow list
      followList.close();
      sem_post(followListSem);
      sem_close(followListSem);
    }
    else {
      reply->set_msg("You already follow the target");
      Status no(grpc::StatusCode::OK, "You already follow the target");
      return no;
    }
    
    // Add requestor to target's followers list & followers file if not already a follower
    if (std::find(target->client_followers.begin(), target->client_followers.end(), requestor)
    == target->client_followers.end()) {
      target->client_followers.push_back(requestor);
      std::string followersListPath = directory+target->username+"_followers.txt";
      std::string followersListSemName = semPrefix+target->username+"_followers.txt";
      std::fstream followersList;
      sem_t *followersListSem = sem_open(followersListSemName.c_str(), O_CREAT, 0777, 1);
      if (followersListSem == SEM_FAILED) {
        perror("sem_open failed");
        std::cout << "sem: " << " -> " << followersListSemName << std::endl;
        log(ERROR, "Couldn't open semaphore when writing to user's followers list file");
        Status no(grpc::StatusCode::CANCELLED, "Failed to open user's followers list semaphore");
        return no;
      }
      sem_wait(followersListSem);
      followersList.open(followersListPath, std::ios::app);
      if (!followersList.is_open()) {
        Status no(grpc::StatusCode::CANCELLED, "Failed to open user's followers list file");
        sem_post(followersListSem);
        sem_close(followersListSem);
        return no;
      }
      followersList << requestor->username << std::endl;
      followersList.close();
      sem_post(followersListSem);
      sem_close(followersListSem);
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

    if (!myInfo.ismaster()) {
      std::cout << "im a slave getting a replicated login request" << std::endl;
    }

    // Forward the RPC to your slave
    if (myInfo.ismaster()) {
      // Find your slave 
      ClientContext forCoord;
      ServerInfo slaveInfo;
      ID myId;
      myId.set_id(myInfo.clusterid());
      Status slaveReqStatus = coordStub_->GetSlave(&forCoord, myId, &slaveInfo);

      if (slaveInfo.serverid() != -1) {
        std::cout << "i have a slave" << std::endl;
        // Connect to your slave
        std::string slaveHostname = slaveInfo.hostname();
        std::string slavePort = slaveInfo.port();
        log(INFO, "Master of cluster "+std::to_string(myInfo.clusterid())+" has discovered slave "+
        std::to_string(slaveInfo.serverid())+"\n");
        std::cout << "Master of cluster "+std::to_string(myInfo.clusterid())+" has discovered slave "+
        std::to_string(slaveInfo.serverid())+"\n" << std::endl;

        std::string slaveConnection = slaveHostname+":"+slavePort;
        slaveStub_ = SNSService::NewStub(grpc::CreateChannel(slaveConnection, grpc::InsecureChannelCredentials()));
        // if (slaveStub_ != nullptr) {
        //   std::cout << "yay" << std::endl;
        // }

        // Send them this RPC
        ClientContext forSlave;
        Request req;
        Reply reply;
        req.set_username(clientName);
        Status slaveResponseStatus = slaveStub_->Login(&forSlave, req, &reply);
        std::cout << reply.msg() << std::endl;
      }
    }

    // Create this server's directories (silently succeeds if they already exist)
    fs::path directory = "./cluster" + std::to_string(myInfo.clusterid()) + "/" + std::to_string(myInfo.serverid())+"/";

    try {
      fs::create_directories(directory);  
      log(INFO, "Created/Verified "+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"'s directory\n");
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Error: " << e.what() << std::endl;
    }

    Client* client;
    // Check if the requested username is already logged in
    for (int i = 0; i < client_db.size(); i++) {
      // If so, return login failed
      if (client_db.at(i)->username == clientName) {
        std::cout << client_db.at(i)->username << "is already logged in" << std::endl;
        reply->set_msg("You are already logged in");
        Status no(grpc::StatusCode::CANCELLED, "You are already logged in");
        return no; // Can make this a better error/failure message
      }
    }

    // Log new client in & write their username to all users file safely
    client = new Client();
    client->username = clientName;
    client_db.push_back(client);
    std::string allUsersSemName = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_all_users.txt";
    std::string allUsersPath = directory.string()+"all_users.txt";
    std::fstream allUsers;
    sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
    if (allUsersSem == SEM_FAILED) {
      perror("sem_open failed");
      std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
      log(ERROR, "Couldn't open semaphore when writing to all users file");
      Status no(grpc::StatusCode::CANCELLED, "Failed to open all users semaphore");
      return no;
    }
    else {
      std::cout << "login - sem: " << " -> " << allUsersSemName << std::endl;
    }
    sem_wait(allUsersSem);
    allUsers.open(allUsersPath, std::ios::app);
    if (!allUsers.is_open()) {
      Status no(grpc::StatusCode::CANCELLED, "Failed to open all users file");
      sem_post(allUsersSem);
      sem_close(allUsersSem);
      return no;
    }
    if (!file_contains_user("all_users.txt", clientName)) {
      allUsers << clientName << std::endl; // Need to check if they're already there (re-connections)
    }
    else {
      std::cout << clientName+" is already logged in" << std::endl;
    }
    allUsers.close();
    sem_post(allUsersSem);
    sem_close(allUsersSem);

    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    Message message;
    while (stream->Read(&message)) {
      // The prefix to all of this server's semaphores
      std::string semPrefix = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_";

      // Create this server's directories (silently succeeds if they already exist)
      fs::path directory = "./cluster" + std::to_string(myInfo.clusterid()) + "/" + std::to_string(myInfo.serverid())+"/";
      try {
        fs::create_directories(directory);  
        log(INFO, "Created/Verified "+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"'s directory\n");
      } catch (const fs::filesystem_error& e) {
          std::cerr << "Error: " << e.what() << std::endl;
      }

      std::string username = message.username();
      std::string userFilePath = directory.string()+username+".txt";
      std::string userFileSemName = semPrefix+username+".txt";
      std::string userFollowingPath = directory.string()+username+"_following.txt";
      std::string userFollowingSemName = semPrefix+username+"_following.txt";
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
          log(INFO, username + " posted\n");
          std::fstream userFile;
          // Open semaphore for this file first
          sem_t *userFileSem = sem_open(userFileSemName.c_str(), O_CREAT, 0777, 1);
          if (userFileSem == SEM_FAILED) {
            Status no(grpc::StatusCode::CANCELLED, "Failed to open user's post history semaphore");
            return no;
          }
          // Open user file to append
          sem_wait(userFileSem);
          userFile.open(userFilePath, std::ios::app);
          if (!userFile.is_open()) {
            Status no(grpc::StatusCode::CANCELLED, "Failed to open user's post history file");
            return no;
          }
          userFile << formattedBuf.str();
          userFile.close();
          sem_post(userFileSem);
          sem_close(userFileSem);
        }
        else { // User just used timeline command for the first time, show last 20 following posts
          log(INFO, username + " entered timeline\n");
          std::vector<std::string> post;
          std::fstream userFollowing;
          std::string line;
          std::vector<std::vector<std::string>> posts;

          sem_t *userFollowingSem = sem_open(userFollowingSemName.c_str(), O_CREAT, 0777, 1);
          if (userFollowingSem == SEM_FAILED) {
            log(ERROR, "Couldn't open semaphore when writing");
            Status no(grpc::StatusCode::CANCELLED, "Couldn't open/create user following semaphore");
            return no;
          }
          sem_wait(userFollowingSem);
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
          sem_post(userFollowingSem);
          sem_close(userFollowingSem);

          // Now that we have all the posts that could be in user's feed, write them in backwards
          // // First reverse the vector so we can read it in backwards
          // std::reverse(posts.begin(), posts.end());
          // Write the latest 20 to client's stream
          size_t numRead = std::min(posts.size(), size_t(20));
          std::vector<std::vector<std::string>> thesePosts(posts.end() - numRead, posts.end());
          for (size_t i = 0; i < numRead; i++) {
            grpc::WriteOptions options;
            Message messageWrite;
            google::protobuf::Timestamp* stamp = new google::protobuf::Timestamp();
            std::string rfcString = thesePosts.at(i).at(0);
            rfcString.replace(10, 1, "T");
            rfcString.append("Z");
            google::protobuf::util::TimeUtil::FromString(rfcString, stamp);
            messageWrite.set_msg(thesePosts.at(i).at(2));
            messageWrite.set_username(thesePosts.at(i).at(1));
            messageWrite.set_allocated_timestamp(stamp);
            stream->Write(messageWrite, options);
          }
          if (userClient->following_file_size == 0) userClient->following_file_size += posts.size();
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
              log(INFO, "Writing to "+ follower->username +"'s stream\n");
              follower->stream->Write(messageWrite, options);
              log(INFO, "Succesfully wrote to "+ follower->username +"'s stream\n");
            }

            // Append to following files
            std::string followerFollowingPath = directory.string()+follower->username+"_following.txt";
            std::string followerFollowingSemName = semPrefix+follower->username+"_following.txt";
            sem_t *followerFollowingSem = sem_open(followerFollowingSemName.c_str(), O_CREAT, 0777, 1);
            if (followerFollowingSem == SEM_FAILED) {
              log(ERROR, "Couldn't open semaphore when writing");
              Status no(grpc::StatusCode::CANCELLED, "A follower's following semaphore was not able to be opened");
              return no;
            }
            sem_wait(followerFollowingSem);
            std::fstream followerFollowing(followerFollowingPath, std::ios::app);
            if (!followerFollowing.is_open()) {
              Status no(grpc::StatusCode::CANCELLED, "A follower's following file was not able to be opened");
              sem_post(followerFollowingSem);
              sem_close(followerFollowingSem);
              return no;
            }
            followerFollowing << formattedBuf.str();
            followerFollowing.close();
            std::cout << username+" is writing to "<<follower->username<< std::endl;
            follower->following_file_size += 1;// Increment the size of their following file (1 write = 4 lines)
            sem_post(followerFollowingSem);
            sem_close(followerFollowingSem);
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
  bool amMaster = confirmation.status();
  if (amMaster) {
    myInfo.set_ismaster(amMaster);
    log(INFO, std::to_string(myInfo.serverid())+" is the master of cluster "+std::to_string(myInfo.clusterid())+"\n");
    // std::cout << std::to_string(myInfo.serverid())+" is the master of cluster "+std::to_string(myInfo.clusterid())+"\n";
  }

  log(INFO, "Registration heartbeat from "+cid+"/"+std::to_string(info.serverid()));

  std::this_thread::sleep_for(std::chrono::seconds(5));

  // Send heartbeat every 5 seconds
  while (1) {
    bool prevMasterState = myInfo.ismaster();
    ClientContext context;
    context.AddMetadata("clusterid",cid);
    Status heartbeatStatus = coordStub->Heartbeat(&context, info, &confirmation);
    log(INFO, "Normal heartbeat from "+cid+"/"+std::to_string(info.serverid()));
    bool amMaster = confirmation.status();
    if (amMaster) {
      myInfo.set_ismaster(amMaster);
      log(INFO, std::to_string(myInfo.serverid())+" is the master of cluster "+std::to_string(myInfo.clusterid())+"\n");
      // std::cout << std::to_string(myInfo.serverid())+" is the master of cluster "+std::to_string(myInfo.clusterid())+"\n"; 
      if (amMaster != prevMasterState) { // If you've become master from being slave, clear your clientdb so the same clients can re-connect
        client_db.clear();
      }
    }

    std::this_thread::sleep_for(std::chrono::seconds(5));
 }
}

// File updating functions

// This function updates server's clientdb with newly discovered clients on different clusters
void updateAllUsers() {
  // Read in clients in the file
  std::string allUsersPath = "./cluster"+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"/all_users.txt";
  std::string allUsersSemName = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_all_users.txt";
  std::ifstream allUsers;
  sem_t *allUsersSem = sem_open(allUsersSemName.c_str(), O_CREAT, 0777, 1);
  if (allUsersSem == SEM_FAILED) {
    perror("sem_open failed");
    std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
    log(ERROR, "Couldn't open semaphore when writing to all users file");
    return;
  }
  else {
    std::cout << "sem: " << " -> " << allUsersSemName << std::endl;
  }
  sem_wait(allUsersSem);
  allUsers.open(allUsersPath, std::ios::app);
  if (!allUsers.is_open()) {
    log(ERROR, "Couldn't open all users file to read in new users");
    return;
  }
  std::string user;
  while (getline(allUsers, user)) {
    if (!user.empty()) {
      for (int i = 0; i < client_db.size(); i++) { // Check if the user already exists in db
        if (client_db.at(i)->username == user) {
          break;
        }
        // If the requestor's username is not in the db, add them to the db
        if (i == client_db.size() - 1) { // Reached last index and still no match
          Client *newClient = new Client();
          newClient->username = user;
          client_db.push_back(newClient);
          std::cout << "added new user "+user << std::endl;
        }
      }
    }
  }
  allUsers.close();
  sem_post(allUsersSem);
  sem_close(allUsersSem);
}

// This function updates server's clients with their new followers from other clusters
void updateFollowingRelationships() {
  for (const auto& client : client_db) {// Iterate through the client list
    // For each client, check thier followers file. 
    std::string followersPath = "./cluster"+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"/"+client->username+"_followers.txt";
    std::string followersSemName = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_"+client->username+"_followers.txt";
    std::fstream followersFile;
    sem_t *followersSem = sem_open(followersSemName.c_str(), O_CREAT, 0777, 1);
    if (followersSem == SEM_FAILED) {
      perror("sem open failed");
      std::cout << "sem: ->" << followersSemName << std::endl;
      log(ERROR, "Failed to open semaphore");
      continue;
    }
    sem_wait(followersSem);
    followersFile.open(followersPath, std::ios::in);
    if (!followersFile.is_open()) {
      perror("file open failed");
      log(ERROR, "Failed to open file "+followersPath);
      sem_post(followersSem);
      sem_close(followersSem);
      continue;
    }
    std::string user;
    while (getline(followersFile, user)) {// For each user in this file, check to see if they are in client's followers vector in-memory
      if (!user.empty()) {
        bool found = false;
        for (auto* follower : client->client_followers) {
          if (follower->username == user) { // This username is already in the client_followers vector
            found = true;
            break;
          }
        }
        if (!found) { // If the username is nowhere in there, update the vector
          Client *newFollower = new Client();
          newFollower->username = user;
          client->client_followers.push_back(newFollower);
        } 
      }
    }
    followersFile.close();
    sem_post(followersSem);
    sem_close(followersSem);
  }
}

// This function updates client's timelines if they have new stuff in their following file
void updateTimelineStreams() {
  for (auto *client : client_db) {
    if (client->stream == 0) { // Check if this client is in timeline mode (They have a write stream)
      continue; // Move on, this client not in timeline
    }
    ServerReaderWriter<Message, Message>* clientStream = client->stream;
    std::string followingPath = "./cluster"+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"/"+client->username+"_following.txt";
    std::string followingSemName = "/"+std::to_string(myInfo.clusterid())+"_"+std::to_string(myInfo.serverid())+"_"+client->username+"_following.txt";
    std::fstream followingFile;
    sem_t *followingSem = sem_open(followingSemName.c_str(), O_CREAT, 0777, 1);
    if (followingSem == SEM_FAILED) {
      perror("sem open failed");
      std::cout << "sem: ->" << followingSemName << std::endl;
      log(ERROR, "Failed to open semaphore");
      continue;
    }
    sem_wait(followingSem);
    followingFile.open(followingPath, std::ios::in);
    if (!followingFile.is_open()) {
      perror("file open failed");
      log(ERROR, "Failed to open file "+followingPath);
      sem_post(followingSem);
      sem_close(followingSem);
      continue;
    }
    int counter = 0;
    int currentLines = 0;
    std::string line;
    std::vector<std::vector<std::string>> posts;
    std::vector<std::string> post;
    while (std::getline(followingFile, line)) {
      // Skip the empty lines to maintain counting logic
      if (line.empty())continue; 
      // Add content lines to the total number of lines for the file (every 3 lines is a post)
      else currentLines++;
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
    followingFile.close();
    sem_post(followingSem);
    sem_close(followingSem);
    // Write in the number of posts that the client is missing
    size_t numRead = (currentLines - client->following_file_size * 3) / 3; // (Difference in lines / 3) gives you the number of missing posts
    std::vector<std::vector<std::string>> thesePosts(posts.end() - numRead, posts.end());
    std::cout << "adding "+std::to_string(numRead)+" missing posts" << std::endl;
    for (size_t i = 0; i < numRead; i++) {
      grpc::WriteOptions options;
      Message messageWrite;
      google::protobuf::Timestamp* stamp = new google::protobuf::Timestamp();
      std::string rfcString = thesePosts.at(i).at(0);
      rfcString.replace(10, 1, "T");
      rfcString.append("Z");
      google::protobuf::util::TimeUtil::FromString(rfcString, stamp);
      messageWrite.set_msg(thesePosts.at(i).at(2));
      messageWrite.set_username(thesePosts.at(i).at(1));
      messageWrite.set_allocated_timestamp(stamp);
      clientStream->Write(messageWrite, options);
    }
    client->following_file_size = currentLines / 3;
  }
}

void checkFiles() {
  std::cout << "starting file checker" << std::endl;
  while (1) {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    updateAllUsers();
    updateFollowingRelationships();
    updateTimelineStreams();
  }
}

void RunServer(std::string port_no, std::string clusterId, std::string serverId,
  std::string coordIp, std::string coordPort) {
  std::thread hb; // Heartbeat thread
  std::thread cf; // Check updating of files
  if (coordIp != "-1") {
    // Make stub to communicate with coordinator
    std::string coordinatorConnection = coordIp + ":" + coordPort;
    auto coordinatorChannel = grpc::CreateChannel(coordinatorConnection, grpc::InsecureChannelCredentials());
    coordStub_ = CoordService::NewStub(coordinatorChannel);

    // Build ServerInfo object with this server's info
    myInfo.set_hostname(coordIp);
    myInfo.set_port(port_no);
    myInfo.set_clusterid(std::stoi(clusterId));
    myInfo.set_serverid(std::stoi(serverId));

    // Start heartbeat thread
    hb = std::thread(sendHeartbeat, std::ref(myInfo), coordStub_, std::ref(clusterId));
  }
  cf = std::thread(checkFiles);

  std::string server_address = "127.0.0.1:"+port_no;
  SNSServiceImpl service;

  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);

  // Create this server's directories (silently succeeds if they already exist)
  fs::path directory = "./cluster" + std::to_string(myInfo.clusterid()) + "/" + std::to_string(myInfo.serverid())+"/";

  try {
    fs::create_directories(directory);  
    log(INFO, "Created/Verified "+std::to_string(myInfo.clusterid())+"/"+std::to_string(myInfo.serverid())+"'s directory\n");
  } catch (const fs::filesystem_error& e) {
      std::cerr << "Error: " << e.what() << std::endl;
  }

  server->Wait();

  if (hb.joinable()) hb.join();
}

int main(int argc, char** argv) {

  std::string port = "3010";
  std::string coordIp = "-1";
  std::string coordPort = "9000";
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