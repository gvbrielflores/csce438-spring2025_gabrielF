#include <iostream>
#include <memory>
#include <thread>
#include <vector>
#include <sstream>
#include <string>
#include <unistd.h>
#include <csignal>
#include <grpc++/grpc++.h>
#include "client.h"

#include "sns.grpc.pb.h"
using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::SNSService;

void sig_ignore(int sig) {
  std::cout << "Signal caught " + sig;
}

Message MakeMessage(const std::string& username, const std::string& msg) {
    Message m;
    m.set_username(username);
    m.set_msg(msg);
    google::protobuf::Timestamp* timestamp = new google::protobuf::Timestamp();
    timestamp->set_seconds(time(NULL));
    timestamp->set_nanos(0);
    m.set_allocated_timestamp(timestamp);
    return m;
}


class Client : public IClient
{
public:
  Client(const std::string& hname,
	 const std::string& uname,
	 const std::string& p)
    :hostname(hname), username(uname), port(p) {}

  
protected:
  virtual int connectTo();
  virtual IReply processCommand(std::string& input);
  virtual void processTimeline();

private:
  std::string hostname;
  std::string username;
  std::string port;
  
  // You can have an instance of the client stub
  // as a member variable.
  std::unique_ptr<SNSService::Stub> stub_;
  
  IReply Login();
  IReply List();
  IReply Follow(const std::string &username);
  IReply UnFollow(const std::string &username);
  void   Timeline(const std::string &username);
};


///////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////
int Client::connectTo()
{
  // ------------------------------------------------------------
  // In this function, you are supposed to create a stub so that
  // you call service methods in the processCommand/porcessTimeline
  // functions. That is, the stub should be accessible when you want
  // to call any service methods in those functions.
  // Please refer to gRpc tutorial how to create a stub.
  // ------------------------------------------------------------
    
///////////////////////////////////////////////////////////
    // Create the stub associated with the server's hostname and port
    std::string connectionLocation = hostname + ":" + port;
    auto channel = grpc::CreateChannel(connectionLocation, grpc::InsecureChannelCredentials()); 
    this->stub_ = SNSService::NewStub(channel);

    // Attempt login
    IReply iRep = Login();

    // Check status and return
    if (iRep.comm_status != SUCCESS) {
      return -1;
    }
//////////////////////////////////////////////////////////
    else {
      return 1;
    }
}

IReply Client::processCommand(std::string& input)
{
  // ------------------------------------------------------------
  // GUIDE 1:
  // In this function, you are supposed to parse the given input
  // command and create your own message so that you call an 
  // appropriate service method. The input command will be one
  // of the followings:
  //
  // FOLLOW <username>
  // UNFOLLOW <username>
  // LIST
  // TIMELINE
  // ------------------------------------------------------------
  
  // ------------------------------------------------------------
  // GUIDE 2:
  // Then, you should create a variable of IReply structure
  // provided by the client.h and initialize it according to
  // the result. Finally you can finish this function by returning
  // the IReply.
  // ------------------------------------------------------------
  
  
  // ------------------------------------------------------------
  // HINT: How to set the IReply?
  // Suppose you have "FOLLOW" service method for FOLLOW command,
  // IReply can be set as follow:
  // 
  //     // some codes for creating/initializing parameters for
  //     // service method
  //     IReply ire;
  //     grpc::Status status = stub_->FOLLOW(&context, /* some parameters */);
  //     ire.grpc_status = status;
  //     if (status.ok()) {
  //         ire.comm_status = SUCCESS;
  //     } else {
  //         ire.comm_status = FAILURE_NOT_EXISTS;
  //     }
  //      
  //      return ire;
  // 
  // IMPORTANT: 
  // For the command "LIST", you should set both "all_users" and 
  // "following_users" member variable of IReply.
  // ------------------------------------------------------------

    IReply ire;

    // Parse input to exctract tokens
    std::stringstream inputStream(input);
    std::string token;
    std::vector<std::string> tokens;
    while (std::getline(inputStream, token, ' ')) {
      tokens.push_back(token);
    }

    // Assign tokens to command and argument respectively
    std::string command = tokens.at(0);
    std::string arg;
    if (tokens.size() > 1) {
      arg = tokens.at(1);
    }

    if (command == "FOLLOW") {
      ire = Follow(arg);
    }
    else if (command == "UNFOLLOW") {
      ire = UnFollow(arg);
    }
    else if (command == "LIST") {
      ire = List();
    }
    else {
      processTimeline();
    }

    return ire;
}


void Client::processTimeline()
{
    Timeline(username);
}

// List Command
IReply Client::List() {

    IReply ire;
    ListReply listReply;
    Request req;
    ClientContext context;
    Status status;

    // Build request with username, send List rpc thru stub and get return status
    req.set_username(this->username);
    status = this->stub_->List(&context, req, &listReply);
    ire.grpc_status = status;

    if (status.ok()) {
      // If List returned correctly, update ire's listreply variables
      for (int i = 0; i < listReply.all_users_size(); i++) {
        ire.all_users.push_back(listReply.all_users(i));
      }
      for (int i = 0; i < listReply.followers_size(); i++) {
        ire.followers.push_back(listReply.followers(i));
      }
      ire.comm_status = SUCCESS;
    }
    else {
      ire.comm_status = FAILURE_NOT_EXISTS;
    }

    return ire;
}

// Follow Command        
IReply Client::Follow(const std::string& username2) {

    IReply ire; 
    ClientContext context;
    Reply reply;
    Request req;
    Status status;

    req.set_username(this->username);
    req.add_arguments(username2);
    status = this->stub_->Follow(&context, req, &reply);
    ire.grpc_status = status;
    if (reply.msg() == "You already follow the target") {
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    }
    else if (reply.msg() == "Requestor does not exist") {
      ire.comm_status = FAILURE_NOT_EXISTS;
    }
    else if (reply.msg() == "Target does not exist") {
      ire.comm_status = FAILURE_NOT_EXISTS;
    }
    else {
      ire.comm_status = SUCCESS;
    }

    return ire;
}

// UNFollow Command  
IReply Client::UnFollow(const std::string& username2) {

    IReply ire;
    ClientContext context;
    Reply reply;
    Request req;
    Status status;

    req.set_username(this->username);
    req.add_arguments(username2);
    status = this->stub_->UnFollow(&context, req, &reply);
    ire.grpc_status = status;

    if (reply.msg() == "Requestor does not exist") {
      ire.comm_status = FAILURE_NOT_EXISTS;
    }
    else if (reply.msg() == "Target does not exist") {
      ire.comm_status = FAILURE_NOT_EXISTS;
    }
    else {
      ire.comm_status = SUCCESS;
    }

    return ire;
}

// Login Command  
IReply Client::Login() {

    IReply ire;
    ClientContext context;
    Request req;
    Reply reply;
    Status status;  
  
    // Build request with client username and send login request thru stub and get return status
    req.set_username(username);
    status = stub_->Login(&context, req, &reply);
    ire.grpc_status = status;

    // Set ire status based on returned message
    if (!status.ok()) {
      ire.comm_status = FAILURE_ALREADY_EXISTS;
    }
    else {
      ire.comm_status = SUCCESS;
    }

    return ire;
}

// Timeline Command
void Client::Timeline(const std::string& username) {

    // ------------------------------------------------------------
    // In this function, you are supposed to get into timeline mode.
    // You may need to call a service method to communicate with
    // the server. Use getPostMessage/displayPostMessage functions 
    // in client.cc file for both getting and displaying messages 
    // in timeline mode.
    // ------------------------------------------------------------

    // ------------------------------------------------------------
    // IMPORTANT NOTICE:
    //
    // Once a user enter to timeline mode , there is no way
    // to command mode. You don't have to worry about this situation,
    // and you can terminate the client program by pressing
    // CTRL-C (SIGINT)
    // ------------------------------------------------------------
  
    /***
    YOUR CODE HERE
    ***/

}



//////////////////////////////////////////////
// Main Function
/////////////////////////////////////////////
int main(int argc, char** argv) {

  std::string hostname = "127.0.0.1";
  std::string username = "default";
  std::string port = "3010";
    
  int opt = 0;
  while ((opt = getopt(argc, argv, "h:u:p:")) != -1){
    switch(opt) {
    case 'h':
      hostname = optarg;break;
    case 'u':
      username = optarg;break;
    case 'p':
      port = optarg;break;
    default:
      std::cout << "Invalid Command Line Argument\n";
    }
  }
      
  std::cout << "Logging Initialized. Client starting...";
  
  Client myc(hostname, username, port);
  
  myc.run();
  
  return 0;
}
