#include "tcp_server.hpp" 
#include <asm-generic/socket.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h> 
#include <vector>
#include "../protocol/frame.hpp" 

#include "../handler/request_dispatcher.hpp"
//the backlog is the queue size  for connection waiting to be accepted
namespace  {
   constexpr int KListenbacklog = 128;   
   //sent every byte in a response handling partial sent 
   bool sent_all(int client_fd, const std::vector<std::byte>&bytes){
      //track how maany bytes has already been sent 
      std::size_t sent_total =0;  
      while (sent_total < bytes.size()) {
        const auto bytes_sent = ::send(client_fd, bytes.data() + sent_total, bytes.size()+sent_total,MSG_NOSIGNAL); 
        if(bytes_sent > 0){
          sent_total += static_cast<std::size_t>(bytes_sent); 
          continue;
        } 
        if(bytes_sent == -1 && errno == EINTR){
          continue;
        } 
        return false;
      } 
      return true;
   }
}   
//constructor  
TcpServer::TcpServer(std::uint16_t port):port_(port){};     
//start the tcp server and keep it runing  
void TcpServer::start(){
    //create a tcp socket and return a file descriptor  or -1 if creation fialed 
    const int server_fd = ::socket(AF_INET,SOCK_STREAM, 0);   
    //a return of -1 indicates that socket creation failed 
    if(server_fd == -1){
        std::perror("socket"); 
        //terminate the prog 
        std::exit(EXIT_FAILURE);
    }  
    bool protcol_error = false;
    //enable reuse of the local address so restarting server is easier  
    int reuse_address = 1; 
    if(::setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) == -1){
         ::close(server_fd); 
         std::perror("setsockopt"); 
         std::exit(EXIT_FAILURE);
    }  
    //create an zero  intailize an ipv4 socket adddress structure 
    sockaddr_in address {}; 
    //specify as ipv4   
    address.sin_family = AF_INET; 
    //listen only to loopback interface 
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);   
    //set the port number 
    address.sin_port = htons(port_);  
    //bind the socket to the choosen ip  adress and port 
    if(::bind(server_fd, reinterpret_cast<sockaddr*>(&address),sizeof(address)) == -1){
        ::close(server_fd); 
        std::perror("bind"); 
        std::exit(EXIT_FAILURE);
    } 
    //mark the socket as listening 
    if(::listen(server_fd, KListenbacklog) == -1){
        ::close(server_fd); 
        std::perror("listen"); 
        std::exit(EXIT_FAILURE);
    } 
    std::cout << "Server runing. \n"; 
    //keep acceptoing clients untill an unrecoverable accepts errors occurs 
    while (true) {
      //wait for client to connect 
       const int client_fd = ::accept(server_fd, nullptr, nullptr);   
       //value -1 means accept failed 
       if(client_fd == -1){
         //EINTR means a signal Interrupt the system call retry 
         if(errno == EINTR){
            continue;
         }  
         //any other error as accept error 
         std::perror("accept"); 
         break;
       }     
       const RequestDispacther dispatcher;
       std::cout << "Client Connected \n"; 
       //allocate a tempoaray buffer to hold bytes  recived form client   
       std::vector<std::byte> recive_buffer; 

       std::byte buffer[4096]; 
       //keep reading from client   
       while (true) {
          //recive up the buffer form the client 
          const auto bytes_recived = ::recv(client_fd, buffer, sizeof(buffer),0);  
          //a positive return value means the many bytes  were recived 
          if(bytes_recived > 0){
             std::cout << bytes_recived  << "\n";       
             const auto count = static_cast<std::size_t>(bytes_recived);   
             recive_buffer.insert(
                recive_buffer.end(), 
                buffer, 
                buffer +count
             );   
             while (!recive_buffer.empty()) { 
                std::size_t consumed =0; 
                try {
                   const auto frame = FrameParser::parse_one(recive_buffer, consumed); 
                   if(!frame.has_value()){
                     break;
                   } 
                  
                   //Dispatch the request to its correponding api handler
                    const auto response = dispatcher.dispatch(frame->payload);
                   if(!sent_all(client_fd,response)){
                      std::cerr << "Failed to send response \n"; 
                      protcol_error = true; 
                      break;
                   }
                   recive_buffer.erase(recive_buffer.begin(), recive_buffer.begin() + static_cast<std::vector<std::byte>::difference_type>(consumed));    

                   
                } catch (const std::exception & error) {
                   std::cout <<error.what();  
                   protcol_error = true;
                   break;
                }
               
             } 
             if(protcol_error == true){
               break;
             }
              continue;

          }       
          //value  0  means disconnect 
          if(bytes_recived == 0){
            std::cout << "Cleint disconnect"; 
            break;
          }    
          //EINTR mens interrrupt continue 
           if(errno == EINTR){
            continue;
           }  
           //for other error  
           std::perror("recv"); 
           break;

       } 
       //close the client socket  
       ::close(client_fd); 

    }  
    //close the server 
    ::close(server_fd);


}