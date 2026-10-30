#include "tcp_server.hpp" 
#include <asm-generic/socket.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <mutex>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h> 
#include <vector>
#include "../protocol/frame.hpp" 

#include "../handler/request_dispatcher.hpp"
//the backlog is the queue size  for connection waiting to be accepted
namespace  {
   constexpr int KListenbacklog = 128;   
   constexpr std::size_t kreciveBufferSize = 4096;   
   //protect cosole output shared by mutiple client coutputs 

   std::mutex log_mutext;   
   //pritn a message  
   void log_message(const std::string & message){
      //lock the mutex 
      const std::lock_guard<std::mutex>lock(log_mutext); 
      //log the message 
      std::cout << message << '\n';
   }
   
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
   void handle_clinet(int client_fd){
      //each client has its own request dispatcher 
      const RequestDispacther dispathcer; 
      //each client has its own recive buffer 
      std::vector<std::byte>recive_buffer; 
      //temporary storage for incoming socket bytes 
      std::byte buffer[kreciveBufferSize]; 
      //track protocol error for this client only 
      bool protocol_error = false; 
      //announc the connection 
      log_message("Client Connected"); 
      //continue processing until theis client disocnnect or false 
      while (true) {
         //recieve bytes from the particular client 
         const auto bytes_recived = ::recv(client_fd, buffer, sizeof(buffer),0); 
         //a positive result means bytes were recived 
         if(bytes_recived > 0){
            //convert the byte count to the vector size type 
            const auto count = static_cast<std::size_t>(bytes_recived); 
            //append recive bytes to the clints buffer 
            recive_buffer.insert(recive_buffer.end(), buffer, buffer + count); 
            while (!recive_buffer.empty()) {
               //the parser reports many bytes from the frame 
               std::size_t consumed =0; 
               try {
                    //attempt  to parse one complete kafak frame 
                    const auto frame = FrameParser::parse_one(recive_buffer, consumed); 
                    if(!frame.has_value()){
                      break;
                    } 
                    //dispatch the request to its api 
                    const auto response = dispathcer.dispatch(frame->payload);  
                    if(!sent_all( client_fd, response)){
                         protocol_error = true; 
                         break;
                    }    
                    //convert the consume into iterator type  
                    using DiffrenceType = std::vector<std::byte>::difference_type;     
                    //remove the process frme from the recived buffer 
                    recive_buffer.erase(recive_buffer.begin(), recive_buffer.begin() + static_cast<DiffrenceType>(consumed)); 

                     
               } catch (const std::exception error) {
                 log_message(std::string("Protocol error ") + error.what());  
                 protocol_error = true; 
                 break;
               }
            }  
            if(protocol_error){
               break;
            } 
            continue;
         } 
         if(bytes_recived == 0){
            log_message("Client disconnected"); 
            break;
         } 
         if(errno == EINTR){
            continue;
         } 
         log_message("Cleint socke error"); 
         break;
      } 
      ::close(client_fd); 
      log_message("Cleint Conenction Close");
   } 
   void start_client(int client_fd, std::vector<std::thread>&client_thread){
      //create a new thread that own this client connection 
      client_thread.emplace_back([client_fd](){
         //process the request until client disconnects 
         handle_clinet(client_fd);
      });
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
    log_message("Kaflow Server runing"); 
    //store all client thread so they can be joinable 
    std::vector<std::thread> client_threads;
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
       try {
           start_client(client_fd,client_threads);     

       } catch (const std::exception & e) {
           log_message(std::string("Could not satrt client thread") + e.what()); 
           //close the client connection 
           ::close(client_fd);
       }
       
    }
    //close the server 
    ::close(server_fd); 
    //wait for every client thread to finish 
    for(std::thread &client_thread:client_threads){
      //join threads that are joinble 
      if(client_thread.joinable()){
         client_thread.join();
      }
    } 
    //repoty server has stoped 
    log_message("Kaflow server  stopped");



}