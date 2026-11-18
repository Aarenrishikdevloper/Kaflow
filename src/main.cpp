#include "server/tcp_server.hpp"
#include "storage/storage.hpp"
#include <exception>
#include <iostream>

int main() {
  try { 
    StorageManager storage("data");  
    storage.intialize();
    TcpServer server(9092);
    server.start();
   
  } catch (const std::exception&e) {  
      std::cerr <<"Broker startup Failed " << e.what()<< '\n'; 
      return 1;
  } 
   return 0;
}