#include "server/tcp_server.hpp"  
#include "test.hpp"
int main(){ 
     seed_test_topics();
 
    TcpServer server(9092); 
    server.start(); 
    return  0;
}