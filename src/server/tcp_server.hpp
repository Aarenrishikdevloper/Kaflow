#pragma  once  
#include <cstdint>   
class TcpServer{
    public: 
      explicit TcpServer(std::uint16_t port);  
      void start(); 
      private: 
      std::uint16_t port_;
};