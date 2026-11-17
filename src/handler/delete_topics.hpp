//represent one topic name recived in a dleteTopics request 
#include "protocol/request_header.hpp"
#include <cstdint>
#include <string>
#include <vector>
struct DeleteTopicEntry{
    std::string name;
}; 
//represent the complete DeleteTopics V0 request 
struct DeleeteTopicRequest{
   //all topic name requeted for deletion 
   std::vector<DeleteTopicEntry>topics; 
   //max timeout 
   std::int32_t timeout_ms =0; 
   //parse request 
   static DeleeteTopicRequest parse(const std::vector<std::byte>&body);   
};  
std::vector<std::byte>handle_delete_topics(const Requestheader & header, const std::vector<std::byte>&body);