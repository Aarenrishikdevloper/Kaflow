//one manaul partion asisgment 
#include "protocol/request_header.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
struct CreateTopicAssigment{
    std::int32_t partion_index;  
    //broker ids assined to this partion 
    std::vector<std::int32_t> broker_ids;  
};    
//one topic configuration 
struct createTopicConfig{
    std::string name; 
    //kafka configuration values can be null  
    std::optional<std::string> value;
};
//one topic requested by the client 
struct createTopicEntry{
  std::string name; 
  std::int32_t num_partion; 
  std::int16_t replication_factor; 
  std::vector<CreateTopicAssigment> asisgment; 
  std::vector<createTopicConfig> configs;
};  
//parse createtopics v0 request 
struct createTopicRequest{
    std::vector<createTopicEntry> topics;  
    std::int32_t timeout_ms;  
    static createTopicRequest parse(const std::vector<std::byte>&body);
}; 
//handle a createTopic ApI V0 request 
std::vector<std::byte>handle_create_topics(const Requestheader& header, const std::vector<std::byte>&body);