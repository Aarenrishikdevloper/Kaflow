//represent the optional pagination cursor
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>
#include "../metadata/topic.hpp"
struct DescribeTopicPartionRequest{
  //name of topic reaueted by the client 
  std::vector<std::string> topics;  
  //min no of partions allowed in the response  
  std::int32_t reponse_partion_limits; 
  //optional pagination cursor 
  std::optional<DescribeTopicCursor> cursor; 
  //psrse the api  reuest body  
  static DescribeTopicPartionRequest parse(const std::vector<std::byte>&body);
};    
//handler 
std::vector<std::byte> handle_description_topic_partions(std::int32_t correlation_id, const DescribeTopicPartionRequest&request);
