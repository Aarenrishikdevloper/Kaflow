#pragma once 
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector> 
struct DescribeTopicCursor{
    std::string Topic_name;  
    std::int32_t partition_index;
}; 
struct partionMetadata{
    //partion number starting at zero 
    std::int32_t partion_index;  
    //broker id that leds this partion 
    std::uint32_t leader_id; 
    //leader epoch 
    std::int32_t leader_epoch; 
    //all brokers that replicate this position 
    std::vector<std::int32_t> replica_node; 
    //broker currently in sync  
    std::vector<std::int32_t> isr_nodes;  
    //elligibe leader replicas 
    std::vector<std::int32_t> elligibe_leader_replicas;  
    //brokers that are offline 
    std::vector<std::int32_t> offline_replicas;

}; 
//represent the kafka topic 
struct TopicMetadata{
    //topic name 
    std::string name; 
    //kafka  topic id is a 16 byte uuid 
    std::array<std::byte, 16> topic_id{}; 
    //ehether this is a internal kafka topic 
    bool is_internal = false; 
    //all partion belonging to this topic 
    std::vector<partionMetadata> partitions;     
    //topic autorization bitfield  
    std::int32_t authorize_operations = std::numeric_limits<std::int32_t>::min();


};    
struct TopicPageEntry{
    TopicMetadata topic; 
    std::vector<partionMetadata>partions; 
    bool unkown =false;
};
//everything needed to seialize one page  
struct DescribeTopicpage{
    std::vector<TopicPageEntry> topics;  
    std::optional<DescribeTopicCursor>next_cursor;

   };