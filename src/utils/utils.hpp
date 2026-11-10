#include "../handler/api_registry.hpp" 
#include "../protocol/compact_writer.hpp" 
#include "../metadata/topic.hpp"
#include <cstdint>
#include <string>
#include <vector>
//find the api by its key 
const APIversionInfo*find_api(std::int16_t api_key);    
void write_broker_array(CompacrWriter &writer, const std::vector<std::int32_t>&brokers);
void write_nullable_broker_array(CompacrWriter&writer, const std::vector<int32_t>&broker); 
void write_partion(CompacrWriter& writer, const partionMetadata& partion); 
void write_topic(CompacrWriter &writer, const TopicPageEntry&entry); 
void write_unkown_topic(CompacrWriter & writer, const std::string& topic_name); 
void write_next_curosr(const std::optional<DescribeTopicCursor>&curosor, CompacrWriter&writer);
