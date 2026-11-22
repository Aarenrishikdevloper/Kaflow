#include "../handler/api_registry.hpp" 
#include "../protocol/compact_writer.hpp" 
#include "../metadata/topic.hpp"
#include "protocol/byte_reader.hpp"
#include "protocol/byte_writer.hpp"
#include <cstdint>
#include <optional>
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
std::string read_string(ByteReader& reader);
std::int32_t read_array_length(ByteReader& reader);
void Write_string(ByteWriter&writer, const std::string&value); 
std::optional<std::string>read_nullable_string(ByteReader &reader); 
void write_string(ByteWriter&writer, const std::string& value); 
std::vector<std::byte> add_frame(const std::vector<std::byte>& payload);    
std::array<std::byte, 16> generate_topic_uuid(); 
