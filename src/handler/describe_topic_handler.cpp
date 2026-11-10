#include "../metadata/topic.hpp"
#include "../metadata/topic_manager.hpp"
#include "../protocol/byte_writer.hpp"
#include "../protocol/compact_reader.hpp"
#include "../protocol/compact_writer.hpp"
#include "../protocol/response_header.cpp"
#include "../utils/utils.hpp"
#include "describre_topic_partion.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
namespace {
DescribeTopicpage
build_describe_page(const DescribeTopicPartionRequest &request) {
  DescribeTopicpage page;
  std::int32_t included_partions = 0;
  // kafka returrns topics sroted by name the cursor refres to that order
  std::vector<std::string> topic_names = request.topics;
  std::sort(topic_names.begin(), topic_names.end());
  std::size_t first_topic_index = 0;
  if (request.cursor.has_value()) {
    const auto it = std::find(topic_names.begin(), topic_names.end(),
                              request.cursor->Topic_name);
    if (it == topic_names.end()) {
      throw std::invalid_argument(
          "Curosr topic is not present in requested topics");
    }
    first_topic_index = static_cast<std::size_t>(it - topic_names.begin());
  }
  for (std::size_t topic_index = first_topic_index;
       topic_index < topic_names.size(); ++topic_index) {
    const std::string &topic_name = topic_names[topic_index];
    const auto topic_result = topic_manager().find_topic(topic_name);
    TopicPageEntry entry;
    if (!topic_result.has_value()) {
      entry.topic.name = topic_name;
      entry.unkown = true;
      page.topics.push_back(std::move(entry));
      continue;
    }
    entry.topic = *topic_result;
    std::sort(entry.topic.partitions.begin(), entry.topic.partitions.end(),
              [](const partionMetadata &l, const partionMetadata &r) {
                return l.partion_index < r.partion_index;
              });
    std::int32_t start_partion_index = 0;
    if (request.cursor.has_value() && topic_index == first_topic_index) {
      start_partion_index = request.cursor->partition_index;
    }    
    for(const partionMetadata &partion: entry.topic.partitions){
      if(partion.partion_index < start_partion_index) continue; 
      if(included_partions >= request.reponse_partion_limits){
        page.next_cursor = DescribeTopicCursor{topic_name,partion.partion_index}; 
        if(!entry.partions.empty()){
           page.topics.push_back(std::move(entry));
        } 
        return page;
      }  
      entry.partions.push_back(partion); 
      ++included_partions;
    }   
    page.topics.push_back(std::move(entry));
    
    
  }   
  return page;

}
} // namespace
DescribeTopicPartionRequest
DescribeTopicPartionRequest::parse(const std::vector<std::byte> &body) {
  // create a compact reader
  CompactReader reader(body);
  // read the topic array length
  const auto topic_count = reader.read_compact_array_lenght();
  if (!topic_count.has_value()) {
    throw std::runtime_error("Describepartions topics array cant be null");
  }
  constexpr std::uint32_t kMaxTopics = 10000;
  if (*topic_count > kMaxTopics) {
    throw std::runtime_error("too many topica  in describeparion request");
  }
  // create a topic list
  std::vector<std::string> topics;
  // reseve memory for all topic
  topics.reserve(*topic_count);
  // read every topic count
  for (std::uint32_t i = 0; i < *topic_count; ++i) {
    // read the topic name
    const std::string topic_name = reader.read_compact_string();
    // store the topic name
    topics.push_back(topic_name);
    // each TopicRequest ends with tagged field
    reader.skipped_tag_filed();
  }
  // read response partion limit
  const std::int32_t response_partion_limit = reader.read_int32();
  if (response_partion_limit <= 0) {
    throw std::runtime_error("Invalid partition limit");
  }
  // read the optional curosr
  // anullable kafka structure begins with boolean
  const std::uint8_t cursor_marker = reader.read_uint8();
  std::optional<DescribeTopicCursor> cursor;
  if (cursor_marker == 0x01) {
    // read the curosr topic name
    const std::string cursor_topic_name = reader.read_compact_string();
    // read the startimg position
    const std::int32_t cursor_partion_index = reader.read_int32();
    if (cursor_partion_index < 0) {
      throw std::runtime_error("Invaid curosr partion index");
    }
    // read the cursor tagged
    reader.skipped_tag_filed();
    // store the cursor
    cursor = DescribeTopicCursor(cursor_topic_name, cursor_partion_index);
  }else if (cursor_marker != 0xFF) {
    throw std::runtime_error("Invalid curosr partion index");
  }
  // read top-level tagged field
  reader.skipped_tag_filed();
  // no unexpected byte should remain
  if (reader.position() != body.size()) {
    throw std::runtime_error(
        "Unexpected trailing bytes in DescribePartions Request");
  }
  return DescribeTopicPartionRequest{std::move(topics), response_partion_limit,
                                     std::move(cursor)};
}   

std::vector<std::byte>
handle_description_topic_partions(std::int32_t correlation_id,
                                  const DescribeTopicPartionRequest &request) {
  // select the topics and partions for this page
  const DescribeTopicpage page = build_describe_page(request);
  // create a writer to the fixed part of the response header
  ByteWriter fixed_header_writer;
  CompacrWriter header_tagged_writer;
  // contruct the flexible response
  ResponseHeader response_header{
      correlation_id,

  };
  /// write the colleration id and header tagged field
  response_header.write_flexible(fixed_header_writer, header_tagged_writer);
  // combine the fixed header and tagged header
  std::vector<std::byte> reponse_payload = fixed_header_writer.data();
  // append the header tagged field
  header_tagged_writer.data();
  // append the hader tagged field
  reponse_payload.insert(reponse_payload.end(),
                         header_tagged_writer.data().begin(),
                         header_tagged_writer.data().end());
  // create a writer for response
  CompacrWriter body_writer;
  // write thottle time
  body_writer.write_int32(0);
  // no of topic entries in this page
  body_writer.wriet_compoact_arraylenght(
      static_cast<std::uint32_t>(page.topics.size()));

  // serialize each topic
  for (const TopicPageEntry &entry : page.topics) {
    if (entry.unkown) {
      write_unkown_topic(body_writer, entry.topic.name);
    } else {
      write_topic(body_writer, entry);
    }
  }
  // write the nullable next cursor
  write_next_curosr(page.next_cursor, body_writer);
  // top level tagged fields
  body_writer.write_emty_tagged_field();
  // append the  body to the response payload
  const auto &data = body_writer.data();
  reponse_payload.insert(reponse_payload.end(), data.begin(), data.end());
  // validate the kafka frame
  if (reponse_payload.size() >
      static_cast<size_t>(std::numeric_limits<std::int32_t>::max())) {
    throw std::runtime_error("kafka response is to large");
  }
  ByteWriter frame_writer;
  frame_writer.write_int32(static_cast<std::int32_t>(reponse_payload.size()));
  frame_writer.Write_bytes(reponse_payload);
  return frame_writer.data();
}