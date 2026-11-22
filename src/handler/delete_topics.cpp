#include "delete_topics.hpp"
#include "metadata/topic.hpp"
#include "metadata/topic_manager.hpp"
#include "protocol/byte_reader.hpp"
#include "protocol/byte_writer.hpp"
#include "storage/metadata_store.hpp"
#include "storage/partion_directory_manager.hpp"
#include "utils/utils.hpp"
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
namespace {
constexpr std::int16_t NONE = 0;
constexpr std::int16_t UNKNOWN_TOPIC_OR_PARTION = 3;
constexpr std::int16_t INVALID_TOPIC_EXCEPTION = 17;
constexpr std::int16_t MAX_TOPIC_COUNT = 1000000;

static bool is_valid_topic_name(const std::string &name) {
  if (name.empty()) {
    return false;
  }
  // kafka topic names have a max lenght of 249
  if (name.size() > 249) {
    return false;
  }
  // these two names are invalid
  if (name == "." || name == "..") {
    return false;
  }
  // check every charater
  for (const unsigned char chracter : name) {
    const bool valid = (chracter >= 'a' && chracter <= 'z') ||
                       (chracter >= 'A' && chracter <= 'Z') ||
                       (chracter >= '0' && chracter <= '9') ||
                       chracter == '.' || chracter == '_' || chracter == '-';
    if (!valid) {
      return false;
    }
  }
  return true;
}
} // namespace
DeleeteTopicRequest
DeleeteTopicRequest::parse(const std::vector<std::byte> &body) {
  // bytereader reads the request body
  ByteReader reader(body);
  DeleeteTopicRequest request;
  // the first field is the no of topic name
  const std::int32_t topic_count = reader.read_int32();
  // a negaive array length is invalid
  if (topic_count < 0) {
    throw std::runtime_error("Invalid topics array length");
  }
  // protect the broker from an ecessive large request
  if (topic_count > MAX_TOPIC_COUNT) {
    throw std::runtime_error("To many topics in DeleteTopics request");
  }
  // reserve memory for thr topic
  request.topics.reserve(static_cast<std::size_t>(topic_count));
  // read each topic name from the request
  for (std::int32_t i = 0; i < topic_count; ++i) {
    DeleteTopicEntry entry;
    entry.name = read_string(reader);
    // save thr topic entry
    request.topics.push_back(std::move(entry));
  }
  // the final field is timeout_ms
  request.timeout_ms = reader.read_int32();
  // the request must not contain unepected trailing bytes
  if (!reader.empty()) {
    throw std::runtime_error(
        "Unexpected traiiling bytes in dletetopics request");
  }
  return request;
}
std::vector<std::byte>
handle_delete_topics(const Requestheader &header,
                     const std::vector<std::byte> &body) {
  // parse the incoming request body
  const DeleeteTopicRequest request = DeleeteTopicRequest::parse(body);
  ByteWriter response;
  // kafka reponse header vo contains only the correlation id
  response.write_int32(header.correlation_id);
  // response body
  response.write_int32(static_cast<std::int32_t>(request.topics.size()));
  // process every request topic separetly
  for (const DeleteTopicEntry &topic : request.topics) {
    // every response every starts with the topic name
    Write_string(response, topic.name);
    // determine this result for this topic
    std::int16_t error_code = NONE;
    static std::mutex persist_mutext;
    std::lock_guard<std::mutex> lock(persist_mutext);
    // invalid names  must not  be passed to topic manager
    if (!is_valid_topic_name(topic.name)) {
      error_code = INVALID_TOPIC_EXCEPTION;
    } else {
      // copy kept for rollback
      const std::optional<TopicMetadata> rollback_topic =
          topic_manager().find_topic(topic.name);
      if (!rollback_topic.has_value()) {
        error_code = UNKNOWN_TOPIC_OR_PARTION;
      } else if (!topic_manager().remove_topic(topic.name)) {
        error_code = UNKNOWN_TOPIC_OR_PARTION;
      } else {
        try {
          metadata_store().save_topics(topic_manager().list_topics());
          error_code = NONE;
        } catch (const std::exception &) {
          topic_manager().try_and_topic(*rollback_topic);
          error_code = -1;
        }
      }
      if (error_code == NONE) {
        try {
          partion_directory_manager().delete_topic(*rollback_topic);
        } catch (const std::exception &e) {
          // Topic is already gone from metadata. Leftover directories
          // are harmless garbage, so log and still report success.
          std::fprintf(stderr, "failed to delete storage for %s: %s\n",
                       rollback_topic->name.c_str(), e.what());
        }
      }
    }
    // write the result for this topic
    response.write_int16(error_code);
  }
  return add_frame(response.data());
}