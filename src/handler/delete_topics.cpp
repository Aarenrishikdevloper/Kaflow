#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string> 
#include <utility>
#include <vector>
#include "delete_topics.hpp"
#include "metadata/topic_manager.hpp"
#include "protocol/byte_reader.hpp" 
#include "protocol/byte_writer.hpp"
#include "utils/utils.hpp"
namespace  { 
    constexpr std::int16_t NONE =0;  
    constexpr std::int16_t  UNKNOWN_TOPIC_OR_PARTION = 3; 
    constexpr std::int16_t INVALID_TOPIC_EXCEPTION = 17; 
    constexpr std::int16_t MAX_TOPIC_COUNT=1000000;

  static bool is_valid_topic_name(const std::string&name){
    if(name.empty()){
        return false;
    } 
      //kafka topic names have a max lenght of 249 
      if(name.size() > 249){
         return false;
      }  
      //these two names are invalid 
      if(name == "." || name==".."){
         return false ;
      } 
      //check every charater 
      for(const unsigned char chracter : name){
         const bool valid = (chracter >= 'a' && chracter <= 'z') || (chracter >= 'A' && chracter <= 'Z') ||(chracter >= '0' && chracter <= '9') || chracter =='.' ||chracter =='_'||chracter=='-'; 
         if(!valid){
            return false;
         } 
      } 
      return true;
  }
} 
DeleeteTopicRequest DeleeteTopicRequest::parse(const std::vector<std::byte> &body){
    //bytereader reads the request body  
    ByteReader reader(body); 
    DeleeteTopicRequest request; 
    //the first field is the no of topic name 
    const std::int32_t topic_count = reader.read_int32(); 
    //a negaive array length is invalid  
    if(topic_count < 0){
        throw std::runtime_error("Invalid topics array length");
    } 
    //protect the broker from an ecessive large request 
    if(topic_count > MAX_TOPIC_COUNT){
        throw std::runtime_error("To many topics in DeleteTopics request");
    } 
    //reserve memory for thr topic 
    request.topics.reserve(static_cast<std::size_t>(topic_count)); 
    //read each topic name from the request 
    for (std::int32_t i=0; i <topic_count; ++i) {
      DeleteTopicEntry entry; 
      entry.name = read_string(reader); 
      //save thr topic entry  
      request.topics.push_back(std::move(entry));
    } 
    //the final field is timeout_ms  
    request.timeout_ms =reader.read_int32(); 
    //the request must not contain unepected trailing bytes 
    if(!reader.empty()){
        throw std::runtime_error("Unexpected traiiling bytes in dletetopics request");
    } 
    return request;

}   
std::vector<std::byte>handle_delete_topics(const Requestheader & header, const std::vector<std::byte>&body){
    //parse the incoming request body 
    const DeleeteTopicRequest request = DeleeteTopicRequest::parse(body); 
    ByteWriter response; 
    //kafka reponse header vo contains only the correlation id 
    response.write_int32(header.correlation_id); 
    //response body  
    response.write_int32(static_cast<std::int32_t>(request.topics.size())); 
    //process every request topic separetly 
    for (const DeleteTopicEntry&topic : request.topics) {
      //every response every starts with the topic name 
      Write_string(response, topic.name); 
      //determine this result for this topic 
      std::int16_t error_code = NONE; 
      //invalid names  must not  be passed to topic manager 
      if(!is_valid_topic_name(topic.name)){
        error_code = INVALID_TOPIC_EXCEPTION;
      }else{
        //remove one in memory topic 
        const bool removed =topic_manager().remove_topic(topic.name); 
        //if erse founs no topic error code 3 
        if(!removed){
            error_code = UNKNOWN_TOPIC_OR_PARTION;
        }
      } 
      //write the result for this topic 
      response.write_int16(error_code); 
      


    } 
    return add_frame(response.data());
}