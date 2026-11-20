#include "metadata/topic.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector> 
#include "create_topics.hpp" 
#include "metadata/topic_manager.hpp"
#include "protocol/byte_reader.hpp" 
#include "protocol/byte_writer.hpp"
#include "utils/utils.hpp" 
#include "storage/metadata_store.hpp"
namespace  {
   constexpr std::int16_t kNone = 0; 
   constexpr std::int16_t kInvalidTopic = 17; 
   constexpr std::int16_t KTopicAlreadyExist = 36; 
   constexpr std::int16_t KinvalidPartions = 37; 
   constexpr std::int16_t kreplicationFactor = 38; 
   constexpr std::int16_t kinvalidreplicaAssigment = 39; 
   constexpr std::int16_t kInvalidConfig = 40;  
   constexpr std::int32_t kBrokerId = 1; 
   constexpr std::int32_t kMaxArrayelemets = 1000000;  
   //validate a kafka topic name 
   std::int16_t validate_topic_name(const std::string&name){
      //empty name are invalid 
      if(name.empty()){
         return kInvalidTopic;
      } 
      //kafka topic names have a max lenght of 249 
      if(name.size() > 249){
         return kInvalidTopic;
      }  
      //these two names are invalid 
      if(name == "." || name==".."){
         return kInvalidTopic;
      } 
      //check every charater 
      for(const unsigned char chracter : name){
         const bool valid = (chracter >= 'a' && chracter <= 'z') || (chracter >= 'A' && chracter <= 'Z') ||(chracter >= '0' && chracter <= '9') || chracter =='.' ||chracter =='_'||chracter=='-'; 
         if(!valid){
            return kInvalidTopic;
         } 
      } 
      return kNone;
   }  
   //convert the requested assignment into partition metadata 
   std::optional<std::vector<partionMetadata>>build_manual_partion(const createTopicEntry&topic){ 
      //manual assigment requires both fields to -1
      if(topic.num_partion != -1 ||topic.replication_factor != -1){
         return std::nullopt;
      }   
      //there must  be at least one assignment 
      if(topic.asisgment.empty()){
         return std::nullopt;
      } 
      //copy assigment so they can be sorted 
      auto assigment = topic.asisgment; 
      //sort by partion index
      std::sort(assigment.begin(),assigment.end(),[](const CreateTopicAssigment&left, const CreateTopicAssigment&right){
         return left.partion_index < right.partion_index;
      }); 
      //build final partion 
      std::vector<partionMetadata>partions; 
      partions.reserve(assigment.size()); 
      //partion id must be contigious from zero  
      for(std::size_t i =0; i < assigment.size(); ++i){
         const auto&assign = assigment[i]; 
         if(assign.partion_index != static_cast<std::int32_t>(i)){
            return std::nullopt;
         }    
         if(assign.broker_ids.size() != 1 || assign.broker_ids[0] != kBrokerId){
            return std::nullopt;
         }  
         //construct the partionmetadat 
         partionMetadata partion; 
         partion.partion_index  = assign.partion_index; 
         partion.leader_id = kBrokerId; 
         partion.leader_epoch = 0; 
         partion.replica_node = {kBrokerId}; 
         partion.isr_nodes ={kBrokerId}; 
         //store the partion 
         partions.push_back(std::move(partion));
      }
     return partions;
      
   } 
   std::optional<std::vector<partionMetadata>>build_automatic_partion(const createTopicEntry&topic){
      //this first implementation requires a positive partion count 
      if(topic.num_partion <=0 || topic.num_partion > kMaxArrayelemets){
         return std::nullopt;
      } 
      //single broker can only provide one replication factor 1  
      if(topic.replication_factor != 1){
       return std::nullopt;  
     }
      //maual assigment must be empty 
      if(!topic.asisgment.empty()){
         return std::nullopt;
      } 
      std::vector<partionMetadata>partions; 
      partions.reserve(static_cast<std::size_t>(topic.num_partion));   
      //create a partion indedexs 0 through -1   
      for(std::int32_t index =0; index < topic.num_partion; ++index){
         partionMetadata partion; 
         partion.partion_index = index;    
         partion.leader_epoch = 0; 
         partion.replica_node ={kBrokerId}; 
         partion.isr_nodes ={kBrokerId};  
         partions.push_back(std::move(partion));
      }
     return partions;
   }
   //*attempt to create one kafka topic 
   std::int16_t create_one_topic(const createTopicEntry&request){
      //first validate the topic name 
      const std::int16_t name_error = validate_topic_name(request.name); 
      if(name_error != kNone){
         return name_error;
      }    
      //reject a aleary existing topic 
      if(topic_manager().find_topic(request.name).has_value()){
         return KTopicAlreadyExist;
      } 
      if(!request.configs.empty()){
         return kInvalidConfig;
      } 
      //build partion according to reuest mode 
      std::optional<std::vector<partionMetadata>>partions; 
      if(!request.asisgment.empty()){
        partions =  build_manual_partion(request);   
         if(!partions.has_value()){
            return kinvalidreplicaAssigment;
         }
      }else{
         //automatic single nroker assigment  node 
         if(request.num_partion < 0){
            return KinvalidPartions;
         } 
         if(request.replication_factor != 1){
            return kreplicationFactor;
         } 
         partions = build_automatic_partion(request);
      } 
      //create the topic metadata object 
      TopicMetadata topic; 
      topic.name = request.name; 
      //the first version use a zero uuid placeholder 
      topic.topic_id.fill(std::byte{0});  
      //this is not a internal topic 
      topic.is_internal = false; 
      //store the partion  
      topic.partitions = std::move(*partions);    
      //serialize insert plus snapshot +save so snapshot reach disk in a order
      static std::mutex persist_mutext;  
      std::lock_guard<std::mutex>lock(persist_mutext); 

      //indert automatically to avoid duplicate creation races 
      if(!topic_manager().try_and_topic(std::move(topic))){
         return KTopicAlreadyExist;
      }  
      //take a snapshot of all current topics, includihg the new one 
      const std::vector<TopicMetadata>topics = topic_manager().list_topics(); 
      //persist the full snapshot 
      metadata_store().save_topics(topics);
      return kNone;
   }
   
   
}//namesapce ends 
//createTopic v0 request parse r 
createTopicRequest createTopicRequest::parse(const std::vector<std::byte> &body){
   //create a reader over the request 
   ByteReader reader(body); 
   //read the number of topics 
   const std::int32_t topic_count = read_array_length(reader); 
   //crete parse request 
   createTopicRequest request; 
   //resserve space for topic entries 
   request.topics.reserve(static_cast<std::size_t>(topic_count));  
   //parse every topic entry 
   for(std::int32_t i =0; i <topic_count; ++i){
      //read the topic name 
      createTopicEntry topic; 
      topic.name = read_string(reader);  
      //read the requeted partion count  
      topic.num_partion = reader.read_int32(); 
      //read the requeted replication factor 
      topic.replication_factor = reader.read_int16(); 
      //read every manual assigment  
      const std::int32_t assigment_count = read_array_length(reader);  
      //parse every manual assigment 
      topic.asisgment.reserve(static_cast<std::size_t>(assigment_count));
      for(std::int32_t j =0; j <assigment_count; ++j ){
         CreateTopicAssigment assigment; 
         //read the partion index 
         assigment.partion_index = reader.read_int32(); 
         //read broker id array 
         const std::int32_t broker_count = read_array_length(reader); 
         //read the broker id 
         assigment.broker_ids.reserve(static_cast<std::size_t>(broker_count)); 
         for(std::int32_t k =0; k < broker_count; ++k){
            assigment.broker_ids.push_back(reader.read_int32()); 
            
         } 
         //store the assigmnt 
         topic.asisgment.push_back(std::move(assigment));  
      }
         //read topic configuration error 
         const std::int32_t comfig_count = read_array_length(reader); 
         //parse configuration entries 
         topic.configs.reserve(static_cast<std::size_t>(comfig_count)); 
         for(std::int32_t j =0; j <comfig_count; ++j){
            createTopicConfig config; 
            //read config name 
            config.name = read_string(reader); 
            //raad nullable configuration value 
            config.value = read_nullable_string(reader); 
            //store the configs 
            topic.configs.push_back(std::move(config));
         } 
         //store the topic request 
         request.topics.push_back(std::move(topic));
      } 
      //read request timeout 
      request.timeout_ms = reader.read_int32(); 
      //reject unexpected trailling bytes 
      if(!reader.empty()){
         throw std::invalid_argument("Unexpected trailling bytes in createtopics request");
      } 
      return request;
   } 
 //createtopics handler 
std::vector<std::byte>handle_create_topics(const Requestheader& header, const std::vector<std::byte>&body){
   //parse the request body 
   const createTopicRequest request = createTopicRequest::parse(body); 
   //start building the reponse payload 
   ByteWriter writer; 
   //write correlation id  from the request 
   writer.write_int32(header.correlation_id); 
   //write the no of topic results
   writer.write_int32(static_cast<std::int32_t>(request.topics.size())); 
   //return one result for every requested topics 
   for (const createTopicEntry & topic: request.topics) {
      //write thr topic name  
      Write_string(writer, topic.name); 
      //attemp to create the topic  
      const std::int16_t error_code = create_one_topic(topic);  
      //write kafka error code 
      writer.write_int16(error_code); 
     

   }  
    //return the complete framed 
   return add_frame(writer.data());
}   

