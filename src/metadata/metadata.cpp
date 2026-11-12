#include "metadata/topic.hpp"
#include "metadata/topic_manager.hpp"
#include "protocol/byte_reader.hpp"
#include "protocol/byte_writer.hpp"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector> 
#include "metadata.hpp"
namespace  {
   constexpr std::int32_t kBrokerId = 1; 
   constexpr std::int32_t kBrokerPort = 9092;  
   const std::string kbrokerHost = "127.0.0.1"; 
   //kafka error code 
   constexpr std::int32_t kNoerror = 0; 
   constexpr std::int32_t kUnknownTopicsOrpartion =3; 

   //legacy kafka helper 
   //write a kafka string 
   void Write_string(ByteWriter&writer, const std::string&value){
     if(value.size() > static_cast<std::size_t>(std::numeric_limits<std::int16_t>::max())){
        throw std::length_error("kafka string is too long");
     } 
     //write the number of bytes not the no of characters 
     writer.write_int16(static_cast<std::int16_t>(value.size()));  
     //convert the string bytes into std::byte vector 
     std::vector<std::byte> bytes; 
     bytes.reserve(value.size()); 
     for(unsigned char chracter :value){
       bytes.push_back(static_cast<std::byte>(chracter));  
     }  
     //append the acual string bytes
     writer.Write_bytes(bytes);
   }  
   //write an array of broker mode ids 
   void write_node_array(ByteWriter&writer, const std::vector<std::int32_t>&nodes){
      //check that the element count fits an int32  
      if(nodes.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())){
         throw std::length_error("kafka array is too large");
      } 
      //write the  no of new id 
      writer.write_int32(static_cast<std::int32_t>(nodes.size()));  
      for(const std::int32_t node_id: nodes){
         writer.write_int32(node_id);
      }
   } 
   //response serialization 
   //write one broker currently known 
   void write_broker(ByteWriter& writer){
      //we currently support  one broker 
      writer.write_int32(1); 
      //write broker id 
      writer.write_int32(kBrokerId); 
      //write broker hostname 
      Write_string(writer, kbrokerHost); 
      //write broker listening port
      writer.write_int32(kBrokerPort);
   } 
   //write one partion in metadata API V0 format 
   void write_partion(ByteWriter& writer, const partionMetadata& partion){
      //write the current  leader broker 
      writer.write_int16(kNoerror); 
      //write the parition index   
      writer.write_int32(partion.partion_index);
      //weiter the partion leader id 
      writer.write_int32(partion.leader_id); 
      //werite all replicas 
      write_node_array(writer, partion.replica_node); 
      //write all in sync replicas 
      write_node_array(writer, partion.isr_nodes);
   } 
   //write metadata for one topic 
   void write_topic(ByteWriter &writer, const TopicMetadata &topic){  
      //topic level error code 
      writer.write_int16(kNoerror); 
      //write the topic name 
      Write_string(writer, topic.name); 
      //make a copy so we can sort partion without changing 
      std::vector<partionMetadata>partions = topic.partitions; 
      //keep partion in assending partion index order 
      std::sort(partions.begin(),partions.end(),[](const partionMetadata &left,const partionMetadata &right){
         return left.partion_index  < right.partion_index  ;
      }); 
      //check before converting to int32 
      if(partions.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())){
         throw std::length_error("Too many topic partion");
      } 
      //write the no  of partions 
      writer.write_int32(static_cast<std::int32_t>(partions.size())); 
      //serialize each partion 
      for(const partionMetadata&partion: partions){
         write_partion(writer,partion);
      }
 
   }   
    //write a topic that do not exist 
    void write_unkown_topics(ByteWriter &writer, const std::string&topic_name){
      //topicerror unknowntopic 
      writer.write_int16(kUnknownTopicsOrpartion); 
      //include the requested topic name 
      Write_string(writer, topic_name);  
      //uknown topics have no known partion 
      writer.write_int32(0);

    } 
    //add the four byte lenght prefix to aresponse load 
    std::vector<std::byte>add_frame(const std::vector<std::byte>& payload){
      if(payload.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())){
         throw std::length_error("Kafka error is too large");
      }  
      //create a write for the final frmae response   
      ByteWriter frame; 
       //write the payload length 
       frame.write_int32(static_cast<std::int32_t>(payload.size()));  
       //write the payload 
       frame.Write_bytes(payload);
       //append the complete frame 
       return frame.data();
    }
} //namepsace ends 
MetaDataRequest MetaDataRequest::parse(const std::vector<std::byte> &body){
   //create a reader over request body 
   ByteReader reader(body); 
   //read the no of requeted topics 
   const std::int32_t topic_count = reader.read_int32();  
   //a count less than -1 is valid
   if(topic_count < -1){
      throw std::invalid_argument("invalid metadata topics");
   }  
   //create a parsed request 
   MetaDataRequest request; 
   //in this implementation  null or empty means all topics 
   if(topic_count == -1 || topic_count == 0){ 
      request.topics.clear();
   }     
   else {
      //avoid reserving an unreasonable amount of memory 
       constexpr std::int32_t kmaxRequestedTopics = 10000; 
       if(topic_count > kmaxRequestedTopics){
          throw std::invalid_argument("Too many topics in metadata requst");
       }  
        request.topics.reserve(static_cast<std::size_t>(topic_count));    
        //read each regular kafka string 
        for(std::int32_t i =0; i < topic_count; i++){
         const std::int16_t string_lenght = reader.read_int16(); 
         //topicnames in this request are not null string  
         if(string_lenght < 0){
            throw std::invalid_argument("Null topic name in metadata request");
         } 
         //read exactly the declared no of bytes 
         const std::vector<std::byte> bytes = reader.read_bytes(static_cast<std::size_t>(string_lenght)); 
         //conver vthe bytes into a  c++ string 
         std::string topic_name;   
         topic_name.reserve(bytes.size()); 
         for(const std::byte byte : bytes){
            topic_name.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
         } 
         request.topics.push_back(std::move(topic_name));

        }

   }   
   //reject unexpected traillingt bytes
   if(!reader.empty()){
     throw  std::invalid_argument("Unexpected traling bytes in metadata request");
   } 
   //return the request 
   return request;

}   
//Metadata API v0 handler  
std::vector<std::byte>handle_metadata(const Requestheader header, const std::vector<std::byte> body){
   //parse the metatdata APi reuest body 
   const MetaDataRequest request = MetaDataRequest::parse(body); 
   //this vector will contain the topics to serialize 
   std::vector<TopicMetadata>topics;   
   //empty topics returns all known topics 
   if(request.topics.empty()){
      topics = topic_manager().list_topics();
   }else{
      //lookup each  explicitly requested topics 
      for(const std::string&topic_name : request.topics){
         const std::optional<TopicMetadata>topic = topic_manager().find_topic(topic_name); 
         if(topic.has_value()){
            topics.push_back(*topic); 

         }else{
            TopicMetadata unknowntopic; 
            unknowntopic.name = topic_name; 
            //an empty  list plus a  topic level error is serialized 
            topics.push_back(std::move(unknowntopic));

         }
      }
   } 
   //sort topics alphabeticaaly for deterministic response 
   std::sort(topics.begin(), topics.end(),[](const TopicMetadata&left, TopicMetadata&right){
      return left.name < right.name;
   }); 
   //start building the response payload 
   ByteWriter writer; 
   //write the correlation Id  
   writer.write_int32(header.correlation_id); 
   //write the broker id 
   write_broker(writer); 
   //response bosy topic  
   //validate the topic count 
   if(topics.size() > static_cast<std::size_t>(std::numeric_limits<std::int32_t>::max())){
      throw std::length_error("Too many topics in response");
   }  
   //write the number of topics 
   writer.write_int32(static_cast<std::int32_t>(topics.size()));
   //serialize  every topic 
   for(const TopicMetadata&topic:topics){
      //look up for topic to distinguish a real topic from unknown topic 
      std::optional<TopicMetadata> stored_topic = topic_manager().find_topic(topic.name); 
      if(stored_topic.has_value()){
         //serialize know topic 
         write_topic(writer, *stored_topic);
      }else {
         //serialize unknow topic 
         write_unkown_topics(writer, topic.name);
      }
   } 
   return add_frame(writer.data());
}
