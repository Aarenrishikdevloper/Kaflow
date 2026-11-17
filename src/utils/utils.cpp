#include "utils.hpp"     
#include "metadata/topic.hpp"
#include "protocol/byte_reader.hpp"
#include "protocol/compact_writer.hpp"
#include <array>
#include <cstddef>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>
const APIversionInfo*find_api(std::int16_t api_key){
    //search through the supported api 
     for (const APIversionInfo&api : supported_apis()) {     
        //check whther this is the requested api 
        if(api.api_key == api_key){
            return &api;
        }
     
     } 
     return nullptr;
}   
void write_broker_array(CompacrWriter &writer, const std::vector<std::int32_t> &brokers){
    //write the no of broker id 
    writer.wriet_compoact_arraylenght(static_cast<std::uint32_t>(brokers.size()));   
    //write each broker id 
    for (const std::int32_t broker_id:brokers) {
       writer.write_int32(broker_id);
    }
}   
void write_nullable_broker_array(CompacrWriter &writer, const std::vector<int32_t> &broker){
    if(broker.empty()){
        writer.write_null_comapct_array(); 
        return;
    } 
    write_broker_array(writer,broker);
}  
void write_partion(CompacrWriter &writer, const partionMetadata &partion){
    writer.write_int16(0);  
    writer.write_int32(partion.partion_index);
    writer.write_int32(partion.leader_id); 
    writer.write_int32(partion.leader_epoch);  
    write_broker_array( writer, partion.replica_node);
    write_broker_array(writer, partion.isr_nodes);   
    write_nullable_broker_array(writer, partion.elligibe_leader_replicas);  
    writer.wriet_compoact_arraylenght(0);
    write_nullable_broker_array(writer, partion.offline_replicas); 
    writer.write_emty_tagged_field();

} 
void write_topic(CompacrWriter &writer, const TopicPageEntry&entry){
    //topic error code  0 mens succeded 
    writer.write_int16(0); 
    //write topic name 
    writer.write_compact_string(entry.topic.name);  
    //write the topic uuid  
    std::vector<std::byte>uuid_bytes(entry.topic.topic_id.begin(), entry.topic.topic_id.end()); 
    writer.write_bytes(uuid_bytes);
  
    //write ehwther topic is internal 
    writer.Write_booleand(entry.topic.is_internal); 
    //wete the no of partions in this repsonse 
    writer.wriet_compoact_arraylenght(static_cast<std::uint32_t>(entry.partions.size()));  
    //encode each partion 
    for(const partionMetadata&partion:entry.partions){
        write_partion(writer, partion); 

    } 
    //write topic authorization operation   
    writer.write_int32(entry.topic.authorize_operations); 
    //topic tagged field :: none 
    writer.write_emty_tagged_field();
} 
//encode an unknow topic 
void write_unkown_topic(CompacrWriter &writer, const std::string &topic_name){
    //error code 3 meamns UNKNIOW_TOPIC_PARTION   
    writer.write_int16(3); 
    //include reqyeted topic name   
    writer.write_compact_string(topic_name); 
    //uknown topic has zero uuid 
    const std::array<std::byte, 16>zero_uuid{}; 
    //write all 16 uuid bytes 
    for(const std::byte value:zero_uuid){ 
        std::vector<std::byte>one_byte{value};
        writer.write_bytes(one_byte);
    } 
    //Unknown topics are not internal topics 
    writer.Write_booleand(false); 
    //unknown topics have no partion 
    writer.wriet_compoact_arraylenght(0); 
    //authorization opertion are unavalaible
     writer.write_int32(std::numeric_limits<std::int32_t>::min()); 
     //topic tagged fields :: none 
     writer.write_emty_tagged_field();
}
void write_next_curosr(const std::optional<DescribeTopicCursor>&curosor, CompacrWriter&writer){ 
    std::vector<std::byte>marker{curosor.has_value() ? std::byte(0x01):std::byte(0xFF)};
    writer.write_bytes(marker);  
    if(curosor.has_value()){
        writer.write_compact_string(curosor->Topic_name); 
        writer.write_int32(curosor->partition_index); 
        writer.write_emty_tagged_field();
    }
}  
//legacy kafka helper 
//read a kafka string 
   std::string read_string(ByteReader& reader){
    const std::int16_t lenght  = reader.read_int16(); 
    if(lenght < 0){
        throw std::invalid_argument("Unexpected null kafka String");
    } 
    //read exactly the no of bytes decalred 
    const std::vector<std::byte>bytes = reader.read_bytes(static_cast<std::size_t>(lenght)); 
    std::string value; 
    value.reserve(bytes.size()); 
    for (const std::byte byte : bytes) {  
        value.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
    
    } 
    return value;
   }
   std::optional<std::string>read_nullable_string(ByteReader &reader){
    const std::int16_t lenght = reader.read_int16(); 
    //-1 represntation null 
    if(lenght == -1){
        return std::nullopt;
    } 
    //other negative lenght are invalid 
    if(lenght < 1){
        throw std::invalid_argument("Invalid nullable kafka String length");
    }  
    //read the string bytes 
    const std::vector<std::byte>bytes = reader.read_bytes(static_cast<std::size_t>(lenght));   
    //convert byte to cpp string 
    std::string value; 
    value.reserve(bytes.size()); 
    for (const std::byte byte : bytes) {
      value.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
    } 
    return value;
} 
//read a regular kafka array 
std::int32_t read_array_length(ByteReader& reader){
    //regular kafka array use int32 lenghts 
    const std::int32_t count = reader.read_int32(); 
    //these array are not nullable in current schema  
    if(count < 0 || count > 1000000){
        throw std::invalid_argument("invalid kafka array lenght");
    } 
    return count;
} 
void Write_string(ByteWriter&writer, const std::string& value){
    if(value.size() > static_cast<std::size_t>(std::numeric_limits<std::int16_t>::max())){
        throw std::length_error("Kafka String is too long");
    } 
    writer.write_int16(static_cast<std::int16_t>(value.size())); 
    //convert chracters to bytes 
    std::vector<std::byte>bytes; 
    bytes.reserve(value.size()); 
    for(const unsigned char chracter :value){
        bytes.push_back(static_cast<std::byte>(chracter));
    } 
    writer.Write_bytes(bytes);
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
    