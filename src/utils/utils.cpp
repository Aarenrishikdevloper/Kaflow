#include "utils.hpp"     
#include "metadata/topic.hpp"
#include "protocol/compact_writer.hpp"
#include <array>
#include <cstddef>
#include <limits>
#include <optional>
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