#include "metadata/topic.hpp"
#include "protocol/byte_reader.hpp"
#include "protocol/byte_writer.hpp"
#include <bit>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <stdexcept>
#include <string>
#include <system_error>
#include <vector> 
#include "metadata_store.hpp"
namespace  {
  //the first four bytes identify  our metadata file 
  constexpr std::byte MAGIC[] ={
    static_cast<std::byte>('M'),  
    static_cast<std::byte>('K'),  
    static_cast<std::byte>('M'),   
    static_cast<std::byte>('D'),  
      

  }; 
  //increment this if we change the binary format later 
  constexpr std::int32_t FORMAT_VERSION =1; 
  //defend  against corrupted files requesting unreasonable allocation  
  constexpr std::int32_t MAX_ITEM_COUNT = 1'000'000; 
  constexpr std::int32_t MAX_TOPIC_NAME_LENGHT =  1'000'000;        
  constexpr std::uintmax_t MAX_METADATA_FILE_SIZE = 256ULL * 1024ULL * 1024ULL;    
  //conevert a string into bytes and append it into the writer 
  void Write_string(ByteWriter & writer, const std::string value){
    if(value.size() > static_cast<std::size_t>(MAX_TOPIC_NAME_LENGHT)){
        throw std::runtime_error("Topic name is too long to persist");
    } 
    //write tghe string first lenght first 
    writer.write_int32(static_cast<std::int32_t>(value.size()));  
    //convert v every chracter into one byte 
     std::vector<std::byte> bytes; 
     bytes.reserve(value.size());
    for(unsigned char chracter: value){ 
         bytes.push_back(static_cast<std::byte>(chracter));
        
    } 
    //append the string 
    writer.Write_bytes(bytes);
  }
  //read a lenght-prefixed string 
  std::string read_string(ByteReader & reader){
    const std::int32_t lenght = reader.read_int32(); 
    if(lenght <  0 || lenght > MAX_TOPIC_NAME_LENGHT){
      throw std::runtime_error("Invalid topic name lenght in metadata");
    }  
    const auto bytes = reader.read_bytes(static_cast<std::size_t>(lenght));
    std::string result; 
    result.reserve(bytes.size()); 
    for (const std::byte byte:bytes) {
    result.push_back(static_cast<char>(std::to_integer<unsigned char>(byte)));
    } 
    return result;
  }
  //write a vector of replica id 
  void write_int_vector(ByteWriter & writer, const std::vector<std::int32_t>&values){
    if(values.size() > static_cast<std::size_t>(MAX_ITEM_COUNT)){
      throw std::runtime_error("To many replicas id"); 
       
       
    } 
    writer.write_int32(static_cast<std::int32_t>(values.size())); 
    for(const std::int32_t value:values){
      writer.write_int32(value);
    }
  } 
  //read a vector of replica IDS 
  std::vector<std::int32_t>read_intvector(ByteReader & reader){
    const std::int32_t count = reader.read_int32(); 
    if(count < 0 || count > MAX_ITEM_COUNT){
      throw std::runtime_error("Invalid replica count");
    } 
    std::vector<std::int32_t> result; 
    result.reserve(static_cast<std::size_t>(count));  
    for (std::int32_t index = 0; index < count; ++index) {
      result.push_back(reader.read_int32());
    } 
    return result;
  } 
  //write one partion and all its replica list 
  void write_partion(ByteWriter & writer, const partionMetadata partion){
    writer.write_int32(partion.partion_index); 
    //prserve all 32 bit of the unsigned  leader id 
    writer.write_int32(std::bit_cast<std::int32_t>(partion.partion_index));   
    writer.write_int32(partion.leader_epoch);
    write_int_vector(writer, partion.replica_node); 
    write_int_vector(writer, partion.isr_nodes); 
    write_int_vector(writer, partion.elligibe_leader_replicas); 
    write_int_vector(writer, partion.offline_replicas);
  } 
  //read one partion 
  partionMetadata read_partion(ByteReader & reader){
    partionMetadata partion{}; 
    partion.partion_index = reader.read_int32(); 
    //restore unsined leader id without cj=hanging its bit patteern 
    partion.leader_id = std::bit_cast<std::uint32_t>(reader.read_int32()); 
    partion.leader_epoch = reader.read_int32(); 
    partion.replica_node = read_intvector(reader); 
    partion.isr_nodes= read_intvector(reader); 
    partion.elligibe_leader_replicas = read_intvector(reader); 
    partion.offline_replicas = read_intvector(reader); 
    return partion;

  } 
  //write one topic and all its partion 
  void write_topics(ByteWriter & writer, const TopicMetadata & topic){
    Write_string(writer, topic.name); 
    //the uuid is always exactly 16 bytes 
    std::vector<std::byte>uuid_bytes (topic.topic_id.begin(), topic.topic_id.end()); 
    writer.Write_bytes(uuid_bytes);  
    //store bool exactly one byte 0 or 1  
    const std::vector<std::byte>internal_flag = {topic.is_internal ? std::byte{1}:std::byte{0}}; 
    writer.Write_bytes(internal_flag); 
    writer.write_int32(topic.authorize_operations);   
    if(topic.partitions.size() > static_cast<std::size_t>(MAX_ITEM_COUNT)){
       throw std::runtime_error("Too many partion to persist");
    } 
    writer.write_int32(static_cast<std::int32_t>(topic.partitions.size())); 
    for(const auto&partion: topic.partitions){
      write_partion(writer, partion);
    }
  }  
  //read one topic and all its partions  
  TopicMetadata read_topics(ByteReader & reader){
    TopicMetadata topic{}; 
    topic.name = read_string(reader);   
    const auto uuid_bytes = reader.read_bytes(16); 
    for(std::size_t index =0; index < topic.topic_id.size(); ++index){
      topic.topic_id[index] = uuid_bytes[index];
    }  
    const auto internal_flag = reader.read_bytes(1); 
    const unsigned char flag = std::to_integer<unsigned char>(internal_flag[0]);  
    if(flag > 1){
       throw std::runtime_error("Inteernal flag error");
    } 
    topic.is_internal = (flag == 1);  
    topic.authorize_operations = reader.read_int32(); 
    const std::int32_t partion_count = reader.read_int32(); 
    if(partion_count < 0 || partion_count > MAX_ITEM_COUNT){
       throw std::runtime_error("Invalid partion Count in metatdata");
    }  
    topic.partitions.reserve(static_cast<std::size_t>(partion_count));   
    for (std::int32_t index =0 ; index < partion_count; ++index) {
       topic.partitions.push_back(read_partion(reader));
    } 
    return topic;
  } 
  //build the complete metadata file in memory 
  std::vector<std::byte>serialize_topics(const std::vector<TopicMetadata>&topics){
    if(topics.size() > static_cast<std::size_t>(MAX_ITEM_COUNT)){
      throw std::runtime_error("Too many topics to persist");
    } 
    ByteWriter writer;  
    //file signature 
    const std::vector<std::byte>magic_bytes( MAGIC,MAGIC+4); 
    writer.Write_bytes(magic_bytes);   
    //binary format version 
    writer.write_int32(FORMAT_VERSION);   
    //no of topics  
    writer.write_int32(static_cast<std::int32_t>(topics.size()));   
    //seririze every topic
    for(const auto&topic:topics){
      write_topics(writer, topic); 

    } 
    return  writer.data();

  }      
  //parse the complete metadata file
  std::vector<TopicMetadata>deserilaize_topics(const std::vector<std::byte>&bytes){
    ByteReader reader(bytes); 
    //verif the file signature 
    const auto magic = reader.read_bytes(4); 
    for(std::size_t index =0; index < 4; ++index){
      if(magic[index] != magic[index]){
        throw std::runtime_error("Invalid metadata file signature");
      }
    }  
    //verify the binary format version 
    const std::int32_t version = reader.read_int32(); 
    if(version != FORMAT_VERSION){
       throw std::runtime_error("Unsupported metadada file version");
    } 
    //read and validate topic count  
    const std::int32_t topic_count = reader.read_int32(); 
    if(topic_count < 0 || topic_count > MAX_ITEM_COUNT){
      throw std::runtime_error("Invalid topic count in metatdata");
    }  
    std::vector<TopicMetadata>topics; 
    topics.reserve(static_cast<std::size_t>(topic_count));  
    for(std::int32_t index =0; index <topic_count;  ++index){
      topics.push_back(read_topics(reader));
    }  
    //reject unexpected bytes after the final topic
    if(!reader.empty()){
       throw std::runtime_error("Unexpected trailing metadad bytes");
    }
    return topics;
  }
} //namesapce ends  
MetadataStore::MetadataStore(const std::filesystem::path&file_path):file_path_(file_path){} 
bool MetadataStore::exists() const{
  return std::filesystem::exists(file_path_);    
} 
void MetadataStore::save_topics(const std::vector<TopicMetadata> &topics) const{
  //serialize before tochinging existing file  
  const std::vector<std::byte>bytes = serialize_topics(topics); 
  const auto parent = file_path_.parent_path(); 
  if(!parent.empty()){
    std::filesystem::create_directories(parent);
  } 
  //write to temoarary file in the same directory 
  std::filesystem::path temoarary_path  =file_path_; 
  temoarary_path += ".tmp"; 
  {
    std::ofstream output(temoarary_path, std::ios::binary | std::ios::trunc);  
    if(!output){
      throw std:: runtime_error("Could not open temporay metadat file for writing");
    } 
    output.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));       
    output.flush(); 
    if(!output){
      throw std:: runtime_error("Failked wgile wrtiing");
    } 
  }   
  //rename the temporary file 
  std::error_code error ; 
  std::filesystem::rename(temoarary_path, file_path_ , error);   
  if(error){
    throw  std::filesystem::filesystem_error("Could not replace metadata file", temoarary_path,file_path_,error);
  }  

} 
std::vector<TopicMetadata>MetadataStore::load_topics() const{
  if(!exists()){
     return {};
  } 
  //reject unexpected lage files before allocating memory 
  const auto file_size = std::filesystem::file_size(file_path_); 
  if(file_size > MAX_METADATA_FILE_SIZE){
    throw std::runtime_error("Metadata file exceedes size limits");
  } 
  std::ifstream input(file_path_, std::ios::binary); 
  if(!input){
    throw std::runtime_error("Could not open meatdata file");
  }  
  //read the complete file into abyte vector 
  std::vector<char>raw_bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());  
  if(input.bad()){
    throw std::runtime_error("Failed while reading metadata file");
  } 
  std::vector<std::byte>bytes; 
  bytes.reserve(raw_bytes.size()); 
  for(const unsigned char value:raw_bytes){
    bytes.push_back(static_cast<std::byte>(value));
  } 
  //parse and return the reconstructed topics 
  return  deserilaize_topics(bytes);
} 
MetadataStore & metadata_store(){
  static MetadataStore store("data/metadata.bin"); 
  return store;
}