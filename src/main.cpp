#include "metadata/topic.hpp"
#include "metadata/topic_manager.hpp"
#include "server/tcp_server.hpp"
#include "storage/partion_directory_manager.hpp"
#include "storage/storage.hpp"
#include <exception>
#include <iostream>
#include <utility>
#include <vector>
#include "storage/metadata_store.hpp"
int main() {
  try { 
    StorageManager storage("data");  
    storage.intialize(); 
    MetadataStore metadata_store(storage.root_directory()/"metadata.bin");  
    std::vector<TopicMetadata> saved_topics = metadata_store.load_topics(); 
    topic_manager().load_topics(std::move(saved_topics));         
    PartionDirectoryManager partion_storage(storage.topics_directory());
    for(const auto&topic: saved_topics){
       partion_storage.create_topic_partion(topic);
    }
    TcpServer server(9092);
    server.start();
   
  } catch (const std::exception&e) {  
      std::cerr <<"Broker startup Failed " << e.what()<< '\n'; 
      return 1;
  } 
   return 0;
}