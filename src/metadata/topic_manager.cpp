#include "topic_manager.hpp"    
#include "topic.hpp"
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
bool TopicManager::try_and_topic(TopicMetadata topic){
  //lock the mutext 
  std::lock_guard<std::mutex>lock(mutext_);  
  //try to insert the topics using its name as key  
  const auto [iterator, inserted] = topics_.emplace(topic.name, std::move(topic));   
  //retunr whether the iteraor suceded 
  return inserted;

}
//fina a topic by name 
std::optional<TopicMetadata> TopicManager::find_topic(const std::string &topic_name) const{
  //loc the mutex 
  std::lock_guard<std::mutex>lock(mutext_); 
  //search for the reueted topic 
  const auto iterator = topics_.find(topic_name); 
  //retun nullopt if the topic does not exist 
  if(iterator == topics_.end()){
    return  std::nullopt;
  } 
  return iterator->second;
}   
std::vector<TopicMetadata> TopicManager::list_topics() const{
  //lock the topic map while reading 
  std::lock_guard<std::mutex>lock(mutext_); 
  //this vector will hold copies of all topic metadata 
  std::vector<TopicMetadata>result; 
  //reserve enough space for aall currently known topics 
  result.reserve(topics_.size()); 
  //iterate over every topics stored in map 
  for (const auto&[topic_name, topic_metadata] : topics_) {
       //copy the topiuc metadatainot the result 
       result.push_back(topic_metadata);
  } 
  return result;
}   
bool TopicManager::remove_topic(const std::string &name){
  //lock mutex 
  std::lock_guard<std::mutex>lock(mutext_); 
  //in memory deletion 
  return topics_.erase(name) >0;
}
//return the shared topic manager 
TopicManager& topic_manager(){
  static TopicManager manager; 
  return manager;
}