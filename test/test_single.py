from kafka import KafkaClient

client = KafkaClient(bootstrap_servers="localhost:9092", api_version=(0, 8, 2))
client.poll(future=client.cluster.request_update())

print("brokers:", client.cluster.brokers())
print("topics:", client.cluster.topics())
print("orders partitions:", client.cluster.partitions_for_topic("orders"))  
print(client.cluster.partitions_for_topic("payments"))  # expect {0, 1}
print(client.cluster.partitions_for_topic("users"))      # expect {0, 1} 
print(client.cluster.partitions_for_topic("mom"))  
print(client.cluster.leader_for_partition(
    __import__("kafka").TopicPartition("orders", 0)))   # expect 1