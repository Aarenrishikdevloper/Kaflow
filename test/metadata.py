import sys
import time
from kafka import KafkaClient
from kafka.protocol.metadata import MetadataRequest_v0

BOOTSTRAP = "127.0.0.1:9092"


def fetch_metadata(client, node_id, topics=None):
    # v0: empty list = all topics, otherwise only the named topics.
    request = MetadataRequest_v0(topics=topics or [])

    future = client.send(node_id, request)
    client.poll(future=future)

    if future.failed():
        raise future.exception

    return future.value


def main():
    topics = sys.argv[1:]  # optional: python3 test_metadata.py client_orders missing_topic

    # (0, 9) makes kafka-python use Metadata v0 for its own refreshes,
    # which is the only version your broker supports.
    client = KafkaClient(bootstrap_servers=BOOTSTRAP, api_version=(0, 9))

    try:
        node_id = client.least_loaded_node()

        deadline = time.time() + 10
        while not client.ready(node_id):
            if time.time() > deadline:
                raise RuntimeError("Could not connect to broker")
            client.poll(timeout_ms=100)

        response = fetch_metadata(client, node_id, topics)

        print("Brokers:")
        for broker_id, host, port in response.brokers:
            print(f"  id={broker_id} host={host} port={port}")

        print("\nTopics:")
        if not response.topics:
            print("  (none)")

        for error_code, name, partitions in response.topics:
            print(f"  {name}: error_code={error_code}, partitions={len(partitions)}")
            for p_error, p_index, leader, replicas, isr in partitions:
                print(
                    f"    partition={p_index} error={p_error} "
                    f"leader={leader} replicas={replicas} isr={isr}"
                )
    finally:
        client.close()


if __name__ == "__main__":
    main()