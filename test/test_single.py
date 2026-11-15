import time
from kafka import KafkaClient
from kafka.protocol.admin import CreateTopicsRequest_v0

BOOTSTRAP = "127.0.0.1:9092"


def create_topics(client, node_id, topics, timeout_ms=30000):
    """
    topics: list of dicts with keys:
        name, num_partitions, replication_factor,
        assignments (optional): [(partition_index, [broker_ids])]
        configs (optional): [(name, value)]
    """
    request = CreateTopicsRequest_v0(
        create_topic_requests=[
            (
                t["name"],
                t["num_partitions"],
                t["replication_factor"],
                t.get("assignments", []),
                t.get("configs", []),
            )
            for t in topics
        ],
        timeout=timeout_ms,
    )

    future = client.send(node_id, request)
    client.poll(future=future)

    if future.failed():
        raise future.exception

    response = future.value
    # v0 response: topic_errors = [(topic_name, error_code), ...]
    return response.topic_errors


def main():
    # api_version is pinned so the client skips ApiVersions probing,
    # which your broker may not support yet.
    client = KafkaClient(bootstrap_servers=BOOTSTRAP, api_version=(0, 10, 1))

    try:
        node_id = client.least_loaded_node()

        # Wait for the connection to be ready.
        deadline = time.time() + 10
        while not client.ready(node_id):
            if time.time() > deadline:
                raise RuntimeError("Could not connect to broker")
            client.poll(timeout_ms=100)

        results = create_topics(client, node_id, [
            {"name": "client_orders", "num_partitions": 3, "replication_factor": 1},
            {"name": "client_users", "num_partitions": 2, "replication_factor": 1},
            {
                "name": "client_manual",
                "num_partitions": -1,
                "replication_factor": -1,
                "assignments": [(0, [1]), (1, [1])],
            },
        ])

        for name, error_code in results:
            print(f"{name}: error_code={error_code}")

    finally:
        client.close()


if __name__ == "__main__":
    main()
