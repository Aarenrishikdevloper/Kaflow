import time
from kafka import KafkaClient
from kafka.protocol.admin import DeleteTopicsRequest_v0

BOOTSTRAP = "127.0.0.1:9092"


def delete_topics(client, node_id, names, timeout_ms=30000):
    request = DeleteTopicsRequest_v0(
        topics=names,
        timeout=timeout_ms,
    )

    future = client.send(node_id, request)
    client.poll(future=future)

    if future.failed():
        raise future.exception

    # v0 response: topic_error_codes = [(topic_name, error_code), ...]
    return future.value.topic_error_codes


def main():
    client = KafkaClient(bootstrap_servers=BOOTSTRAP, api_version=(0, 10, 1))

    try:
        node_id = client.least_loaded_node()

        deadline = time.time() + 10
        while not client.ready(node_id):
            if time.time() > deadline:
                raise RuntimeError("Could not connect to broker")
            client.poll(timeout_ms=100)

        results = delete_topics(client, node_id, [
            "client_orders",
            "client_users",
            "client_manual",
        ])

        for name, error_code in results:
            print(f"{name}: error_code={error_code}")

    finally:
        client.close()


if __name__ == "__main__":
    main()