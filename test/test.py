import socket
import struct


HOST = "127.0.0.1"
PORT = 9092


# ------------------------------------------------------------
# Kafka primitive encoders
# ------------------------------------------------------------

def i16(value):
    return struct.pack(">h", value)


def i32(value):
    return struct.pack(">i", value)


def kafka_string(value):
    encoded = value.encode("utf-8")
    return i16(len(encoded)) + encoded


# ------------------------------------------------------------
# CreateTopics v0 request builder
# ------------------------------------------------------------

def build_topic(
    name,
    num_partitions,
    replication_factor,
    assignments=None,
    configs=None
):
    if assignments is None:
        assignments = []

    if configs is None:
        configs = []

    result = b""

    # Topic name.
    result += kafka_string(name)

    # Partition count.
    result += i32(num_partitions)

    # Replication factor.
    result += i16(replication_factor)

    # Manual assignments array.
    result += i32(len(assignments))

    for partition_index, broker_ids in assignments:
        result += i32(partition_index)

        # Broker ID array.
        result += i32(len(broker_ids))

        for broker_id in broker_ids:
            result += i32(broker_id)

    # Topic configurations array.
    result += i32(len(configs))

    for config_name, config_value in configs:
        result += kafka_string(config_name)

        if config_value is None:
            result += i16(-1)
        else:
            result += kafka_string(config_value)

    return result


def build_request(topics, correlation_id=42, timeout_ms=30000):
    # Request header:
    # API key = 19
    # API version = 0
    # Correlation ID
    # Client ID = null
    request = (
        i16(19)
        + i16(0)
        + i32(correlation_id)
        + i16(-1)
    )

    # Topics array.
    request += i32(len(topics))

    for topic in topics:
        request += topic

    # CreateTopics v0 timeout.
    request += i32(timeout_ms)

    # Add Kafka frame length.
    return i32(len(request)) + request


# ------------------------------------------------------------
# Socket communication
# ------------------------------------------------------------

def read_exact(sock, size):
    data = b""

    while len(data) < size:
        chunk = sock.recv(size - len(data))

        if not chunk:
            raise RuntimeError("Broker closed the connection")

        data += chunk

    return data


def send_request(request):
    with socket.create_connection((HOST, PORT)) as sock:
        sock.sendall(request)

        # Read response frame length.
        frame_length = struct.unpack(
            ">i",
            read_exact(sock, 4)
        )[0]

        # Read the response payload.
        return read_exact(sock, frame_length)


# ------------------------------------------------------------
# CreateTopics v0 response parser
# ------------------------------------------------------------

def read_i16(data, offset):
    value = struct.unpack_from(">h", data, offset)[0]
    return value, offset + 2


def read_i32(data, offset):
    value = struct.unpack_from(">i", data, offset)[0]
    return value, offset + 4


def read_string(data, offset):
    length, offset = read_i16(data, offset)

    if length < 0:
        raise ValueError("Unexpected null string")

    value = data[offset:offset + length].decode("utf-8")

    return value, offset + length


def parse_response(data):
    offset = 0

    # Response header.
    correlation_id, offset = read_i32(data, offset)

    # Number of topic results.
    topic_count, offset = read_i32(data, offset)

    results = []

    for _ in range(topic_count):
        # Topic name.
        name, offset = read_string(data, offset)

        # Kafka error code.
        error_code, offset = read_i16(data, offset)

        results.append({
            "name": name,
            "error_code": error_code
        })

    # There should be no extra fields in v0.
    if offset != len(data):
        raise AssertionError(
            f"Unexpected trailing bytes: {len(data) - offset}"
        )

    return correlation_id, results


# ------------------------------------------------------------
# Test helper
# ------------------------------------------------------------

def run_test(description, topics):
    print(f"\n--- {description} ---")

    request = build_request(topics)

    response = send_request(request)

    correlation_id, results = parse_response(response)

    print("Correlation ID:", correlation_id)

    for result in results:
        print(
            f"Topic: {result['name']}, "
            f"error: {result['error_code']}"
        )

    assert correlation_id == 42
    assert len(results) == len(topics)

    return results


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

if __name__ == "__main__":

    # Test 1:
    # Create a topic with three partitions.
    results = run_test(
        "Create topic with three partitions",
        [
            build_topic(
                "python_orders",
                3,
                1
            )
        ]
    )

    assert results[0]["error_code"] == 0

    # Test 2:
    # Create multiple topics in one request.
    results = run_test(
        "Create multiple topics",
        [
            build_topic("python_users", 2, 1),
            build_topic("python_payments", 4, 1),
        ]
    )

    assert all(r["error_code"] == 0 for r in results)

    # Test 3:
    # Manual assignments to the single broker.
    results = run_test(
        "Create topic with manual assignments",
        [
            build_topic(
                "python_manual",
                -1,
                -1,
                assignments=[
                    (0, [1]),
                    (1, [1]),
                ]
            )
        ]
    )

    assert results[0]["error_code"] == 0

    # Test 4:
    # Creating an existing topic should return 36.
    results = run_test(
        "Duplicate topic",
        [
            build_topic("python_orders", 3, 1)
        ]
    )

    assert results[0]["error_code"] == 36

    # Test 5:
    # Invalid topic name should return 17.
    results = run_test(
        "Invalid topic name",
        [
            build_topic("bad/name", 3, 1)
        ]
    )

    assert results[0]["error_code"] == 17

    # Test 6:
    # Single broker cannot support replication factor 2.
    results = run_test(
        "Invalid replication factor",
        [
            build_topic("python_invalid_rf", 3, 2)
        ]
    )

    assert results[0]["error_code"] == 38

    # Test 7:
    # Broker ID 2 does not exist in this single-broker setup.
    results = run_test(
        "Invalid manual broker assignment",
        [
            build_topic(
                "python_invalid_assignment",
                -1,
                -1,
                assignments=[
                    (0, [2])
                ]
            )
        ]
    )

    assert results[0]["error_code"] == 39

    print("\nAll CreateTopics v0 tests passed.")