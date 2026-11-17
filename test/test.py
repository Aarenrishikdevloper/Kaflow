
import socket
import struct


HOST = "127.0.0.1"
PORT = 9092


# ------------------------------------------------------------
# Kafka encoding helpers
# ------------------------------------------------------------

def int16(value):
    return struct.pack(">h", value)


def int32(value):
    return struct.pack(">i", value)


def kafka_string(value):
    encoded = value.encode("utf-8")
    return int16(len(encoded)) + encoded


def read_exact(sock, count):
    """Read exactly count bytes from the socket."""

    result = b""

    while len(result) < count:
        chunk = sock.recv(count - len(result))

        if not chunk:
            raise RuntimeError("Connection closed unexpectedly")

        result += chunk

    return result


# ------------------------------------------------------------
# Send one Kafka request and receive one response
# ------------------------------------------------------------

def send_request(api_key, api_version, correlation_id, body):
    # Legacy Kafka request header v1:
    # api_key + api_version + correlation_id + client_id.
    header = (
        int16(api_key)
        + int16(api_version)
        + int32(correlation_id)
        + kafka_string("python-delete-test")
    )

    # Combine header and body.
    payload = header + body

    # Kafka TCP frame starts with a 4-byte payload length.
    frame = int32(len(payload)) + payload

    with socket.create_connection((HOST, PORT)) as sock:
        # Send the complete request.
        sock.sendall(frame)

        # Read the 4-byte response length.
        response_size = struct.unpack(
            ">i", read_exact(sock, 4)
        )[0]

        # Read the complete response payload.
        return read_exact(sock, response_size)


# ------------------------------------------------------------
# CreateTopics API v0
# ------------------------------------------------------------

def create_topic(name, correlation_id):
    body = (
        int32(1)             # One topic
        + kafka_string(name)
        + int32(1)            # One partition
        + int16(1)            # Replication factor = 1
        + int32(0)            # No manual assignments
        + int32(0)            # No configs
        + int32(5000)         # Timeout
    )

    response = send_request(
        api_key=19,
        api_version=0,
        correlation_id=correlation_id,
        body=body,
    )

    # Response header: correlation ID.
    received_correlation = struct.unpack(">i", response[:4])[0]
    assert received_correlation == correlation_id

    # Response body begins with the result array count.
    count = struct.unpack(">i", response[4:8])[0]
    assert count == 1

    # Read topic name and error code.
    name_length = struct.unpack(">h", response[8:10])[0]
    returned_name = response[10:10 + name_length].decode()

    error_offset = 10 + name_length
    error_code = struct.unpack(
        ">h", response[error_offset:error_offset + 2]
    )[0]

    assert returned_name == name
    assert error_code == 0, f"CreateTopics failed: {error_code}"


# ------------------------------------------------------------
# DeleteTopics API v0
# ------------------------------------------------------------

def delete_topics(names, correlation_id):
    # Request array starts with the number of topic names.
    body = int32(len(names))

    # Append each topic name as a Kafka STRING.
    for name in names:
        body += kafka_string(name)

    # Append timeout_ms.
    body += int32(5000)

    response = send_request(
        api_key=20,
        api_version=0,
        correlation_id=correlation_id,
        body=body,
    )

    # Check the response correlation ID.
    received_correlation = struct.unpack(">i", response[:4])[0]
    assert received_correlation == correlation_id

    # Read the number of topic results.
    count = struct.unpack(">i", response[4:8])[0]

    assert count == len(names)

    # Parse every topic result.
    offset = 8
    results = []

    for _ in range(count):
        # Read topic name length.
        name_length = struct.unpack(
            ">h", response[offset:offset + 2]
        )[0]

        offset += 2

        # Read topic name.
        name = response[offset:offset + name_length].decode()

        offset += name_length

        # Read topic error code.
        error_code = struct.unpack(
            ">h", response[offset:offset + 2]
        )[0]

        offset += 2

        results.append((name, error_code))

    # Make sure we consumed the entire response.
    assert offset == len(response)

    return results


# ------------------------------------------------------------
# Tests
# ------------------------------------------------------------

def test_delete_existing_topic():
    name = "delete_existing_test"

    create_topic(name, 1001)

    results = delete_topics([name], 1002)

    assert results == [(name, 0)]

    print("PASS: Delete existing topic")


def test_delete_unknown_topic():
    name = "topic_that_does_not_exist"

    results = delete_topics([name], 1003)

    assert results == [(name, 3)]

    print("PASS: Delete unknown topic")


def test_delete_multiple_topics():
    names = ["delete_multi_a", "delete_multi_b"]

    create_topic(names[0], 1004)
    create_topic(names[1], 1005)

    results = delete_topics(names, 1006)

    assert results == [
        (names[0], 0),
        (names[1], 0),
    ]

    print("PASS: Delete multiple topics")


def test_delete_mixed_topics():
    existing = "delete_mixed_existing"
    missing = "delete_mixed_missing"

    create_topic(existing, 1007)

    results = delete_topics(
        [existing, missing],
        1008,
    )

    assert results == [
        (existing, 0),
        (missing, 3),
    ]

    print("PASS: Delete existing and unknown topics together")


def test_delete_invalid_topic():
    name = "invalid/topic"

    results = delete_topics([name], 1009)

    assert results == [(name, 17)]

    print("PASS: Reject invalid topic name")


def test_delete_empty_array():
    results = delete_topics([], 1010)

    assert results == []

    print("PASS: DeleteTopics with empty topic array")


if __name__ == "__main__":
    test_delete_existing_topic()
    test_delete_unknown_topic()
    test_delete_multiple_topics()
    test_delete_mixed_topics()
    test_delete_invalid_topic()
    test_delete_empty_array()

    print("\nAll DeleteTopics tests passed!")

