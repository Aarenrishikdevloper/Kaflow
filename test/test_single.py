import socket
import struct
import sys


HOST = "127.0.0.1"
PORT = 9092

API_KEY = 75
API_VERSION = 0
CORRELATION_ID = 42
CLIENT_ID = "pagination-test"

# Must match seed_test_topics() in the broker.
EXPECTED_PARTITIONS = {
    "orders": 3,
    "payments": 2,
    "users": 2,
}

# Sorted topic order expected from the broker.
TOPICS = sorted(EXPECTED_PARTITIONS)

# Kafka nullable struct marker bytes.
CURSOR_NULL = b"\xff"     # -1
CURSOR_PRESENT = b"\x01"


# ==================================================
# 1. Compact protocol helpers
# ==================================================

def encode_varint(value):
    if value < 0:
        raise ValueError("Varint cannot be negative")

    result = bytearray()

    while value >= 128:
        result.append((value & 0x7F) | 0x80)
        value >>= 7

    result.append(value)
    return bytes(result)


def compact_string(value):
    encoded = value.encode("utf-8")
    return encode_varint(len(encoded) + 1) + encoded


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read(self, size):
        if size < 0 or self.pos + size > len(self.data):
            raise ValueError("Unexpected end of response")

        result = self.data[self.pos:self.pos + size]
        self.pos += size
        return result

    def i8(self):
        return struct.unpack(">b", self.read(1))[0]

    def i16(self):
        return struct.unpack(">h", self.read(2))[0]

    def i32(self):
        return struct.unpack(">i", self.read(4))[0]

    def boolean(self):
        value = self.read(1)[0]

        if value not in (0, 1):
            raise ValueError(f"Invalid boolean: {value}")

        return value == 1

    def varint(self):
        result = 0
        shift = 0

        for _ in range(5):
            byte = self.read(1)[0]
            result |= (byte & 0x7F) << shift

            if byte < 128:
                return result

            shift += 7

        raise ValueError("Invalid compact varint")

    def compact_string(self):
        length = self.varint()

        if length == 0:
            raise ValueError("Unexpected null compact string")

        return self.read(length - 1).decode("utf-8")

    def nullable_compact_string(self):
        length = self.varint()

        if length == 0:
            return None

        return self.read(length - 1).decode("utf-8")

    def compact_array_length(self):
        length = self.varint()

        if length == 0:
            return None

        return length - 1

    def tagged_fields(self):
        count = self.varint()

        for _ in range(count):
            self.varint()  # Tag ID
            size = self.varint()
            self.read(size)


# ==================================================
# 2. Build DescribeTopicPartitions request
# ==================================================

def build_request(topic_names, partition_limit=1000, cursor=None):
    body = bytearray()

    # Compact topic array.
    body += encode_varint(len(topic_names) + 1)

    for name in topic_names:
        body += compact_string(name)
        body += b"\x00"  # Topic tagged fields.

    # Response partition limit.
    body += struct.pack(">i", partition_limit)

    # Nullable cursor: 0xFF = null, 0x01 = present.
    if cursor is None:
        body += CURSOR_NULL
    else:
        body += CURSOR_PRESENT
        body += compact_string(cursor["topic"])
        body += struct.pack(">i", cursor["partition"])
        body += b"\x00"  # Cursor tagged fields.

    # Request tagged fields.
    body += b"\x00"

    # Flexible request header v2.
    header = bytearray()
    header += struct.pack(">h", API_KEY)
    header += struct.pack(">h", API_VERSION)
    header += struct.pack(">i", CORRELATION_ID)
    header += compact_string(CLIENT_ID)
    header += b"\x00"  # Header tagged fields.

    payload = bytes(header + body)

    # Kafka frame length prefix.
    return struct.pack(">i", len(payload)) + payload


# ==================================================
# 3. Parse partition metadata
# ==================================================

def read_int32_array(reader, nullable):
    count = reader.compact_array_length()

    if count is None:
        if nullable:
            return None
        raise ValueError("Array cannot be null")

    return [reader.i32() for _ in range(count)]


def parse_partition(reader):
    partition = {}

    partition["error"] = reader.i16()
    partition["index"] = reader.i32()
    partition["leader"] = reader.i32()
    partition["leader_epoch"] = reader.i32()

    partition["replicas"] = read_int32_array(reader, nullable=False)
    partition["isr"] = read_int32_array(reader, nullable=False)
    partition["eligible_leader_replicas"] = read_int32_array(
        reader, nullable=True
    )
    partition["last_known_elr"] = read_int32_array(reader, nullable=True)
    partition["offline_replicas"] = read_int32_array(reader, nullable=True)

    reader.tagged_fields()

    return partition


# ==================================================
# 4. Parse topic metadata
# ==================================================

def parse_topic(reader):
    topic = {}

    topic["error"] = reader.i16()
    topic["name"] = reader.nullable_compact_string()
    topic["topic_id"] = reader.read(16).hex()
    topic["is_internal"] = reader.boolean()

    count = reader.compact_array_length()

    if count is None:
        raise ValueError("Partitions array cannot be null")

    topic["partitions"] = [
        parse_partition(reader)
        for _ in range(count)
    ]

    topic["authorized_operations"] = reader.i32()

    reader.tagged_fields()

    return topic


# ==================================================
# 5. Parse complete response
# ==================================================

def parse_response(payload):
    reader = Reader(payload)

    response = {}

    response["correlation_id"] = reader.i32()

    # Flexible response header v1.
    reader.tagged_fields()

    response["throttle_time"] = reader.i32()

    count = reader.compact_array_length()

    if count is None:
        raise ValueError("Topics array cannot be null")

    response["topics"] = [
        parse_topic(reader)
        for _ in range(count)
    ]

    # Nullable cursor: 0xFF (-1) = null, 0x01 = present.
    marker = reader.i8()
    response["next_cursor"] = None

    if marker == 1:
        response["next_cursor"] = {
            "topic": reader.compact_string(),
            "partition": reader.i32(),
        }

        reader.tagged_fields()
    elif marker != -1:
        raise ValueError(f"Invalid cursor marker: {marker}")

    reader.tagged_fields()

    if reader.pos != len(payload):
        raise ValueError(
            f"Unexpected trailing bytes: "
            f"{len(payload) - reader.pos}"
        )

    return response


# ==================================================
# 6. TCP communication
# ==================================================

def recv_exact(sock, size):
    result = bytearray()

    while len(result) < size:
        chunk = sock.recv(size - len(result))

        if not chunk:
            raise ConnectionError("Broker closed connection")

        result.extend(chunk)

    return bytes(result)


def describe_topics(topic_names, partition_limit=1000, cursor=None):
    request = build_request(
        topic_names,
        partition_limit,
        cursor
    )

    with socket.create_connection(
        (HOST, PORT), timeout=5
    ) as sock:

        sock.sendall(request)

        response_length = struct.unpack(
            ">i", recv_exact(sock, 4)
        )[0]

        if response_length < 0:
            raise ValueError("Invalid response length")

        payload = recv_exact(sock, response_length)

    return parse_response(payload)


# ==================================================
# 7. Helpers for testing
# ==================================================

def flatten_partitions(response):
    """
    Convert a response into a list of (topic_name, partition_index).
    Topics with errors are excluded.
    """

    result = []

    for topic in response["topics"]:
        if topic["error"] != 0:
            continue

        for partition in topic["partitions"]:
            result.append((topic["name"], partition["index"]))

    return result


def expected_all_partitions():
    """
    Construct the expected globally sorted partition list.
    """

    result = []

    for topic in sorted(EXPECTED_PARTITIONS):
        for index in range(EXPECTED_PARTITIONS[topic]):
            result.append((topic, index))

    return result


def check(condition, message):
    if not condition:
        raise AssertionError(message)

    print("PASS:", message)


def print_page(page_number, response):
    print(f"\n========== PAGE {page_number} ==========")

    for topic in response["topics"]:
        indexes = [p["index"] for p in topic["partitions"]]

        print(
            f"{topic['name']}: "
            f"error={topic['error']}, "
            f"partitions={indexes}"
        )

    print("Next cursor:", response["next_cursor"])


# ==================================================
# 8. Test topic sorting and multiple topics
# ==================================================

def test_multiple_topics():
    print("\nTEST: Multiple topics and sorting")

    # Deliberately unsorted request.
    requested = ["users", "orders", "payments"]

    response = describe_topics(requested)

    returned = [topic["name"] for topic in response["topics"]]

    check(
        response["correlation_id"] == CORRELATION_ID,
        "Correlation ID is correct"
    )

    check(
        returned == sorted(requested),
        "Topics are alphabetically sorted"
    )

    check(
        len(returned) == len(set(returned))
        and set(returned) == set(requested),
        "All requested topics appear exactly once"
    )

    for topic in response["topics"]:
        name = topic["name"]

        check(topic["error"] == 0, f"{name} has no topic error")

        check(
            len(topic["partitions"]) == EXPECTED_PARTITIONS[name],
            f"{name} has the expected partition count"
        )

        indexes = sorted(p["index"] for p in topic["partitions"])

        check(
            indexes == list(range(EXPECTED_PARTITIONS[name])),
            f"{name} has correct partition indexes"
        )

        check(
            isinstance(topic["is_internal"], bool),
            f"{name} internal flag is a boolean"
        )


# ==================================================
# 9. Test pagination
# ==================================================

def test_pagination(limit):
    print(f"\nTEST: Pagination with limit = {limit}")

    expected = expected_all_partitions()

    cursor = None
    collected = []
    page_number = 0

    while True:
        page_number += 1

        if page_number > len(expected) + 2:
            raise AssertionError("Too many pagination pages")

        response = describe_topics(
            TOPICS,
            partition_limit=limit,
            cursor=cursor
        )

        check(
            response["correlation_id"] == CORRELATION_ID,
            f"Page {page_number}: correlation ID"
        )

        page_partitions = flatten_partitions(response)

        print_page(page_number, response)

        check(
            len(page_partitions) <= limit,
            f"Page {page_number}: partition limit respected"
        )

        expected_page = expected[len(collected):len(collected) + limit]

        check(
            page_partitions == expected_page,
            f"Page {page_number}: correct partition sequence"
        )

        for item in page_partitions:
            check(
                item not in collected,
                f"Page {page_number}: no duplicate {item}"
            )

        collected.extend(page_partitions)

        next_cursor = response["next_cursor"]

        if len(collected) == len(expected):
            check(next_cursor is None, "Final page has no next cursor")
            break

        check(
            next_cursor is not None,
            "Next cursor exists while partitions remain"
        )

        expected_next = expected[len(collected)]

        check(
            next_cursor == {
                "topic": expected_next[0],
                "partition": expected_next[1],
            },
            "Next cursor points to the next expected partition"
        )

        cursor = next_cursor

    check(
        collected == expected,
        f"All partitions collected with limit {limit}"
    )

    print(
        f"\nPagination completed in {page_number} pages "
        f"with limit {limit}."
    )


# ==================================================
# 10. Test unknown topic
# ==================================================

def test_unknown_topic():
    print("\nTEST: Unknown topic")

    response = describe_topics(["topic_does_not_exist"])

    check(
        len(response["topics"]) == 1,
        "Unknown topic response contains one topic"
    )

    topic = response["topics"][0]

    check(
        topic["name"] == "topic_does_not_exist",
        "Unknown topic name is preserved"
    )

    check(topic["error"] == 3, "Unknown topic returns error code 3")

    check(
        len(topic["partitions"]) == 0,
        "Unknown topic has no partitions"
    )


# ==================================================
# 11. Test empty topic list
# ==================================================

def test_empty_topics():
    print("\nTEST: Empty topics array")

    response = describe_topics([])

    check(
        response["topics"] == [],
        "Empty request returns empty topics array"
    )

    check(
        response["next_cursor"] is None,
        "Empty request has no next cursor"
    )


# ==================================================
# 12. Test large limit without pagination
# ==================================================

def test_no_pagination():
    print("\nTEST: Large partition limit")

    response = describe_topics(TOPICS, partition_limit=1000)

    check(
        flatten_partitions(response) == expected_all_partitions(),
        "Large limit returns every partition"
    )

    check(
        response["next_cursor"] is None,
        "Large limit returns null cursor"
    )


# ==================================================
# 13. Run everything
# ==================================================

def main():
    print("===================================")
    print(" Kafka DescribeTopicPartitions")
    print(" Pagination and Metadata Tests")
    print("===================================")

    print("Expected topics:", EXPECTED_PARTITIONS)

    test_multiple_topics()
    test_no_pagination()

    for limit in (1, 2, 3, 4):
        test_pagination(limit)

    test_unknown_topic()
    test_empty_topics()

    print("\n===================================")
    print("ALL TESTS PASSED")
    print("===================================")


if __name__ == "__main__":
    try:
        main()

    except (
        AssertionError,
        OSError,
        ValueError,
        ConnectionError,
        struct.error,
    ) as error:
        print("\nTEST FAILED:", error)
        sys.exit(1)