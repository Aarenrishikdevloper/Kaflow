import socket
import struct
import sys

HOST = "127.0.0.1"
PORT = 9092

API_KEY = 75
API_VERSION = 0
CORRELATION_ID = 42
CLIENT_ID = "multi-topic-test"


# ---------- Compact protocol helpers ----------

def encode_varint(value):
    result = bytearray()

    while value >= 128:
        result.append((value & 0x7F) | 0x80)
        value >>= 7

    result.append(value)
    return bytes(result)


def compact_string(value):
    data = value.encode("utf-8")
    return encode_varint(len(data) + 1) + data


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read(self, size):
        if self.pos + size > len(self.data):
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
            raise ValueError("Invalid boolean")

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

        raise ValueError("Invalid varint")

    def compact_string(self):
        length = self.varint()

        if length == 0:
            raise ValueError("Unexpected null string")

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
            self.varint()
            size = self.varint()
            self.read(size)


# ---------- Build request ----------

def build_request(topic_names, limit=1000, cursor=None):
    """
    cursor: None, or a (topic_name, partition_index) tuple.

    Nullable struct encoding: 0xFF (-1) = null, 0x01 = present.
    """
    body = bytearray()

    body += encode_varint(len(topic_names) + 1)

    for name in topic_names:
        body += compact_string(name)
        body += b"\x00"  # Topic tagged fields

    body += struct.pack(">i", limit)  # Response partition limit

    if cursor is None:
        body += b"\xff"  # Null cursor
    else:
        cursor_topic, cursor_partition = cursor
        body += b"\x01"  # Cursor present
        body += compact_string(cursor_topic)
        body += struct.pack(">i", cursor_partition)
        body += b"\x00"  # Cursor tagged fields

    body += b"\x00"  # Body tagged fields

    header = bytearray()
    header += struct.pack(">h", API_KEY)
    header += struct.pack(">h", API_VERSION)
    header += struct.pack(">i", CORRELATION_ID)
    header += compact_string(CLIENT_ID)
    header += b"\x00"  # Header tagged fields

    payload = bytes(header + body)

    return struct.pack(">i", len(payload)) + payload


# ---------- Decode partition ----------

def parse_partition(reader):
    partition = {}

    partition["error"] = reader.i16()
    partition["index"] = reader.i32()
    partition["leader"] = reader.i32()
    partition["leader_epoch"] = reader.i32()

    count = reader.compact_array_length()
    if count is None:
        raise ValueError("Null replicas array")
    partition["replicas"] = [reader.i32() for _ in range(count)]

    count = reader.compact_array_length()
    if count is None:
        raise ValueError("Null ISR array")
    partition["isr"] = [reader.i32() for _ in range(count)]

    count = reader.compact_array_length()
    partition["elr"] = (
        None if count is None
        else [reader.i32() for _ in range(count)]
    )

    count = reader.compact_array_length()
    partition["last_known_elr"] = (
        None if count is None
        else [reader.i32() for _ in range(count)]
    )

    count = reader.compact_array_length()
    partition["offline"] = (
        None if count is None
        else [reader.i32() for _ in range(count)]
    )

    reader.tagged_fields()
    return partition


# ---------- Decode topic ----------

def parse_topic(reader):
    topic = {}

    topic["error"] = reader.i16()
    topic["name"] = reader.nullable_compact_string()
    topic["topic_id"] = reader.read(16).hex()
    topic["is_internal"] = reader.boolean()

    count = reader.compact_array_length()

    if count is None:
        raise ValueError("Null partitions array")

    topic["partitions"] = [parse_partition(reader) for _ in range(count)]

    topic["authorized_operations"] = reader.i32()
    reader.tagged_fields()

    return topic


# ---------- Decode response ----------

def parse_response(payload):
    reader = Reader(payload)

    response = {}

    response["correlation_id"] = reader.i32()
    reader.tagged_fields()  # Response header v1

    response["throttle_time"] = reader.i32()

    count = reader.compact_array_length()

    if count is None:
        raise ValueError("Null topics array")

    response["topics"] = [parse_topic(reader) for _ in range(count)]

    # Nullable cursor: 0xFF (-1) = null, 0x01 = present
    marker = reader.i8()
    response["next_cursor"] = None

    if marker == 1:
        response["next_cursor"] = {
            "topic": reader.compact_string(),
            "partition": reader.i32(),
        }
        reader.tagged_fields()
    elif marker != -1:
        raise ValueError(f"Invalid cursor marker {marker}")

    reader.tagged_fields()

    if reader.pos != len(payload):
        raise ValueError("Unexpected trailing response bytes")

    return response


# ---------- Send request ----------

def recv_exact(sock, size):
    result = bytearray()

    while len(result) < size:
        chunk = sock.recv(size - len(result))

        if not chunk:
            raise ConnectionError("Broker closed connection")

        result.extend(chunk)

    return bytes(result)


def describe_topics(topic_names, limit=1000, cursor=None):
    request = build_request(topic_names, limit, cursor)

    with socket.create_connection((HOST, PORT), timeout=5) as sock:
        sock.sendall(request)

        length = struct.unpack(">i", recv_exact(sock, 4))[0]

        if length < 0:
            raise ValueError("Invalid response length")

        payload = recv_exact(sock, length)

    return parse_response(payload)


# ---------- Tests ----------

def main():
    # Deliberately request topics in unsorted order.
    requested = ["users", "orders", "payments"]

    # Must match the seeded metadata in seed_test_topics().
    expected_partitions = {
        "orders": 3,
        "payments": 2,
        "users": 2,
    }

    print("Requested topics:", requested)

    response = describe_topics(requested)
    topics = response["topics"]

    returned_names = [topic["name"] for topic in topics]

    print("\nReturned topic order:", returned_names)

    # Test 1: Correlation ID
    assert response["correlation_id"] == CORRELATION_ID
    print("PASS: Correlation ID")

    # Test 2: Alphabetical sorting
    assert returned_names == sorted(requested), (
        f"Topics are not alphabetically sorted: {returned_names}"
    )
    print("PASS: Alphabetical topic sorting")

    # Test 3: No missing or duplicate topics
    assert len(returned_names) == len(requested)
    assert len(set(returned_names)) == len(requested)
    assert set(returned_names) == set(requested)
    print("PASS: All topics returned exactly once")

    # Test 4: Topic and partition metadata
    for topic in topics:
        name = topic["name"]

        assert topic["error"] == 0, f"{name}: topic error {topic['error']}"

        partitions = topic["partitions"]
        expected_count = expected_partitions[name]

        assert len(partitions) == expected_count, (
            f"{name}: expected {expected_count} partitions, "
            f"got {len(partitions)}"
        )

        indexes = [p["index"] for p in partitions]

        assert sorted(indexes) == list(range(expected_count)), (
            f"{name}: incorrect partition indexes {indexes}"
        )

        assert len(set(indexes)) == len(indexes), (
            f"{name}: duplicate partition indexes"
        )

        for partition in partitions:
            assert partition["error"] == 0

        print(
            f"PASS: {name} - {len(partitions)} partitions, "
            f"indexes={sorted(indexes)}"
        )

    # Test 5: No pagination expected with limit 1000
    assert response["next_cursor"] is None
    print("PASS: No unexpected pagination cursor")

    # Test 6: Pagination. Sorted order is orders(3), payments(2), users(2).
    # With limit 3, page 1 holds all of orders and the cursor points at
    # payments partition 0.
    page1 = describe_topics(requested, limit=3)
    page1_names = [t["name"] for t in page1["topics"]]

    assert page1_names == ["orders"], f"Unexpected page 1 topics: {page1_names}"
    assert len(page1["topics"][0]["partitions"]) == 3
    assert page1["next_cursor"] == {"topic": "payments", "partition": 0}, (
        f"Unexpected page 1 cursor: {page1['next_cursor']}"
    )
    print("PASS: Pagination page 1 (orders, cursor=payments:0)")

    # Page 2 resumes from the cursor: payments(2) + users(1) hits limit 3,
    # so the cursor should point at users partition 1.
    cursor = (page1["next_cursor"]["topic"], page1["next_cursor"]["partition"])
    page2 = describe_topics(requested, limit=3, cursor=cursor)
    page2_names = [t["name"] for t in page2["topics"]]

    assert page2_names == ["payments", "users"], (
        f"Unexpected page 2 topics: {page2_names}"
    )
    assert len(page2["topics"][0]["partitions"]) == 2
    assert len(page2["topics"][1]["partitions"]) == 1
    assert page2["next_cursor"] == {"topic": "users", "partition": 1}, (
        f"Unexpected page 2 cursor: {page2['next_cursor']}"
    )
    print("PASS: Pagination page 2 (payments, users[0], cursor=users:1)")

    print("\nALL MULTI-TOPIC TESTS PASSED")


if __name__ == "__main__":
    try:
        main()
    except (AssertionError, OSError, ValueError,
            ConnectionError, struct.error) as error:
        print("\nTEST FAILED:", error)
        sys.exit(1)