import socket, struct

HOST, PORT = "127.0.0.1", 9092
CORR = 42
CID = b"test-client"

def build(topics):
    hdr = struct.pack(">hhih", 3, 0, CORR, len(CID)) + CID
    body = struct.pack(">i", len(topics))
    for t in topics:
        t = t.encode()
        body += struct.pack(">h", len(t)) + t
    p = hdr + body
    return struct.pack(">i", len(p)) + p

def recv_exact(s, n):
    b = b""
    while len(b) < n:
        c = s.recv(n - len(b))
        if not c:
            raise SystemExit(f"closed after {len(b)}/{n} bytes")
        b += c
    return b

class R:
    def __init__(self, d): self.d, self.o = d, 0
    def i16(self): v = struct.unpack_from(">h", self.d, self.o)[0]; self.o += 2; return v
    def i32(self): v = struct.unpack_from(">i", self.d, self.o)[0]; self.o += 4; return v
    def s(self):
        n = self.i16(); v = self.d[self.o:self.o+n].decode(); self.o += n; return v
    def arr(self): return [self.i32() for _ in range(self.i32())]

def metadata(topics):
    print(f"\n=== request: {topics or 'ALL'} ===")
    with socket.create_connection((HOST, PORT), timeout=5) as s:
        s.sendall(build(topics))
        size = struct.unpack(">i", recv_exact(s, 4))[0]
        data = recv_exact(s, size)
    r = R(data)
    assert r.i32() == CORR, "correlation id mismatch"
    for _ in range(r.i32()):
        print(f"broker node_id={r.i32()} host={r.s()} port={r.i32()}")
    for _ in range(r.i32()):
        err, name = r.i16(), r.s()
        print(f"topic {name} err={err}")
        for _ in range(r.i32()):
            perr, idx, leader = r.i16(), r.i32(), r.i32()
            print(f"   p{idx} err={perr} leader={leader} replicas={r.arr()} isr={r.arr()}")
    assert r.o == len(data), f"{len(data) - r.o} unread bytes"
    print("OK, no leftover bytes")

metadata([])
metadata(["orders"])
metadata(["nonexistent"])