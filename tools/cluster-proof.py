#!/usr/bin/env python3
"""Three-node cluster proof for redis-csharp.

Drives three ALREADY-RUNNING cluster-enabled instances (see
cluster-proof.ps1 for the harness that starts them) over raw RESP
with nothing but the standard library:

  1. topology: CLUSTER INFO/SLOTS on every node agree: state ok,
     three known nodes, the slot map covers 0..16383 exactly once
  2. routing: a MOVED-following client writes --keys keys; every
     write's final node must be the node the local CRC16 slot math
     (the same function pinned against real Redis) says owns the key,
     and the per-node DBSIZE values must sum to the key count
  3. wrong node: a write sent to a non-owner without following must
     come back MOVED with the owner's exact address and slot; the
     follow-up write to that address succeeds
  4. cross-slot MULTI: a queued multi-key command spanning two slots
     is refused at EXEC with CROSSSLOT and changes nothing
  5. keyspace union: SCAN on all three nodes returns every written
     key exactly once, each on its computed owner node

Every stage prints "STAGE <name>: PASS|FAIL ..."; the last line is
"PROOF RESULT: PASS|FAIL" and the exit code is 0 only on full success.
"""

import argparse
import socket
import sys

# --- CRC16/XMODEM slot math (identical to Redis Cluster) ---

_CRC_TABLE = []
for _i in range(256):
    _crc = _i << 8
    for _ in range(8):
        _crc = ((_crc << 1) ^ 0x1021) & 0xFFFF if _crc & 0x8000 else (_crc << 1) & 0xFFFF
    _CRC_TABLE.append(_crc)


def crc16(data):
    crc = 0
    for b in data:
        crc = ((crc << 8) ^ _CRC_TABLE[((crc >> 8) ^ b) & 0xFF]) & 0xFFFF
    return crc


def key_slot(key):
    raw = key.encode() if isinstance(key, str) else key
    open_at = raw.find(b"{")
    if open_at >= 0:
        close_at = raw.find(b"}", open_at + 1)
        if close_at > open_at + 1:
            raw = raw[open_at + 1:close_at]
    return crc16(raw) % 16384


# --- minimal RESP client ---

class RespError(Exception):
    pass


def encode_command(args):
    parts = [b"*%d\r\n" % len(args)]
    for arg in args:
        if isinstance(arg, str):
            arg = arg.encode()
        elif isinstance(arg, int):
            arg = str(arg).encode()
        parts.append(b"$%d\r\n%s\r\n" % (len(arg), arg))
    return b"".join(parts)


class Client:
    def __init__(self, host, port, timeout=60):
        self.host = host
        self.port = port
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self.file = self.sock.makefile("rb")

    def close(self):
        try:
            self.file.close()
            self.sock.close()
        except OSError:
            pass

    def _line(self):
        line = self.file.readline()
        if not line:
            raise ConnectionError("connection closed")
        return line.rstrip(b"\r\n")

    def _reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            raise RespError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            size = int(rest)
            if size < 0:
                return None
            data = self.file.read(size)
            self.file.read(2)
            return data.decode()
        if kind in (b"*", b"%"):
            count = int(rest)
            if count < 0:
                return None
            items = [self._reply() for _ in range(count)]
            if kind == b"%":
                return {items[i]: items[i + 1] for i in range(0, len(items), 2)}
            return items
        raise RespError(f"unexpected reply byte {kind!r}")

    def call(self, *args):
        self.sock.sendall(encode_command(args))
        return self._reply()


class ClusterClient:
    """A client that follows MOVED redirects across the three nodes."""

    def __init__(self, nodes):
        self.nodes = nodes  # list of (host, port)
        self.clients = {}

    def _client(self, index):
        if index not in self.clients:
            host, port = self.nodes[index]
            self.clients[index] = Client(host, port)
        return self.clients[index]

    def owner_index(self, key):
        slot = key_slot(key)
        for index, (start, end) in enumerate(RANGES):
            if start <= slot <= end:
                return index
        raise AssertionError(f"slot {slot} unowned")

    def call(self, *args, key=None):
        index = 0
        for _ in range(8):
            try:
                return self._client(index).call(*args), index
            except RespError as error:
                text = str(error)
                if not text.startswith("MOVED "):
                    raise
                _, _slot, address = text.split(" ", 2)
                host, port = address.rsplit(":", 1)
                index = self.nodes.index((host, int(port)))
        raise RespError("too many MOVED redirects")


RANGES = [(0, 5460), (5461, 10922), (10923, 16383)]
_failures = []


def stage(name, ok, detail=""):
    print(f"STAGE {name}: {'PASS' if ok else 'FAIL'} {detail}")
    if not ok:
        _failures.append(name)


def parse_info(text):
    fields = {}
    for line in text.splitlines():
        if ":" in line:
            name, value = line.split(":", 1)
            fields[name] = value
    return fields


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ports", default="7411,7412,7413",
                        help="comma-separated node ports, in slot-range order")
    parser.add_argument("--keys", type=int, default=300)
    args = parser.parse_args()

    ports = [int(p) for p in args.ports.split(",")]
    if len(ports) != 3:
        print("PROOF RESULT: FAIL (exactly three node ports are required)")
        return 1
    nodes = [("127.0.0.1", p) for p in ports]
    direct = [Client(host, port) for host, port in nodes]
    cluster = ClusterClient(nodes)

    try:
        # 1. Topology agreement.
        ok = True
        maps = []
        for client in direct:
            info = parse_info(client.call("CLUSTER", "INFO"))
            ok = ok and info.get("cluster_state") == "ok"
            ok = ok and info.get("cluster_known_nodes") == "3"
            ok = ok and info.get("cluster_slots_assigned") == "16384"
            slots = client.call("CLUSTER", "SLOTS")
            maps.append([(entry[0], entry[1], entry[2][0], entry[2][1]) for entry in slots])
        ok = ok and maps[0] == maps[1] == maps[2]
        covered = sorted((s, e) for s, e, _, _ in maps[0])
        ok = ok and covered == RANGES
        stage("topology", ok, f"map={covered}")

        # 2. Routed writes land on the computed owner.
        landed = {}
        ok = True
        for i in range(args.keys):
            key = f"clusterproof:{i}"
            _, final_index = cluster.call("SET", key, f"value-{i}", key=key)
            expected = cluster.owner_index(key)
            if final_index != expected:
                ok = False
                print(f"  key {key} landed on node {final_index}, owner is {expected}")
            landed[key] = expected
        sizes = [client.call("DBSIZE") for client in direct]
        ok = ok and sum(sizes) == args.keys
        stage("routing", ok, f"dbsize per node={sizes} total={sum(sizes)}")

        # Read-back through the redirecting client returns the values.
        ok = True
        for i in range(0, args.keys, 37):
            key = f"clusterproof:{i}"
            value, _ = cluster.call("GET", key, key=key)
            ok = ok and value == f"value-{i}"
        stage("readback", ok)

        # 3. Wrong node gets a followable MOVED with the exact owner.
        probe_key = "clusterproof:wrong-node"
        owner = cluster.owner_index(probe_key)
        wrong = (owner + 1) % 3
        try:
            direct[wrong].call("SET", probe_key, "x")
            stage("moved", False, "write to a non-owner was not redirected")
        except RespError as error:
            expected = f"MOVED {key_slot(probe_key)} 127.0.0.1:{ports[owner]}"
            ok = str(error) == expected
            value = direct[owner].call("SET", probe_key, "x")
            ok = ok and value == "OK" and direct[owner].call("GET", probe_key) == "x"
            stage("moved", ok, f"reply={error}")

        # 4. Cross-slot multi-key command inside MULTI is refused.
        key_a, key_b = "clusterproof:{alpha}1", None
        for i in range(1000):
            candidate = f"clusterproof:beta{i}"
            if key_slot(candidate) != key_slot(key_a):
                key_b = candidate
                break
        cluster.call("SET", key_a, "a", key=key_a)
        cluster.call("SET", key_b, "b", key=key_b)
        conn = direct[0]
        conn.call("MULTI")
        queued = conn.call("DEL", key_a, key_b)
        exec_error = None
        try:
            conn.call("EXEC")
        except RespError as error:
            # An error element inside the EXEC array surfaces here: the
            # refusal proof is its text plus the keys being untouched.
            exec_error = str(error)
        refused = exec_error is not None and "CROSSSLOT" in exec_error
        still_there = (cluster.call("GET", key_a, key=key_a)[0] == "a" and
                       cluster.call("GET", key_b, key=key_b)[0] == "b")
        stage("crossslot-multi", bool(queued == "QUEUED" and refused and still_there),
              f"queued={queued} exec_error={exec_error} keys_intact={still_there}")

        # 5. Keyspace union: every key exactly once, on its owner.
        seen = {}
        ok = True
        for index, client in enumerate(direct):
            cursor = "0"
            while True:
                cursor, batch = client.call("SCAN", cursor, "MATCH", "clusterproof:*", "COUNT", "500")
                for key in batch:
                    if key in seen:
                        ok = False
                        print(f"  key {key} on two nodes: {seen[key]} and {index}")
                    seen[key] = index
                    if cluster.owner_index(key) != index:
                        ok = False
                        print(f"  key {key} sits on node {index}, owner is {cluster.owner_index(key)}")
                if cursor == "0":
                    break
        expected_keys = {f"clusterproof:{i}" for i in range(args.keys)} | {probe_key, key_a, key_b}
        ok = ok and set(seen) == expected_keys
        stage("keyspace-union", ok, f"scanned={len(seen)} expected={len(expected_keys)}")
    finally:
        for client in direct:
            client.close()
        for client in cluster.clients.values():
            client.close()

    print("PROOF RESULT: " + ("PASS" if not _failures else "FAIL"))
    return 0 if not _failures else 1


if __name__ == "__main__":
    sys.exit(main())
