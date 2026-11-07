#!/usr/bin/env python3
"""Two-process replication proof for redis-csharp.

Drives two ALREADY-RUNNING server instances (see replica-proof.ps1 for
the harness that starts them) through the replication feature end to
end, over raw RESP with nothing but the standard library:

  full mode:
    1. point the replica at the primary with REPLICAOF and verify the
       replica refuses writes with the READONLY error while serving reads
    2. load a mixed dataset on the primary (>= --keys keys: strings,
       hashes, lists, sets, sorted sets, some with TTLs)
    3. wait until the replica's applied offset converges with the
       primary's stream offset (INFO replication on both sides)
    4. compare the FULL keyspace: SCAN + per-type canonical dumps,
       hashed, on both servers
    5. issue a second wave of live writes (creates, updates, deletes,
       a short-TTL key that must expire on both sides), re-converge,
       and compare the full keyspace again

  verify mode: stages 3+4 only, against the dataset a previous full
  run loaded. Used after a primary kill/restart: the primary comes back
  with a fresh replication id, so the replica must full-resync and the
  keyspaces must still end up identical.

Every stage prints "STAGE <name>: PASS|FAIL ..."; the last line is
"PROOF RESULT: PASS|FAIL" and the exit code is 0 only on full success.
"""

import argparse
import hashlib
import socket
import sys
import time


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
            raise ConnectionError("server closed the connection")
        return line.rstrip(b"\r\n")

    def _reply(self):
        prefix = self.file.read(1)
        if not prefix:
            raise ConnectionError("server closed the connection")
        if prefix == b"+":
            return self._line().decode("utf-8", "replace")
        if prefix == b"-":
            return RespError(self._line().decode("utf-8", "replace"))
        if prefix == b":":
            return int(self._line())
        if prefix == b"$":
            length = int(self._line())
            if length == -1:
                return None
            data = self.file.read(length)
            self.file.read(2)
            return data
        if prefix == b"*":
            count = int(self._line())
            if count == -1:
                return None
            return [self._reply() for _ in range(count)]
        raise ValueError("unexpected reply prefix %r" % prefix)

    def call(self, *args):
        """One command; an error reply comes back as a RespError value."""
        self.sock.sendall(encode_command(args))
        return self._reply()

    def cmd(self, *args):
        """One command; an error reply raises."""
        reply = self.call(*args)
        if isinstance(reply, RespError):
            raise reply
        return reply

    def pipeline(self, commands):
        self.sock.sendall(b"".join(encode_command(c) for c in commands))
        return [self._reply() for _ in commands]


def info(client, section="replication"):
    raw = client.cmd("INFO", section).decode("utf-8", "replace")
    parsed = {}
    for line in raw.splitlines():
        if line and not line.startswith("#") and ":" in line:
            key, value = line.split(":", 1)
            parsed[key.strip()] = value.strip()
    return parsed


RESULTS = []


def stage(name, ok, detail=""):
    RESULTS.append(ok)
    print("STAGE %s: %s%s" % (name, "PASS" if ok else "FAIL",
                              (" " + detail) if detail else ""), flush=True)
    return ok


def wait_converged(primary, replica, timeout):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        p = info(primary)
        r = info(replica)
        last = (p.get("master_replid"), p.get("master_repl_offset"),
                r.get("master_repl_offset"), r.get("master_link_status"))
        if (r.get("master_link_status") == "up"
                and p.get("master_replid") == r.get("master_replid")
                and p.get("master_repl_offset") == r.get("master_repl_offset")):
            return True, last
        time.sleep(0.25)
    return False, last


def load_dataset(primary, total_keys):
    n_str = total_keys * 6 // 10
    n_each = total_keys // 10
    commands = []
    for i in range(n_str):
        commands.append(("SET", "proof:str:%d" % i, "value-%d" % i))
        if i % 10 == 0:
            commands.append(("EXPIRE", "proof:str:%d" % i, 3600))
    for i in range(n_each):
        commands.append(("HSET", "proof:hash:%d" % i,
                         "f0", "v%d-0" % i, "f1", "v%d-1" % i,
                         "f2", "v%d-2" % i, "f3", "v%d-3" % i))
        members = []
        for j in range(5):
            members.append("elem-%d-%d" % (i, j))
        commands.append(("RPUSH", "proof:list:%d" % i, *members))
        commands.append(("SADD", "proof:set:%d" % i, *members))
        zargs = []
        for j in range(5):
            zargs.append(str(j * 1.5))
            zargs.append("member-%d-%d" % (i, j))
        commands.append(("ZADD", "proof:zset:%d" % i, *zargs))
    errors = 0
    batch = 250
    for start in range(0, len(commands), batch):
        for reply in primary.pipeline(commands[start:start + batch]):
            if isinstance(reply, RespError):
                errors += 1
    return len(commands), errors


def canonical_dump(client, match="proof:*"):
    keys = []
    cursor = b"0"
    while True:
        cursor, batch = client.cmd("SCAN", cursor, "MATCH", match, "COUNT", 500)
        keys.extend(batch)
        if cursor == b"0":
            break
    keys = sorted(set(keys))

    dump = {}
    chunk = 200
    for start in range(0, len(keys), chunk):
        part = keys[start:start + chunk]
        probe = []
        for key in part:
            probe.append(("TYPE", key))
            probe.append(("PTTL", key))
        probe_replies = client.pipeline(probe)
        meta = {}
        reads = []
        read_keys = []
        for idx, key in enumerate(part):
            typ = probe_replies[2 * idx]
            pttl = probe_replies[2 * idx + 1]
            if typ == "none" or pttl == -2:
                continue  # expired between scan and probe; gone on both sides by read time
            meta[key] = (typ, pttl > 0)
            read_keys.append(key)
            if typ == "string":
                reads.append(("GET", key))
            elif typ == "list":
                reads.append(("LRANGE", key, 0, -1))
            elif typ == "set":
                reads.append(("SMEMBERS", key))
            elif typ == "hash":
                reads.append(("HGETALL", key))
            elif typ == "zset":
                reads.append(("ZRANGE", key, 0, -1, "WITHSCORES"))
        payloads = client.pipeline(reads) if reads else []
        for key, payload in zip(read_keys, payloads):
            typ, has_ttl = meta[key]
            if typ == "string":
                body = payload
            elif typ == "list":
                body = tuple(payload)
            elif typ == "set":
                body = tuple(sorted(payload))
            elif typ == "hash":
                body = tuple(sorted(zip(payload[::2], payload[1::2])))
            elif typ == "zset":
                body = tuple(
                    (payload[j], repr(float(payload[j + 1])))
                    for j in range(0, len(payload), 2))
            else:
                body = payload
            record = repr((typ, has_ttl, body)).encode()
            dump[key] = hashlib.sha256(record).hexdigest()
    return dump


def compare_keyspaces(primary, replica):
    left = canonical_dump(primary)
    right = canonical_dump(replica)
    if left == right:
        return True, "%d keys identical" % len(left)
    details = []
    only_left = sorted(set(left) - set(right))
    only_right = sorted(set(right) - set(left))
    differ = sorted(k for k in set(left) & set(right) if left[k] != right[k])
    if only_left:
        details.append("only on primary: %s" % [k.decode() for k in only_left[:5]])
    if only_right:
        details.append("only on replica: %s" % [k.decode() for k in only_right[:5]])
    if differ:
        details.append("content differs: %s" % [k.decode() for k in differ[:5]])
    return False, "; ".join(details)


def live_write_wave(primary):
    primary.cmd("SET", "proof:live:new", "born-after-sync")
    for _ in range(3):
        primary.cmd("INCR", "proof:live:counter")
    primary.cmd("LPUSH", "proof:list:0", "injected-head")
    primary.cmd("ZADD", "proof:zset:0", "99.5", "member-0-0")
    primary.cmd("HSET", "proof:hash:0", "f9", "late-field")
    primary.cmd("SADD", "proof:set:0", "late-member")
    primary.cmd("DEL", "proof:str:1", "proof:hash:1", "proof:list:1",
                "proof:set:1", "proof:zset:1")
    primary.cmd("PERSIST", "proof:str:0")  # had a TTL from the load wave
    primary.cmd("SET", "proof:expire:soon", "gone", "PX", 1200)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--primary-port", type=int, default=7771)
    parser.add_argument("--replica-port", type=int, default=7772)
    parser.add_argument("--keys", type=int, default=10000)
    parser.add_argument("--timeout", type=int, default=120,
                        help="per-convergence wait budget, seconds")
    parser.add_argument("--mode", choices=["full", "verify"], default="full")
    args = parser.parse_args()

    primary = Client(args.host, args.primary_port)
    replica = Client(args.host, args.replica_port)
    try:
        if args.mode == "full":
            reply = replica.cmd("REPLICAOF", args.host, args.primary_port)
            stage("replicaof", reply == "OK", "reply=%r" % reply)

            refused = replica.call("SET", "proof:readonly:probe", "x")
            stage("readonly", isinstance(refused, RespError)
                  and str(refused).startswith("READONLY"),
                  "reply=%r" % refused)
            readable = replica.call("PING")
            stage("replica-serves-reads", readable == "PONG", "reply=%r" % readable)

            count, errors = load_dataset(primary, args.keys)
            stage("load", errors == 0, "%d commands, %d errors" % (count, errors))

            ok, last = wait_converged(primary, replica, args.timeout)
            stage("converge-after-load", ok, "replid/offsets=%r" % (last,))
            if ok:
                same, detail = compare_keyspaces(primary, replica)
                stage("keyspace-after-load", same, detail)

                live_write_wave(primary)
                ok, last = wait_converged(primary, replica, args.timeout)
                stage("converge-after-writes", ok, "offsets=%r" % (last,))

                # The short-TTL key must be gone on BOTH sides (absolute
                # deadline propagated; each side expires it locally).
                time.sleep(2.5)
                gone_p = primary.cmd("TYPE", "proof:expire:soon") == "none"
                gone_r = replica.cmd("TYPE", "proof:expire:soon") == "none"
                stage("expiry-converged", gone_p and gone_r,
                      "primary_gone=%s replica_gone=%s" % (gone_p, gone_r))

                same, detail = compare_keyspaces(primary, replica)
                stage("keyspace-after-writes", same, detail)
        else:
            ok, last = wait_converged(primary, replica, args.timeout)
            stage("converge-after-restart", ok, "replid/offsets=%r" % (last,))
            if ok:
                same, detail = compare_keyspaces(primary, replica)
                stage("keyspace-after-restart", same, detail)
    finally:
        primary.close()
        replica.close()

    passed = all(RESULTS) and len(RESULTS) > 0
    print("PROOF RESULT: %s" % ("PASS" if passed else "FAIL"), flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
