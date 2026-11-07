# Redis Server in C#

A complete, from-scratch implementation of a Redis-compatible server in C# on .NET 8 — the RESP2 and RESP3 wire protocols (`HELLO` negotiation), all five core data types, transactions, pub/sub, memory limits with eviction, and both of Redis's persistence models (snapshots and an append-only file), served over `System.IO.Pipelines`.

It speaks the real Redis wire protocol, so standard Redis clients can drive it.

## Why this project

Redis looks like "just a key/value store" from the outside, but building one properly touches the core of backend engineering:

- **Networking** — a concurrent TCP server speaking a binary-safe wire protocol
- **Protocol parsing** — incremental RESP parsing: pipelining, split packets, inline commands
- **Data structures** — lists, sets, hashes, and sorted sets with Redis's exact edge-case semantics
- **Concurrency** — many clients, one shared store, atomic composite commands and transactions
- **Durability** — snapshots with checksums, an append log with fsync policies, crash recovery

## Features

**Strings** — `SET` (with `EX` / `PX` / `NX` / `XX` / `GET` / `KEEPTTL`), `GET`, `DEL`, `EXISTS`, `INCR` / `DECR` / `INCRBY` / `DECRBY`, `APPEND`, `STRLEN`

**Keys** — `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` (with `NX` / `XX` / `GT` / `LT`), `TTL` / `PTTL`, `PERSIST`, `TYPE`, `KEYS`, `SCAN` (with `MATCH` / `COUNT` / `TYPE`), `FLUSHDB` / `FLUSHALL`

**Lists** — `LPUSH` / `RPUSH`, `LPOP` / `RPOP` (with count), `LLEN`, `LRANGE`, `LINDEX`

**Sets** — `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`

**Hashes** — `HSET`, `HGET`, `HDEL`, `HGETALL`, `HLEN`

**Sorted sets** — `ZADD` (with `NX` / `XX` / `GT` / `LT` / `CH` / `INCR`), `ZINCRBY`, `ZSCORE`, `ZRANGE` (with `WITHSCORES`), `ZREM`, `ZCARD`

**Transactions** — `MULTI` / `EXEC` / `DISCARD`, with queue-time validation (`EXECABORT`) and Redis's no-rollback semantics for runtime errors

**Pub/sub** — `SUBSCRIBE` / `UNSUBSCRIBE`, `PSUBSCRIBE` / `PUNSUBSCRIBE` (glob patterns), `PUBLISH`, restricted-mode command rules while subscribed

**Server** — `PING`, `ECHO`, `QUIT`, `DBSIZE`, `INFO` (server / clients / memory / persistence / stats / keyspace sections), `SAVE` / `BGSAVE`, `BGREWRITEAOF`

**Protocols** — `HELLO` with protocol negotiation (RESP2 default; RESP3 only after `HELLO 3`, per-connection), including its `AUTH` and `SETNAME` clauses and Redis's error order; the RESP3 reply shapes the command set needs (maps for `HGETALL`, sets, doubles, the single-character null, verbatim `INFO`, pub/sub frames as pushes). `CLIENT` — `SETNAME` / `GETNAME` / `SETINFO` / `ID`, so real client libraries connect without workarounds

**Memory** — `--maxmemory` ceiling with the eviction policies `noeviction` (with Redis's exact `OOM` error for writes; reads keep working), `allkeys-lru`, `allkeys-random`, `volatile-lru`, `volatile-random`, `volatile-ttl`, using Redis-style approximate sampling (five-candidate reservoir, access-sequence LRU). Evictions are logged to the AOF as `DEL` effects, exactly like client deletes

**Security** — `AUTH` (with `--requirepass`; per-connection state, Redis-exact `NOAUTH` / `WRONGPASS` behaviour probed against redis-server 7.0.15) and an optional TLS listener (`--tls-port` + a PKCS#12 server certificate) alongside the plaintext port, sharing the same store and command pipeline

**Behaviour**
- Expiry both ways Redis does it: lazy on access, plus an active sweep (100 ms maintenance loop)
- Empty collections delete their key; `SET` clears the TTL unless `KEEPTTL`; mutations preserve it
- Exact Redis error texts and edge semantics: `WRONGTYPE`, integer grammar (no leading zeros), overflow errors, `TTL` rounding, duplicate-counting `EXISTS`
- Pipelining, commands split across packets, inline (telnet-style) commands with Redis quoting

## Running it

```bash
dotnet run --project src/RedisServer
# Redis server listening on 127.0.0.1:6379
```

Options:

| Flag | Meaning | Default |
|------|---------|---------|
| `-p <port>` | TCP port to listen on | `6379` |
| `--dir <path>` | Directory holding the snapshot and AOF files | `.` |
| `--dbfilename <name>` | Snapshot file name | `dump.rcdb` |
| `--appendonly yes\|no` | Enable the append-only file | `no` |
| `--appendfilename <name>` | AOF file name | `appendonly.aof` |
| `--appendfsync always\|everysec\|no` | AOF fsync policy | `everysec` |
| `--requirepass <password>` | Require `AUTH` before any other command (or the `REDIS_REQUIREPASS` env var) | _(none)_ |
| `--maxmemory <bytes>` | Dataset memory ceiling; Redis size suffixes (`k` = 1000, `kb` = 1024, `m` / `mb`, `g` / `gb`) | `0` (no limit) |
| `--maxmemory-policy <name>` | Eviction policy at the ceiling (see Memory above; LFU policies are refused at startup) | `noeviction` |
| `--tls-port <port>` | Additional TLS listener port | `0` (off) |
| `--tls-cert-file <path>` | PKCS#12 file with the TLS server certificate and key | — |
| `--tls-cert-password <password>` | Certificate file password (or the `REDIS_TLS_CERT_PASSWORD` env var) | _(none)_ |

## Trying it out

Inline commands work over any raw TCP client:

```bash
$ nc 127.0.0.1 6379
SET greeting hello
+OK
INCR counter
:1
LPUSH jobs build test deploy
:3
LRANGE jobs 0 -1
*3
$6
deploy
$4
test
$5
build
```

…or point a real client at it — it speaks RESP exactly as Redis does, in both protocol versions.

## Architecture

```
src/RedisServer/
    Program.cs                     Generic-host wiring: options → DI → hosted services
    Options/ServerOptions.cs       -p / --dir / --appendonly / --appendfsync configuration
    Transport/TcpServer.cs         Loopback listener + accept loop (one session per connection), optional TLS listener
    Transport/StreamDuplexPipe.cs  NetworkStream ↔ IDuplexPipe adapter
    Protocol/RespParser.cs         Incremental RESP reader: arrays, bulk strings, inline commands
    Protocol/RespWriter.cs         RESP reply writers over IBufferWriter<byte>
    Protocol/RedisSession.cs       Per-connection pump: parse → execute → batched replies, pub/sub outbox
    Protocol/CommandProcessor.cs   Command semantics, MULTI/EXEC state, AOF effect logging
    Protocol/CommandProcessorCollections.cs   Collection + SCAN + pub/sub commands
    Protocol/PubSubHub.cs          Channel/pattern subscriptions and fan-out
    Core/RedisStore.cs             Strings + keys: one gate, lazy expiry, sweep, strict number grammar
    Core/RedisStoreCollections.cs  Lists, sets, hashes, sorted sets; state export for persistence
    Core/Glob.cs                   Redis glob matching (* ? [...] escapes) for KEYS/SCAN/PSUBSCRIBE
    Persistence/SnapshotFile.cs    RCDB snapshot format: typed entries, expiries, CRC32 trailer
    Persistence/AppendOnlyFile.cs  RESP-encoded append log + rewrite (compaction); streamed, bounded-memory replay
    Persistence/PersistenceManager.cs  Load precedence, SAVE/BGSAVE, rewrite orchestration
    Hosting/                       Listener lifetime + 100 ms maintenance (sweep, everysec fsync)
tests/RedisServer.Tests/
    StoreTests.cs / CollectionTests.cs   Store semantics per type, incl. ZADD modes and glob
    ExpiryTests.cs                 Fake-clock expiry: lazy, swept, TTL -1/-2, KEEPTTL
    PersistenceTests.cs            Snapshot round-trips, CRC corruption, AOF replay/rewrite, torn tails
    ProtocolTests.cs               Wire-level sessions over in-memory transport: exact reply bytes
    WireGapTests.cs                Redis-exact option/error matrices: WRONGTYPE sweep, SET/EXPIRE/ZADD edges
    AuthTests.cs                   NOAUTH gate, WRONGPASS forms, per-connection auth state, AUTH in MULTI
    Resp3Tests.cs                  HELLO handshake + error order, RESP3 shapes byte-for-byte, RESP2 unchanged
    MaxMemoryTests.cs              OOM rules, LRU/TTL victim choice, volatile scope, eviction DELs in the AOF
    ClientTests.cs                 CLIENT SETNAME/GETNAME/SETINFO/ID, shared state with HELLO
    AofStreamingTests.cs           Streaming reader equivalence with the materialising reader, torn tail
    ServerOptionsTests.cs          --requirepass / --tls-* / --maxmemory parsing and PKCS#12 certificate loading
    TransactionTests.cs            MULTI/EXEC/DISCARD, EXECABORT, runtime-error semantics
    PubSubTests.cs                 Channels, patterns, delivery counts, restricted mode
    ConcurrencyTests.cs            Parallel INCR exactness, mixed load vs the sweep, AOF under parallelism
tools/RedisBenchmark/              One-connection loopback benchmark (pipeline 1 and 50)
```

Design notes worth knowing:

- **Threading model** — commands execute under one gate, deliberately. Redis itself runs commands on a single thread; matching that model makes every command — and every `EXEC` block — atomic by construction, keeps AOF order identical to execution order, and removes a whole class of races. Concurrency lives where it belongs: connections, parsing, and I/O are fully parallel (one session per connection over `System.IO.Pipelines`, a single-writer outbox per connection so pub/sub fan-out never blocks a publisher). The honest trade-off: no parallel command execution across cores.
- **Throughput shape** — a pipelined batch parsed from one read executes under a single gate acquisition with one effects list, all its replies are rendered into one buffer and written to the socket as one frame, and the output pump drains every queued frame into a single write per burst. Command dispatch is keyed on the raw name bytes (ASCII case-insensitive), so the hot path allocates no strings; the per-connection cost of a pipelined command is the parse, the store operation, and the reply render — not a lock, a channel hop, and a socket write each.
- **Snapshot format (RCDB)** — a small binary format of my own: magic + version, then per key a type tag, an optional absolute expiry (Unix ms), and length-prefixed payloads (sorted-set scores stored as exact IEEE-754 bits), closed by an end marker and a CRC32 over everything before it. Writes go to a temp file, are fsynced, then atomically moved over the previous snapshot, so a crash mid-`BGSAVE` can never destroy the last good snapshot. A file that fails CRC or parsing at startup is logged and ignored — the server starts empty rather than crash-looping on a corrupt file.
- **AOF** — every mutating command is appended as a RESP array, but in *normalised effect* form: a conditional `SET … NX EX 60` is logged as the plain `SET` it performed plus an absolute `PEXPIREAT`, an `EXPIRE` that removed a key is logged as `DEL`, and both flush commands log as `FLUSHALL`. Replay therefore reproduces state exactly, with no dependence on wall-clock timing or on options that no longer hold. `BGREWRITEAOF` rebuilds the log from live state (one command per key) while the server keeps running. fsync policies mirror Redis: `always` (fsync before the reply), `everysec` (background fsync, the default), `no` (left to the OS). Startup replay *streams* the file in chunks and applies each command as it is parsed, so peak memory during load tracks the dataset rather than the file — measured on a 330 MB AOF holding 700,000 keys: ~541 MB peak RSS streaming, versus ~1,034 MB for the whole-file loader it replaced, with an identical final state.
- **Crash consistency** — with `--appendfsync always`, a completed write survives a process kill (verified by a kill -9 test, below). With `everysec`, up to ~1 second of writes can be lost on a kill — the same window real Redis has. A crash in the middle of an append leaves a torn tail; replay applies every complete command and ignores the partial one. Snapshots are the coarse fallback: state is as of the last `SAVE`/`BGSAVE`.
- **Time** — everything time-based goes through `TimeProvider`, so expiry behaviour is tested with a fake clock instead of sleeps.

## Honest limits

- **One logical database** (db 0). No `SELECT`, no multiple databases.
- **Authentication is the single default user only** — no ACL users, no per-user command rules. RESP3 covers the reply shapes of the implemented command set; commands that do not exist here (streams, geospatial, modules) have no RESP3 form to give.
- **No replication and no cluster mode** — the remaining frontier, with its design questions recorded under Future work. It is deliberately unbuilt rather than half-built: a replica that has not survived a two-process kill/restart proof with an exactly-compared keyspace is a liability dressed as a feature.
- **Memory accounting is approximate and data-only**: a flat charge per key and per collection element plus exact payload bytes, maintained incrementally. Redis's `used_memory` also counts allocator and server overhead, so the figures are comparable in shape, not absolute value — a cap here should be set with headroom. The LFU eviction policies are not implemented (refused at startup), and the snapshot file (unlike the AOF) is still read into memory whole at load.
- **TLS is an additional listener, never the only one**: the plaintext port always serves too (there is no `--tls-port`-only mode and no `port 0` convention), and client-certificate authentication (`tls-auth-clients`) is not implemented.
- **SCAN is snapshot-based**: the first call snapshots the matching keys and pages through the snapshot, instead of walking a live hash table like Redis. The cursor contract holds (you see every key that matched at scan start, exactly once), but keys created mid-scan don't appear and deleted ones may still be returned.
- Glob patterns and sorted-set ordering are byte-wise, matching Redis's binary-safe comparisons.

## Tests

```bash
dotnet test
```

132 tests: store semantics per type, fake-clock expiry, persistence round-trips (including CRC corruption, AOF rewrite equivalence, torn-tail recovery, expiry persisted across restarts for every type, snapshot/AOF load precedence both ways, and an AOF rewrite racing concurrent writers), exact wire bytes for commands/transactions/pub/sub over an in-memory transport, a `WRONGTYPE` sweep across the type families, the full `SET` / `EXPIRE` / `ZADD` option matrices, arity and syntax error paths, the AUTH matrix (NOAUTH gate over every command family, WRONGPASS forms, per-connection state, no-password outcomes), TLS/AUTH option parsing and certificate loading, concurrency stress (8 workers × 250 parallel `INCR` must total exactly 2,000; AOF replay after 8-way parallel writes must reproduce the state exactly) — plus the `HELLO`/RESP3 handshake and reply shapes byte-for-byte, the maxmemory matrix (OOM text, LRU and TTL victim choice, volatile policies never touching TTL-less keys, eviction `DEL`s present in the AOF, the memory counter matching a full recount after a mixed workload), `CLIENT` subcommands, and the streaming AOF reader's equivalence with the materialising one.

Follow-up audits also compared edge and error semantics command-by-command against a real redis-server 7.0.15; every deviation found was fixed and pinned by a test — nine in the first audit (from `SET ... GET` answering the old value when the write is declined, to Redis's two distinct `ZADD` option-conflict messages), and in the completion pass a phantom empty sorted set left behind by a declined `ZADD ... INCR XX` on a missing key, plus the missing `ZINCRBY` command itself.

The running server was also driven end-to-end by an independent raw-socket RESP client (a scripted checklist of 80 checks covering every command family, transactions, a pub/sub round-trip, and error texts): **80/80 passed**, plus restart-recovery checks — AOF recovery, snapshot-only recovery, and survival of a `kill -9` with `--appendfsync always` — all passed.

## Client compatibility

The server was verified against the real **redis-py 8.1.0** client library, connected over both the plaintext port (with `AUTH`) and the TLS port: a 57-check sweep covering strings (including `SET ... GET` and duplicate-counting `EXISTS`), the expiry family, all list/set/hash/sorted-set commands, `KEYS` / `SCAN` iteration, `INFO` parsing, a `MULTI`/`EXEC` pipeline, EXEC runtime-error semantics (redis-py raises the first error; the sibling command still runs — both verified identical against real Redis), the exact `WRONGTYPE` text, and a publish/subscribe round trip that counted both a direct and a pattern subscriber: **57/57 passed — twice over: once with redis-py's default settings (RESP3, `HELLO` handshake) and once pinned to `protocol=2`**, with identical results. redis-py's connect-time `CLIENT SETINFO` is answered properly, not merely tolerated.

One note for the TLS door: the live check used a self-signed certificate with verification disabled on the client, as any scratch setup would; the server presents whatever PKCS#12 it is given and negotiates TLS 1.2/1.3 (the check negotiated TLS 1.3).

## Benchmark

Head-to-head against the real thing. The same loopback method was driven against this server and **redis-server 7.0.15** on the same machine, by the same driver — one connection, 1,000 warmup requests, 50,000 requests per cell, every reply parsed. Both servers ran with persistence off. Figures are pooled medians of six runs per cell (two back-to-back sessions), rounded; the machine is a shared sandbox VM under parallel load whose own numbers for real Redis swung by ±40% between sessions, so treat them as indicative, not peak:

| Test | Pipeline | redis-csharp | Redis 7.0.15 |
|------|---------:|-------------:|-------------:|
| SET | 1 | 11,400 ops/s | 35,200 ops/s |
| SET | 50 | 151,400 ops/s | 312,300 ops/s |
| GET | 1 | 12,800 ops/s | 26,700 ops/s |
| GET | 50 | 166,600 ops/s | 255,200 ops/s |
| INCR | 1 | 12,300 ops/s | 26,100 ops/s |
| INCR | 50 | 216,200 ops/s | 262,700 ops/s |
| LPUSH | 1 | 14,000 ops/s | 27,900 ops/s |
| LPUSH | 50 | 171,000 ops/s | 258,500 ops/s |

Read plainly: Redis is roughly 2–3× faster unpipelined. Pipelined, the gap used to be 6–9×; a profiling pass found the old cost was not command execution but plumbing — one gate acquisition, one outbound channel hop, and one flushing socket write *per command* — and fixing that (one gate acquisition and one coalesced write per batch, allocation-free dispatch) roughly tripled this server's pipelined throughput versus the previous build measured the same way (57–78k → 151–216k ops/s across the four commands), narrowing the pipelined gap to roughly 1.2–2×. What remains is the expected distance between a managed, deliberately single-writer design and three decades of C optimization — this project aims for protocol fidelity and correctness, not for beating Redis at throughput.

Method note: the numbers were produced by a Python raw-socket driver implementing exactly the method above — this sandbox blocks direct TCP from .NET processes, so the C# benchmark tool in `tools/RedisBenchmark` (same method, same defaults) could not run here; it runs on any unrestricted machine.

## Future work

**Replication** is the named frontier. The pieces it would build on are already in place — the normalised effect stream is a deterministic replication unit (absolute expiries, exactly the state change performed), snapshots exist as a full-sync format, and sessions already manage per-connection outbound queues. The open design questions, in the order they must be answered:

- **Full sync**: ship the RCDB snapshot file, or stream `ExportState` as commands? (File transfer is simpler and resumable; command streaming reuses one code path.)
- **Offsets and partial resync**: a replication id plus a byte offset into the effect stream, with a bounded backlog ring so a reconnecting replica can PSYNC instead of full-syncing — and the backlog size policy.
- **Read-only enforcement**: a write/read classification per command (the maxmemory deny-OOM set is *not* the write set) and Redis's `READONLY` error for client writes on a replica.
- **Backpressure**: a slow replica must never stall the primary's single command gate — bounded per-replica outboxes with a defined drop/disconnect policy, like the pub/sub fan-out already does.
- **Proof bar**: full sync + live propagation + primary kill/restart resync, ending in a full keyspace comparison between two real processes. Anything less is not shipped.

**Cluster mode** comes after replication by design (it multiplies every one of those questions by slot ownership, hash tags, `MOVED`/`ASK` redirection, and failover). Also named and unbuilt: Lua scripting, ACL users, and the LFU eviction policies.

## License

MIT — see [LICENSE](LICENSE).
