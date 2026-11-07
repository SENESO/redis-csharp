# Redis Server in C#

A complete, from-scratch implementation of a Redis-compatible server in C# on .NET 8 — the RESP2 and RESP3 wire protocols (`HELLO` negotiation), all five core data types, transactions, pub/sub, memory limits with eviction, both of Redis's persistence models (snapshots and an append-only file), and primary/replica replication, served over `System.IO.Pipelines`.

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

**Replication** — `REPLICAOF host port` / `REPLICAOF NO ONE` (or `--replicaof` at startup; `--masterauth` when the primary requires authentication). The primary's writes propagate as the same normalised effect stream the AOF records, addressed by a replication id plus a byte offset; a bounded backlog (16 MB by default, `--repl-backlog-size`) lets a reconnecting replica partial-resync (`PSYNC` → `+CONTINUE`) with exactly the frames it missed, and anything outside the window falls back to a full sync (the RCDB snapshot, taken under the command gate at a stated offset). Replicas are read-only — Redis's exact `READONLY` error, enforced from a per-command write classification — keep serving reads while the primary is down, retry with backoff, and can be promoted with `REPLICAOF NO ONE`. A replica records propagated effects in its own AOF (rewritten from the synced state at full sync), so a replica restart keeps the dataset. A slow replica can never stall the primary: each link's outbound queue is bounded by the backlog size, and overflow drops the link to resync. `INFO replication` reports role, link state, and offsets on both sides. *(Proof status: live-proven by the two-process kill/restart proof — tools/replica-proof.ps1 — on the owner's Windows machine, 2026-11-07: full sync of a 10,000-key mixed dataset, live write wave, TTL expiry converging on both sides, then the primary killed and restarted on its data directory and the replica full-resynced to an identical keyspace.)*

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
| `--replicaof <host> <port>` | Start as a replica of the given primary (runtime `REPLICAOF` can retarget or promote) | _(standalone)_ |
| `--masterauth <password>` | Password presented to the primary when connecting as a replica (or the `REDIS_MASTERAUTH` env var) | _(none)_ |
| `--repl-backlog-size <bytes>` | Replication backlog bound; also each replica link's outbound queue bound. Same size suffixes as `--maxmemory` | `16mb` |

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
    Replication/ReplicationManager.cs  Stream identity (replid + byte offset), bounded backlog, link registry, role state
    Replication/ReplicaLink.cs     One replica's bounded outbound queue + pump on the primary
    Hosting/                       Listener lifetime + 100 ms maintenance (sweep, everysec fsync) + the replica sync loop
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
    ReplicationTests.cs            Backlog/offset math, PSYNC framing, READONLY matrix, replica apply path
    ServerOptionsTests.cs          --requirepass / --tls-* / --maxmemory parsing and PKCS#12 certificate loading
    TransactionTests.cs            MULTI/EXEC/DISCARD, EXECABORT, runtime-error semantics
    PubSubTests.cs                 Channels, patterns, delivery counts, restricted mode
    ConcurrencyTests.cs            Parallel INCR exactness, mixed load vs the sweep, AOF under parallelism
tools/RedisBenchmark/              One-connection loopback benchmark (pipeline 1 and 50)
tools/replica-proof.py             Two-process replication proof (raw RESP, stdlib only)
tools/replica-proof.ps1            Windows harness: starts both servers, runs the proof, optional primary kill/restart phase
```

Design notes worth knowing:

- **Threading model** — commands execute under one gate, deliberately. Redis itself runs commands on a single thread; matching that model makes every command — and every `EXEC` block — atomic by construction, keeps AOF order identical to execution order, and removes a whole class of races. Concurrency lives where it belongs: connections, parsing, and I/O are fully parallel (one session per connection over `System.IO.Pipelines`, a single-writer outbox per connection so pub/sub fan-out never blocks a publisher). The honest trade-off: no parallel command execution across cores.
- **Throughput shape** — a pipelined batch parsed from one read executes under a single gate acquisition with one effects list, all its replies are rendered into one buffer and written to the socket as one frame, and the output pump drains every queued frame into a single write per burst. Command dispatch is keyed on the raw name bytes (ASCII case-insensitive), so the hot path allocates no strings; the per-connection cost of a pipelined command is the parse, the store operation, and the reply render — not a lock, a channel hop, and a socket write each.
- **Snapshot format (RCDB)** — a small binary format of my own: magic + version, then per key a type tag, an optional absolute expiry (Unix ms), and length-prefixed payloads (sorted-set scores stored as exact IEEE-754 bits), closed by an end marker and a CRC32 over everything before it. Writes go to a temp file, are fsynced, then atomically moved over the previous snapshot, so a crash mid-`BGSAVE` can never destroy the last good snapshot. A file that fails CRC or parsing at startup is logged and ignored — the server starts empty rather than crash-looping on a corrupt file.
- **AOF** — every mutating command is appended as a RESP array, but in *normalised effect* form: a conditional `SET … NX EX 60` is logged as the plain `SET` it performed plus an absolute `PEXPIREAT`, an `EXPIRE` that removed a key is logged as `DEL`, and both flush commands log as `FLUSHALL`. Replay therefore reproduces state exactly, with no dependence on wall-clock timing or on options that no longer hold. `BGREWRITEAOF` rebuilds the log from live state (one command per key) while the server keeps running. fsync policies mirror Redis: `always` (fsync before the reply), `everysec` (background fsync, the default), `no` (left to the OS). Startup replay *streams* the file in chunks and applies each command as it is parsed, so peak memory during load tracks the dataset rather than the file — measured on a 330 MB AOF holding 700,000 keys: ~541 MB peak RSS streaming, versus ~1,034 MB for the whole-file loader it replaced, with an identical final state.
- **Crash consistency** — with `--appendfsync always`, a completed write survives a process kill (verified by a kill -9 test, below). With `everysec`, up to ~1 second of writes can be lost on a kill — the same window real Redis has. A crash in the middle of an append leaves a torn tail; replay applies every complete command and ignores the partial one. Snapshots are the coarse fallback: state is as of the last `SAVE`/`BGSAVE`.
- **Replication** — the replication stream *is* the AOF's effect stream: frames are the normalised effects (absolute expiries, the exact state change performed), offsets count encoded frame bytes, and the backlog is a byte-bounded ring of recent frames. Full sync serialises the RCDB snapshot in memory (`Serialize`/`TryParse` are the snapshot file's own code paths) under the command gate, so the snapshot and its advertised offset are exact as a pair; the transfer mirrors Redis's RDB form (a `+FULLRESYNC` line, then the payload as a bulk string with no trailing CRLF). A replica's persistence story: a full sync rewrites its AOF from the synced state under the gate, and every applied frame is appended verbatim, so a replica restart recovers by ordinary AOF replay. Expiry needs no propagation of its own — deadlines are absolute, and each side expires locally (lazy + sweep), the same property AOF replay already relies on. Read-only classification lives on the command spec (`Write`), separate from the maxmemory deny-OOM set: the two answer different questions, and merging them would refuse `LPOP` at a memory ceiling and allow it on a replica, both wrong.
- **Time** — everything time-based goes through `TimeProvider`, so expiry behaviour is tested with a fake clock instead of sleeps.

## Honest limits

- **One logical database** (db 0). No `SELECT`, no multiple databases.
- **Authentication is the single default user only** — no ACL users, no per-user command rules. RESP3 covers the reply shapes of the implemented command set; commands that do not exist here (streams, geospatial, modules) have no RESP3 form to give.
- **No cluster mode** — the remaining frontier, recorded under Future work. Replication (above) is implemented and live-proven (see the proof status in Features).
- **Replication state is in-memory only**: the replication id is generated per process and adopted at full sync — it is not persisted (the RCDB format carries no replication metadata), so a primary restart changes its id and its replicas full-resync on reconnect, where Redis's persisted id would let them partial-resync. Backlog and offset likewise restart from the loaded dataset.
- **Memory accounting is approximate and data-only**: a flat charge per key and per collection element plus exact payload bytes, maintained incrementally. Redis's `used_memory` also counts allocator and server overhead, so the figures are comparable in shape, not absolute value — a cap here should be set with headroom. The LFU eviction policies are not implemented (refused at startup), and the snapshot file (unlike the AOF) is still read into memory whole at load.
- **TLS is an additional listener, never the only one**: the plaintext port always serves too (there is no `--tls-port`-only mode and no `port 0` convention), and client-certificate authentication (`tls-auth-clients`) is not implemented.
- **SCAN is snapshot-based**: the first call snapshots the matching keys and pages through the snapshot, instead of walking a live hash table like Redis. The cursor contract holds (you see every key that matched at scan start, exactly once), but keys created mid-scan don't appear and deleted ones may still be returned.
- Glob patterns and sorted-set ordering are byte-wise, matching Redis's binary-safe comparisons.

## Tests

```bash
dotnet test
```

149 tests: store semantics per type, fake-clock expiry, persistence round-trips (including CRC corruption, AOF rewrite equivalence, torn-tail recovery, expiry persisted across restarts for every type, snapshot/AOF load precedence both ways, and an AOF rewrite racing concurrent writers), exact wire bytes for commands/transactions/pub/sub over an in-memory transport, a `WRONGTYPE` sweep across the type families, the full `SET` / `EXPIRE` / `ZADD` option matrices, arity and syntax error paths, the AUTH matrix (NOAUTH gate over every command family, WRONGPASS forms, per-connection state, no-password outcomes), TLS/AUTH option parsing and certificate loading, concurrency stress (8 workers × 250 parallel `INCR` must total exactly 2,000; AOF replay after 8-way parallel writes must reproduce the state exactly) — plus the `HELLO`/RESP3 handshake and reply shapes byte-for-byte, the maxmemory matrix (OOM text, LRU and TTL victim choice, volatile policies never touching TTL-less keys, eviction `DEL`s present in the AOF, the memory counter matching a full recount after a mixed workload), `CLIENT` subcommands, the streaming AOF reader's equivalence with the materialising one, and the replication suite (backlog trimming and offset windows, the full/partial sync decision, PSYNC framing and snapshot payload over real sessions, propagation frames byte-equal to the AOF effect stream, the READONLY matrix over the true write set including the EXEC-array case, and the replica apply path's landing spots: store, replica AOF, served stream).

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

**Replication** is built (see Features and the design notes above); its recorded design answers, for the record: full sync ships the RCDB snapshot bytes rather than a command stream, because the file format already has a CRC and a tested parser, and one state-transfer format beats two. Offsets count encoded frame bytes into the effect stream, with a byte-bounded backlog ring for partial resync. Read-only enforcement uses a dedicated per-command write classification, not the deny-OOM set. Backpressure is a bounded per-replica outbox (the backlog bound) whose overflow drops the link to resync, so the command gate never waits on a replica. The proof bar — full sync + live propagation + primary kill/restart resync ending in a full keyspace comparison between two real processes — was met on the owner's Windows machine on 2026-11-07 via `tools/replica-proof.py` / `replica-proof.ps1` (the script runs there because the development sandbox's permission layer blocks outbound TCP from .NET processes, so the replica's sync connection cannot be exercised in it). Every stage passed: REPLICAOF accepted, writes on the replica refused with the exact READONLY error while reads kept serving, 10,600 load commands converged to identical replication offsets, 10,000 keys compared identical, a live write wave converged, a short-TTL key expired on both sides, and after the primary was killed and restarted on the same data directory the replica full-resynced under the new replication id to an identical 9,997-key keyspace.

**Cluster mode** comes after replication by design (it multiplies every one of those questions by slot ownership, hash tags, `MOVED`/`ASK` redirection, and failover). Also named and unbuilt: Lua scripting, ACL users, and the LFU eviction policies.

## License

MIT — see [LICENSE](LICENSE).
