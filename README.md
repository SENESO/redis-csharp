# Redis Server in C#

A complete, from-scratch implementation of a Redis-compatible server in C# on .NET 8 — the RESP2 wire protocol, all five core data types, transactions, pub/sub, and both of Redis's persistence models (snapshots and an append-only file), served over `System.IO.Pipelines`.

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

**Sorted sets** — `ZADD` (with `NX` / `XX` / `GT` / `LT` / `CH` / `INCR`), `ZSCORE`, `ZRANGE` (with `WITHSCORES`), `ZREM`, `ZCARD`

**Transactions** — `MULTI` / `EXEC` / `DISCARD`, with queue-time validation (`EXECABORT`) and Redis's no-rollback semantics for runtime errors

**Pub/sub** — `SUBSCRIBE` / `UNSUBSCRIBE`, `PSUBSCRIBE` / `PUNSUBSCRIBE` (glob patterns), `PUBLISH`, restricted-mode command rules while subscribed

**Server** — `PING`, `ECHO`, `QUIT`, `DBSIZE`, `INFO` (server / clients / persistence / stats / keyspace sections), `SAVE` / `BGSAVE`, `BGREWRITEAOF`

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

…or point a real client at it — it speaks RESP2 arrays exactly as Redis does.

## Architecture

```
src/RedisServer/
    Program.cs                     Generic-host wiring: options → DI → hosted services
    Options/ServerOptions.cs       -p / --dir / --appendonly / --appendfsync configuration
    Transport/TcpServer.cs         Loopback listener + accept loop (one session per connection)
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
    Persistence/AppendOnlyFile.cs  RESP-encoded append log + rewrite (compaction)
    Persistence/PersistenceManager.cs  Load precedence, SAVE/BGSAVE, rewrite orchestration
    Hosting/                       Listener lifetime + 100 ms maintenance (sweep, everysec fsync)
tests/RedisServer.Tests/
    StoreTests.cs / CollectionTests.cs   Store semantics per type, incl. ZADD modes and glob
    ExpiryTests.cs                 Fake-clock expiry: lazy, swept, TTL -1/-2, KEEPTTL
    PersistenceTests.cs            Snapshot round-trips, CRC corruption, AOF replay/rewrite, torn tails
    ProtocolTests.cs               Wire-level sessions over in-memory transport: exact reply bytes
    WireGapTests.cs                Redis-exact option/error matrices: WRONGTYPE sweep, SET/EXPIRE/ZADD edges
    TransactionTests.cs            MULTI/EXEC/DISCARD, EXECABORT, runtime-error semantics
    PubSubTests.cs                 Channels, patterns, delivery counts, restricted mode
    ConcurrencyTests.cs            Parallel INCR exactness, mixed load vs the sweep, AOF under parallelism
tools/RedisBenchmark/              One-connection loopback benchmark (pipeline 1 and 50)
```

Design notes worth knowing:

- **Threading model** — commands execute under one gate, deliberately. Redis itself runs commands on a single thread; matching that model makes every command — and every `EXEC` block — atomic by construction, keeps AOF order identical to execution order, and removes a whole class of races. Concurrency lives where it belongs: connections, parsing, and I/O are fully parallel (one session per connection over `System.IO.Pipelines`, a single-writer outbox per connection so pub/sub fan-out never blocks a publisher). The honest trade-off: no parallel command execution across cores.
- **Snapshot format (RCDB)** — a small binary format of my own: magic + version, then per key a type tag, an optional absolute expiry (Unix ms), and length-prefixed payloads (sorted-set scores stored as exact IEEE-754 bits), closed by an end marker and a CRC32 over everything before it. Writes go to a temp file, are fsynced, then atomically moved over the previous snapshot, so a crash mid-`BGSAVE` can never destroy the last good snapshot. A file that fails CRC or parsing at startup is logged and ignored — the server starts empty rather than crash-looping on a corrupt file.
- **AOF** — every mutating command is appended as a RESP array, but in *normalised effect* form: a conditional `SET … NX EX 60` is logged as the plain `SET` it performed plus an absolute `PEXPIREAT`, an `EXPIRE` that removed a key is logged as `DEL`, and both flush commands log as `FLUSHALL`. Replay therefore reproduces state exactly, with no dependence on wall-clock timing or on options that no longer hold. `BGREWRITEAOF` rebuilds the log from live state (one command per key) while the server keeps running. fsync policies mirror Redis: `always` (fsync before the reply), `everysec` (background fsync, the default), `no` (left to the OS).
- **Crash consistency** — with `--appendfsync always`, a completed write survives a process kill (verified by a kill -9 test, below). With `everysec`, up to ~1 second of writes can be lost on a kill — the same window real Redis has. A crash in the middle of an append leaves a torn tail; replay applies every complete command and ignores the partial one. Snapshots are the coarse fallback: state is as of the last `SAVE`/`BGSAVE`.
- **Time** — everything time-based goes through `TimeProvider`, so expiry behaviour is tested with a fake clock instead of sleeps.

## Honest limits

- **One logical database** (db 0). No `SELECT`, no multiple databases, no `AUTH`, no TLS.
- **SCAN is snapshot-based**: the first call snapshots the matching keys and pages through the snapshot, instead of walking a live hash table like Redis. The cursor contract holds (you see every key that matched at scan start, exactly once), but keys created mid-scan don't appear and deleted ones may still be returned.
- **The AOF is read fully into memory at startup** before replay; fine for this scale, not for multi-GB logs.
- **No memory bound / eviction**: Redis's `maxmemory` policies are not implemented — this server will use the memory you give it.
- Glob patterns and sorted-set ordering are byte-wise, matching Redis's binary-safe comparisons.

## Tests

```bash
dotnet test
```

92 tests: store semantics per type, fake-clock expiry, persistence round-trips (including CRC corruption, AOF rewrite equivalence, torn-tail recovery, expiry persisted across restarts for every type, snapshot/AOF load precedence both ways, and an AOF rewrite racing concurrent writers), exact wire bytes for commands/transactions/pub/sub over an in-memory transport, a `WRONGTYPE` sweep across the type families, the full `SET` / `EXPIRE` / `ZADD` option matrices, arity and syntax error paths, and concurrency stress (8 workers × 250 parallel `INCR` must total exactly 2,000; AOF replay after 8-way parallel writes must reproduce the state exactly).

A follow-up audit also compared edge and error semantics command-by-command against a real redis-server 7.0.15; every deviation found (nine — from `SET ... GET` answering the old value when the write is declined, to Redis's two distinct `ZADD` option-conflict messages) was fixed and pinned by a test.

The running server was also driven end-to-end by an independent raw-socket RESP client (a scripted checklist of 80 checks covering every command family, transactions, a pub/sub round-trip, and error texts): **80/80 passed**, plus restart-recovery checks — AOF recovery, snapshot-only recovery, and survival of a `kill -9` with `--appendfsync always` — all passed.

## Benchmark

Head-to-head against the real thing. The same loopback method was driven against this server and **redis-server 7.0.15** on the same machine, in the same session, by the same driver — one connection, 1,000 warmup requests, 50,000 requests per cell, every reply parsed. Both servers ran with persistence off. Figures are the median of three runs, rounded; the machine is a shared sandbox VM under parallel load, so treat them as indicative, not peak:

| Test | Pipeline | redis-csharp | Redis 7.0.15 |
|------|---------:|-------------:|-------------:|
| SET | 1 | 14,000 ops/s | 34,300 ops/s |
| SET | 50 | 79,900 ops/s | 706,000 ops/s |
| GET | 1 | 15,700 ops/s | 34,300 ops/s |
| GET | 50 | 87,100 ops/s | 540,100 ops/s |
| INCR | 1 | 16,000 ops/s | 34,100 ops/s |
| INCR | 50 | 93,500 ops/s | 797,700 ops/s |
| LPUSH | 1 | 18,100 ops/s | 35,500 ops/s |
| LPUSH | 50 | 82,800 ops/s | 620,000 ops/s |

Read plainly: Redis is roughly 2–2.5× faster unpipelined and 6–9× faster pipelined. That is the expected gap between a managed, deliberately single-writer design and three decades of C optimization — this project aims for protocol fidelity and correctness, not for beating Redis at throughput.

Method note: the numbers were produced by a Python raw-socket driver implementing exactly the method above — this sandbox blocks direct TCP from .NET processes, so the C# benchmark tool in `tools/RedisBenchmark` (same method, same defaults) could not run here; it runs on any unrestricted machine.

## Future work

Named, deliberately not built here: replication (primary/replica), cluster mode, RESP3, and Lua scripting.

## License

MIT — see [LICENSE](LICENSE).
