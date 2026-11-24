# mongo-survey

A read-only MongoDB health-data collector. It gathers configuration, storage, index-usage, query
and WiredTiger cache data from **every member** of a replica set or sharded cluster. It then samples
each collection's and index's cache and operation counters for a short window, and writes
everything to one compressed JSON-lines file. You can inspect the file, archive it, load it into
MongoDB, or summarize it with `mongo-survey report`.

**Read-only.** mongo-survey never writes to the cluster, and needs only monitoring and read roles.
If you need to approve it before running it against production, the source is small, plain Python
with two dependencies (`pymongo`, `dnspython`), and [every command it sends](#every-command-mongo-survey-runs)
is listed below.

## Quickstart

**1. Create a read-only database user.** On Atlas, with the [Atlas CLI](https://www.mongodb.com/docs/atlas/cli/current/)
(or in the UI under *Database Access*, with these three built-in roles):

```bash
atlas dbusers create --username mongo-survey --role clusterMonitor@admin,readAnyDatabase@admin,read@local
```

It prompts for the password.

On a self-managed deployment, in `mongosh`:

```js
db.getSiblingDB("admin").createUser({
  user: "mongo-survey", pwd: passwordPrompt(),
  roles: ["clusterMonitor", "readAnyDatabase", { role: "read", db: "local" }]
})
```

**2. Install** (Python 3.9+):

```bash
pipx install git+https://github.com/mhelmstetter/mongo-survey
```

**3. Save the connection string in a file** so the password stays out of shell history:

```bash
umask 077; echo 'mongodb+srv://mongo-survey:<password>@<cluster-host>/' > ~/cluster.uri
```

**4. Run the preflight.** It connects, checks the version, your privileges and the namespace
count, and shows how each member will be reached. It collects nothing and writes no file.

```bash
mongo-survey check --uri-file ~/cluster.uri
```

```
12:00:01 INFO    sharded, MongoDB 8.0.12: checking privileges
12:00:02 INFO    discovering members
12:00:09 INFO    namespace inventory
12:00:10 INFO    topology: sharded, MongoDB 8.0.12, 18 data-bearing member(s), 4 database(s), 950 namespace(s)
12:00:10 INFO    atlas: yes — hostname suffix (…); Atlas member tags ['nodeType', 'provider', 'region', 'workloadType'] on 18 member(s)
12:00:10 INFO    collection-level access: mongos (direct shard collection access refused; using mongos tag targeting)
12:00:10 INFO      tags[availabilityZone=us-east-1a | {}] -> shard-0:…, shard-1:…, shard-2:…, shard-3:…, shard-4:…
12:00:10 INFO      tags[availabilityZone=us-east-1b | {}] -> …
12:00:10 INFO      tags[availabilityZone=us-east-1c | {}] -> …
12:00:10 INFO      planned coverage: 15 member(s)
12:00:10 INFO    check passed
```

(18 members counts the config servers; the 15 planned are the shard members, which hold the
collections.)

If a privilege is missing, it lists exactly which ones and the roles that grant them.

**5. Collect.** This takes a few minutes on a large cluster, including a 60-second sampling window:

```bash
mongo-survey collect --uri-file ~/cluster.uri
```

It ends with a coverage summary and the file it wrote, for example
`mongo-survey-mycluster-20260101T120000Z.jsonl.gz`. An exit status of `0` means every member
answered. `2` means the run finished, but something was missing, and the footer of the file says what.

**6. Share or summarize.** Send the `.jsonl.gz` file to whoever is doing the analysis, or preview
the summary yourself:

```bash
mongo-survey report mongo-survey-*.jsonl.gz > report.md
```

## What it collects

- **Cluster and node configuration.** Topology, replica set configs and member states, sharding
  metadata (shards, zones, chunk counts per collection and shard, balancer state), and one snapshot
  per member of `serverStatus`, `hostInfo`, startup options, server parameters, startup warnings
  and the oplog window.
- **Catalog.** Databases, `dbStats`, and collection options (views, time series, validators).
- **Collection and index storage, per member.** Document counts, sizes, free space, compressor,
  and a curated set of WiredTiger cache, cursor, block and btree counters for every collection
  and index. This is the same data `collStats` reports, read in bulk with
  `$_internalAllCollectionStats` (see [How it works](#how-it-works)).
- **Index usage from every member.** `$indexStats` counters are kept per member, so an index that
  looks unused on the primary may serve all the secondary reads. Before hiding or dropping an
  index you need its usage on every member. mongo-survey collects it from every data-bearing
  member, records which members answered, and never calls an index unused unless all of them did.
- **Query workload.** `$queryStats` from every mongos (sharded) or every data member (replica
  set): per query shape, the execution count, total, average and max execution time, documents
  returned, and on 8.0+ the keys and documents examined. Query shapes contain no literal values
  (they are replaced by type placeholders such as `"?number"`). With `--query-stats-hmac`, field and
  collection names are HMAC-hashed by the server as well, using a random key that is never stored.
- **Cache and operation heat.** The same bulk collection stats, sampled repeatedly over a window
  (default 60 s, every 15 s). The window covers each collection's and index's WiredTiger cache
  counters (bytes in cache, read into cache, written from cache, pages requested), plus
  per-collection operation counts and latencies (the counters behind the `top` command) and
  collection-scan counts. The differences between samples show which collections and indexes
  take up the cache, which churn it, and which carry the reads, writes and collection scans.

## How it works

- **One aggregation per batch of collections, not one command per collection.** Collection stats
  come from `$_internalAllCollectionStats`. Each call returns many collections, and the server
  trims WiredTiger's ~180 KB of counters per collection to a curated set before sending it, which
  cuts the volume by about 30x. See [Load on the cluster](#load-on-the-cluster).
- **Every member, even when direct shard access is blocked.** On sharded clusters that reject
  collection-level commands on direct shard connections (Atlas 8.0+, where `directShardOperations`
  cannot be granted), mongo-survey reaches each shard member through mongos with read-preference
  tag sets. It then checks from the results which members actually answered. See
  [How per-member targeting works](#how-per-member-targeting-works).
- **Preflight first.** Topology, version (7.0+), privileges and the namespace count are checked
  before anything is collected, and the run stops if something is missing.

## Install

Requires Python 3.9+ on the machine you run it from, and MongoDB 7.0+ on the cluster.

```bash
pipx install git+https://github.com/mhelmstetter/mongo-survey     # or: uv tool install git+https://github.com/mhelmstetter/mongo-survey
```

Or, with nothing installed but `python3`, use the single-file release:

```bash
curl -LO https://github.com/mhelmstetter/mongo-survey/releases/latest/download/mongo-survey.pyz
curl -LO https://github.com/mhelmstetter/mongo-survey/releases/latest/download/mongo-survey.pyz.sha256
shasum -a 256 -c mongo-survey.pyz.sha256
python3 mongo-survey.pyz --help
```

The `.pyz` is a zip file. `unzip -l mongo-survey.pyz` lists its contents, and you can unzip it to
read the source. Build it yourself with `scripts/build_pyz.sh`.

## Usage

```bash
# keep the password out of shell history: put the URI in a file (or MONGODB_URI)
mongo-survey check   --uri-file ~/cluster.uri     # preflight: topology, privileges, plan; writes nothing
mongo-survey collect --uri-file ~/cluster.uri     # full capture + 60 s cache sampling
mongo-survey cache   --uri-file ~/cluster.uri --cache-duration 300 --cache-interval 30
mongo-survey report  mongo-survey-*.jsonl.gz > report.md
```

Run `check` first. It prints the topology, whether the cluster looks like Atlas (and why), how it
will reach each member, the namespace count, and any missing privileges. It sends no
collection-level commands except one `listCollections` probe on sharded clusters, plus the cheap
namespace count described below.

| Option | Default | |
|---|---|---|
| `--db NAME` (repeatable) | all | only these databases |
| `--exclude-db NAME` | | `admin`, `config`, `local`, `__mdb_internal_atlas` are always skipped unless named with `--db` |
| `--max-namespaces` | 20000 | refuse above this many collections (see [Load](#load-on-the-cluster)) |
| `--allow-many-namespaces` | off | run anyway |
| `--cache-duration` / `--cache-interval` | 60 / 15 s | sampling window; `--cache-duration 0` takes one snapshot |
| `--cache-light` | off | sample op counters only: no `storageStats`, no data handles opened |
| `--shard-access` | auto | `auto` probes; `direct` or `mongos` forces the path |
| `--workers` | 4 | concurrent commands |
| `--batch-size` | 500 | namespaces per `$_internalAllCollectionStats` call |
| `--out` / `--no-compress` | `.` / gzip | output location / plain `.jsonl` |
| `--no-query-stats` | off | skip `$queryStats` |
| `--query-stats-hmac` | off | hash identifiers in `$queryStats` output (needs `queryStatsReadTransformed`, included in `clusterMonitor`) |
| `--include-auth` | off | also capture `usersInfo`/`rolesInfo` (user names; needs `viewUser`/`viewRole`) |

On macOS the tool holds a `caffeinate -i` assertion while it runs. A laptop going to idle sleep
mid-run stalls sampling and resets connections; if it is suspended anyway, the gap is recorded in
the capture (`section: "client"`).

`report` caps the unused and secondary-only index lists at the largest 100 (`--full-lists` for all).

Exit status: `0` complete; `1` refused or fatal (privileges, version, namespace limit); `2`
finished but incomplete (command errors or members that could not be covered: see the footer).

## Required privileges

The run checks privileges up front (`connectionStatus` with `showPrivileges`) and stops with a list
of anything missing. Recommended roles for a dedicated user:

- `clusterMonitor@admin`: server, replication and sharding stats, `$indexStats`, `$queryStats`, `top`, `config` metadata
- `readAnyDatabase@admin`, or `read` on each application database: `listCollections`
- `read@local`: the oplog window

On Atlas, create a database user with those roles. No write role is needed.

## How per-member targeting works

Replica sets, standalones, and sharded clusters that allow direct shard access: mongo-survey opens
a direct connection to each member and runs every command there.

Sharded clusters that refuse direct collection access (Atlas 8.0+, or any 8.0+ cluster without
`directShardOperations`): node-level commands (`serverStatus`, `replSetGetConfig`, `top`…) are still
allowed directly, so member tags come from `replSetGetConfig`. Collection-level commands go through
mongos with a read preference like

```js
{ mode: "nearest", tags: [ { availabilityZone: "us-east-1a" }, {} ] }
```

mongos applies this per shard and picks that shard's member in that zone. One command (a "round")
therefore reads one member of every shard. The planner (`targeting.py`) picks tag sets that each
identify exactly one member within its shard, and builds rounds until every member is planned. On
Atlas that is usually one round per availability zone. The trailing `{}` stops the command failing
on a shard that has no member with that tag. That shard answers from any member, and the duplicate
is harmless.

Every result carries the `host` that produced it. Coverage is computed from what actually came
back, not from the plan, and is recorded in the file's footer. Members that cannot be singled out
(hidden members, or members with identical tags) are reported as gaps, never assumed.

## Load on the cluster

- `storageStats` opens each collection's and index's WiredTiger data handle on every member. With a
  very large number of collections (tens of thousands and up) this can push out data handles the
  workload needs. Hence `--max-namespaces`, and `--cache-light`, which samples only the
  `latencyStats`, `count` and `queryExecStats` sections and opens no data handles.
- One unfiltered light pass per shard (`latencyStats`, `count`, `queryExecStats`; no data
  handles) builds the namespace inventory and records which shards hold each collection. Storage
  stats are then requested in batches of listed namespaces (`$match: {ns: {$in: [...]}}`, absorbed
  into the stage, `--batch-size` 500 by default), so only in-scope collections are opened. In
  direct mode, `$indexStats` is sent only to members of shards that hold the collection.
- Every command is a read. Concurrency is capped by `--workers`.
- `$_internalAllCollectionStats` is an internal, undocumented aggregation stage (MongoDB 6.0+). Its
  behavior can change between releases. mongo-survey is tested on 7.0 and 8.0.

## Output format

One file, `mongo-survey-<cluster>-<UTC>.jsonl.gz`, with one [relaxed Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)
object per line, written as the run progresses (an interrupted run is readable up to the last line):

- **header** (first line): tool and schema version, redacted URI, options, topology with member
  tags and states, Atlas evidence, targeting plan.
- **records**: `{type:"record", section, subsection, host, shard, target, command, ts:{start,end}, output, error}`
  plus `batch`/`ns`/`sample` where relevant. The section/subsection/output layout is the one
  used by the `getMongoData.js` support script, so existing parsers for that format adapt easily.
- **footer** (last line): coverage per kind of data and per namespace, plus every error.

```bash
gzcat capture.jsonl.gz | jq -c 'select(.section=="indexstats") | .output[] | {ns: .name, host, ops: .accesses.ops}'
mongoimport --uri "$ANALYSIS_URI" -c survey --file capture.jsonl   # gunzip first
```

The password is never written. The URI is stored with `***` in its place.

## Every command mongo-survey runs

Seed connection (mongos, or the replica set):

| Command | Why |
|---|---|
| `hello`, `buildInfo` | topology and version (7.0+ enforced on every member) |
| `connectionStatus {showPrivileges:true}` | privilege preflight |
| `listDatabases` | database list |
| `replSetGetConfig`, `replSetGetStatus` | (replica set) members, tags, states |
| `find` on `config.shards`, `databases`, `collections`, `tags`, `settings`, `mongos`; `getShardMap` | (sharded) metadata, config servers |
| `aggregate` on `config.chunks` (`$group` by collection uuid and shard, `$lookup` collections) | chunk counts and jumbo chunks |
| `balancerStatus`, `aggregate [{$shardedDataDistribution:{}}]` | (sharded) balancer, data distribution |
| `getDefaultRWConcern`, `getParameter {featureCompatibilityVersion:1}` | cluster settings |
| `dbStats`, `listCollections` per database | database stats, collection options, views, validators |
| `usersInfo {forAllDBs:true}`, `rolesInfo` | only with `--include-auth` |

Direct connection to each mongos (sharded) or each data member (replica set), `collect` only:

| Command | Why |
|---|---|
| `getParameter {internalQueryStatsRateLimit:1}` | is query stats recording on? (0 = off) |
| `aggregate [{$queryStats:{}}, {$group: by key.queryShape …}]` with `allowDiskUse:false` | per-query-shape workload; grouping on the server folds per-client variants together (98k entries became 695 shapes on one cluster) |

Direct connection to each member (data-bearing members and config servers):

| Command | Why |
|---|---|
| `hello`, `buildInfo`, `serverStatus`, `hostInfo`, `getCmdLineOpts`, `getParameter:'*'`, `getLog:'startupWarnings'`, `top` | node snapshot |
| `replSetGetConfig`, `replSetGetStatus` | member list, tags, states |
| `find` first and last `local.oplog.rs` entry (`ts`, `wall` only), `collStats oplog.rs` | oplog window and size |
| `hostInfo` (`cache` command only) | OS hostname, to match `$indexStats` host names to members |
| `listCollections` (sharded only, once) | probe: is direct shard collection access allowed? |

Per member, directly or through mongos rounds:

| Command | Why |
|---|---|
| `aggregate [{$_internalAllCollectionStats:{stats:{latencyStats,count,queryExecStats}}}]` once per shard (and each sample with `--cache-light`) | namespace inventory (cheap; no data handles) |
| `aggregate [{$_internalAllCollectionStats:{stats:{storageStats,latencyStats,count,queryExecStats}}}, {$match:{ns:{$in:[…]}}}, {$group:…}]` per batch, each sample | sizes, curated WiredTiger cache, cursor, block and btree counters per collection and index (field list in `pipelines.py`) |
| `aggregate [{$indexStats:{}}, {$project:…}]` per collection, at the start and end of sampling | index usage per member |

## Limitations (first release)

- Direct network access to every member is required for node-level commands. A deployment reachable
  only through a private endpoint to mongos is not supported yet.
- Hidden members are reached in direct mode only. Through mongos they show up as coverage gaps.
- `$queryStats` needs MongoDB 7.0.12+/7.1+. Atlas records query stats by default. Self-managed
  deployments default to `internalQueryStatsRateLimit: 0`, which records nothing; the tool warns
  about this. Enable it with `setParameter` (for example `-1`, unlimited) and let the workload run
  for a while before collecting.
- There is no "collect what you can" mode for partial privileges. The run refuses up front instead.
- Tested on MongoDB 7.0 and 8.0 (Atlas and self-managed). Not yet tested on later releases.
- On Atlas 8.0, the node-snapshot `serverStatus` over a direct shard connection is occasionally rejected for a few
  seconds with "connecting directly to a shard" (code 13), naming a collection. The cause is
  server-side and the failures are transient. mongo-survey records them as errors (the exit status is 2) and does not retry.

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[test]'
.venv/bin/pytest
./scripts/build_pyz.sh
```

License: Apache 2.0.
