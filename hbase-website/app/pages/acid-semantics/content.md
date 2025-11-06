# ACID properties of HBase

Apache HBase (TM) is not an ACID compliant database. However, it does guarantee certain specific properties.

## Definitions

For the sake of common vocabulary, we define the following terms:

**Atomicity**  
an operation is atomic if it either completes entirely or not at all

**Consistency**  
all actions cause the table to transition from one valid state directly to another (eg a row will not disappear during an update, etc)

**Isolation**  
an operation is isolated if it appears to complete independently of any other concurrent transaction

**Durability**  
any update that reports "successful" to the client will not be lost

**Visibility**  
an update is considered visible if any subsequent read will see the update as having been committed

The terms _must_ and _may_ are used as specified by RFC 2119. In short, the word "must" implies that, if some case exists where the statement is not true, it is a bug. The word "may" implies that, even if the guarantee is provided in a current release, users should not rely on it.

## APIs to consider

- Read APIs
  - get
  - scan
- Write APIs
  - put
  - batch put
  - delete
- Combination (read-modify-write) APIs
  - incrementColumnValue
  - checkAndPut

## Guarantees Provided

### Atomicity

1. All mutations are atomic within a row. Any put will either wholly succeed or wholly fail.[3]
   1. An operation that returns a "success" code has completely succeeded.
   2. An operation that returns a "failure" code has completely failed.
   3. An operation that times out may have succeeded and may have failed. However, it will not have partially succeeded or failed.
2. This is true even if the mutation crosses multiple column families within a row.
3. APIs that mutate several rows will _not_ be atomic across the multiple rows. For example, a multiput that operates on rows 'a','b', and 'c' may return having mutated some but not all of the rows. In such cases, these APIs will return a list of success codes, each of which may be succeeded, failed, or timed out as described above.
4. The checkAndPut API happens atomically like the typical compareAndSet (CAS) operation found in many hardware architectures.
5. The order of mutations is seen to happen in a well-defined order for each row, with no interleaving. For example, if one writer issues the mutation "a=1,b=1,c=1" and another writer issues the mutation "a=2,b=2,c=2", the row must either be "a=1,b=1,c=1" or "a=2,b=2,c=2" and must _not_ be something like "a=1,b=2,c=1".
   1. Please note that this is not true _across rows_ for multirow batch mutations.

### Consistency and Isolation

1. All rows returned via any access API will consist of a complete row that existed at some point in the table's history.
2. This is true across column families - i.e a get of a full row that occurs concurrent with some mutations 1,2,3,4,5 will return a complete row that existed at some point in time between mutation i and i+1 for some i between 1 and 5.
3. The state of a row will only move forward through the history of edits to it.

#### Consistency of Scans

A scan is **not** a consistent view of a table. Scans do **not** exhibit _snapshot isolation_.

Rather, scans have the following properties:

1. Any row returned by the scan will be a consistent view (i.e. that version of the complete row existed at some point in time) [1]
2. A scan will always reflect a view of the data _at least as new as_ the beginning of the scan. This satisfies the visibility guarantees enumerated below.
   1. For example, if client A writes data X and then communicates via a side channel to client B, any scans started by client B will contain data at least as new as X.
   2. A scan _must_ reflect all mutations committed prior to the construction of the scanner, and _may_ reflect some mutations committed subsequent to the construction of the scanner.
   3. Scans must include _all_ data written prior to the scan (except in the case where data is subsequently mutated, in which case it _may_ reflect the mutation)

Those familiar with relational databases will recognize this isolation level as "read committed".

Please note that the guarantees listed above regarding scanner consistency are referring to "transaction commit time", not the "timestamp" field of each cell. That is to say, a scanner started at time _t_ may see edits with a timestamp value greater than _t_, if those edits were committed with a "forward dated" timestamp before the scanner was constructed.

### Visibility

1. When a client receives a "success" response for any mutation, that mutation is immediately visible to both that client and any client with whom it later communicates through side channels. [3]
2. A row must never exhibit so-called "time-travel" properties. That is to say, if a series of mutations moves a row sequentially through a series of states, any sequence of concurrent reads will return a subsequence of those states.
   1. For example, if a row's cells are mutated using the "incrementColumnValue" API, a client must never see the value of any cell decrease.
   2. This is true regardless of which read API is used to read back the mutation.
3. Any version of a cell that has been returned to a read operation is guaranteed to be durably stored.

### Durability

1. All visible data is also durable data. That is to say, a read will never return data that has not been made durable on disk[2]
2. Any operation that returns a "success" code (eg does not throw an exception) will be made durable.[3]
3. Any operation that returns a "failure" code will not be made durable (subject to the Atomicity guarantees above)
4. All reasonable failure scenarios will not affect any of the guarantees of this document.

### Tunability

All of the above guarantees must be possible within Apache HBase. For users who would like to trade off some guarantees for performance, HBase may offer several tuning options. For example:

- Visibility may be tuned on a per-read basis to allow stale reads or time travel.
- Durability may be tuned to only flush data to disk on a periodic basis

## More Information

For more information, see the [client architecture](book.html#client) or [data model](book.html#datamodel) sections in the Apache HBase Reference Guide.

## Footnotes

[1] A consistent view is not guaranteed intra-row scanning -- i.e. fetching a portion of a row in one RPC then going back to fetch another portion of the row in a subsequent RPC. Intra-row scanning happens when you set a limit on how many values to return per Scan#next (See [Scan#setBatch(int)](http://hbase.apache.org/devapidocs/org/apache/hadoop/hbase/client/Scan.html#setBatch(int))).

[2] In the context of Apache HBase, "durably on disk" implies an hflush() call on the transaction log. This does not actually imply an fsync() to magnetic media, but rather just that the data has been written to the OS cache on all replicas of the log. In the case of a full datacenter power loss, it is possible that the edits are not truly durable.

[3] Puts will either wholly succeed or wholly fail, provided that they are actually sent to the RegionServer. If the writebuffer is used, Puts will not be sent until the writebuffer is filled or it is explicitly flushed.

