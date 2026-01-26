<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# HBase Ribbon Filter Design Document

## 1. Overview

### 1.1 Summary

This document proposes adding **Ribbon Filter** to HBase as an alternative to Bloom Filter. Ribbon Filter provides approximately **30% space savings** compared to Bloom Filter while maintaining the same False Positive Rate (FPR), and shows better performance in certain workloads.

### 1.2 Motivation

Bloom Filter is used to quickly determine whether a key exists in an HFile, reducing disk I/O. However, it has fundamental space limitations: approximately **9.6 bits/key** is required for 1% FPR, which is **44% more** than the theoretical minimum of 6.64 bits/key.

Ribbon Filter, based on research from Facebook/Meta, achieves approximately **7 bits/key** at the same FPR, close to the **theoretical minimum**. In large-scale HBase deployments with billions of keys, this translates to significant memory and storage savings.

Among modern filter alternatives (Cuckoo, Xor, Xor+, Ribbon), Ribbon Filter offers the **best space efficiency** (<10% overhead vs. Bloom's 44%+) while maintaining practical construction and query performance. It is already **proven in production** through RocksDB (since v6.15.0).

### 1.3 Goals

- Provide Ribbon Filter as an alternative filter type alongside existing Bloom Filter
- Achieve ~30% space savings compared to Bloom Filter at the same FPR
- Maintain backward compatibility with existing HFiles and configurations
- Minimize modifications to existing Bloom Filter implementation

---

## 2. Design

### 2.1 Overview

Ribbon Filter is based on the paper ["Ribbon filter: practically smaller than Bloom and Xor"](https://arxiv.org/abs/2103.02515), and this implementation references [FastFilter/fastfilter_cpp](https://github.com/FastFilter/fastfilter_cpp).

Ribbon Filter has combinations of Standard/Homogeneous variants and Row-Major/ICML storage formats. This implementation adopts the **Homogeneous Ribbon + ICML** combination. Homogeneous is a simplified version of Standard, and ICML offers better space efficiency than Row-Major with variable fingerprint bits and cache-friendly structure. Supporting Standard + Row-Major combination would increase implementation complexity with minimal benefits, so it was excluded.

### 2.2 Configuration

A new enum `BloomFilterImpl` is added to select the filter implementation:

```java
public enum BloomFilterImpl {
  BLOOM,   // Traditional Bloom filter (default)
  RIBBON   // Ribbon filter (more space-efficient)
}
```

#### Per-Table Configuration

```ruby
create 'mytable', {NAME => 'cf', BLOOMFILTER => 'ROW', BLOOMFILTER_IMPL => 'RIBBON'}
create 'mytable', {NAME => 'cf', BLOOMFILTER => 'ROWCOL', BLOOMFILTER_IMPL => 'RIBBON'}
create 'mytable', {NAME => 'cf', BLOOMFILTER => 'ROWPREFIX_FIXED_LENGTH',
                   BLOOMFILTER_IMPL => 'RIBBON',
                   CONFIGURATION => {'RowPrefixBloomFilter.prefix_length' => '10'}}
```

#### Global Configuration

A global default can be set in `hbase-site.xml`:

```xml
<property>
  <name>io.storefile.bloom.filter.impl</name>
  <value>RIBBON</value>  <!-- or BLOOM (default) -->
</property>
```

When both global and per-table settings exist, the per-table setting takes precedence.

Ribbon Filter uses existing Bloom Filter settings as-is:

- `io.storefile.bloom.error.rate`: Target false positive rate (default: 0.01)
- `io.storefile.bloom.block.size`: Block size in bytes (default: 128KB)

### 2.3 Storage Format

Ribbon Filter reuses the existing `BLOOM_CHUNK` block type and is distinguished by the VERSION field in metadata (Bloom=3, Ribbon=101).

### 2.4 File Structure

```
hbase-client/
└── regionserver/
    ├── BloomType.java                    # Key extraction strategy
    └── BloomFilterImpl.java              # Filter implementation enum

hbase-server/
├── io/hfile/
│   ├── CompoundBloomFilter.java          # Existing (no changes)
│   ├── CompoundBloomFilterBase.java      # Existing (no changes)
│   ├── CompoundBloomFilterWriter.java    # Existing (no changes)
│   ├── CompoundRibbonFilter.java         # New: Compound Ribbon Filter for reading
│   ├── CompoundRibbonFilterBase.java     # New: Common base class
│   └── CompoundRibbonFilterWriter.java   # New: Compound Ribbon Filter for writing
└── util/
    ├── BloomFilterFactory.java           # Added Ribbon creation logic
    └── ribbon/
        ├── InterleavedRibbonSolution.java # New: ICML storage and back substitution
        ├── RibbonBanding.java             # New: Banding matrix and Gaussian elimination
        ├── RibbonFilterChunk.java         # New: Single chunk Ribbon Filter
        ├── RibbonFilterUtil.java          # New: Utilities
        └── RibbonHasher.java              # New: Hash function
```

---

## 3. Benchmark Results

### 3.1 Test Environment

| Item | Specification                    |
|------|----------------------------------|
| OS | Rocky Linux 8.10                 |
| CPU | Intel Xeon Silver 4410Y (24 cores / 48 threads) |
| RAM | 256GB                            |
| JVM | OpenJDK 21.0.6 (Temurin) with Generational ZGC |
| Measurement | 10 iterations                    |

### 3.2 StoreFile I/O Benchmark

Measures actual HFile write/read performance.

#### 3.2.1 Target FPR: 1%

**ROW Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 16.00 | 13.11 | 1282.35 | 38464.74 | 0.14% |
| | Ribbon | 1 | 8.98 | 7.36 | 1208.15 | 36892.12 | 1.04% |
| | **Diff** | | **-43.8%** | | **-5.8%** | **-4.1%** | |
| 100K | Bloom | 1 | 128.00 | 10.49 | 456.87 | 39406.42 | 0.72% |
| | Ribbon | 1 | 89.30 | 7.32 | 480.35 | 36525.00 | 0.96% |
| | **Diff** | | **-30.2%** | | **+5.1%** | **-7.3%** | |
| 1M | Bloom | 10 | 1184.00 | 9.70 | 393.38 | 36308.41 | 1.00% |
| | Ribbon | 8 | 893.11 | 7.32 | 421.26 | 36830.36 | 0.96% |
| | **Diff** | | **-24.6%** | | **+7.1%** | **+1.4%** | |
| 10M | Bloom | 92 | 11712.00 | 9.59 | 488.49 | 38968.36 | 1.07% |
| | Ribbon | 73 | 8930.68 | 7.32 | 529.74 | 36988.50 | 1.04% |
| | **Diff** | | **-23.7%** | | **+8.4%** | **-5.1%** | |

**ROWCOL Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 16.00 | 13.11 | 1839.74 | 45252.39 | 0.45% |
| | Ribbon | 1 | 10.31 | 8.45 | 1343.47 | 43449.74 | 1.13% |
| | **Diff** | | **-35.5%** | | **-27.0%** | **-4.0%** | |
| 100K | Bloom | 2 | 136.00 | 11.14 | 1236.91 | 43709.29 | 0.91% |
| | Ribbon | 1 | 102.91 | 8.43 | 849.44 | 44318.91 | 1.07% |
| | **Diff** | | **-24.3%** | | **-31.3%** | **+1.4%** | |
| 1M | Bloom | 11 | 1408.00 | 11.53 | 1168.10 | 43475.45 | 1.00% |
| | Ribbon | 9 | 1029.05 | 8.43 | 805.36 | 42599.61 | 1.26% |
| | **Diff** | | **-26.9%** | | **-31.1%** | **-2.0%** | |
| 10M | Bloom | 106 | 13472.00 | 11.04 | 1326.47 | 45979.57 | 0.81% |
| | Ribbon | 84 | 10290.52 | 8.43 | 946.16 | 43730.59 | 0.91% |
| | **Diff** | | **-23.6%** | | **-28.7%** | **-4.9%** | |

#### 3.2.2 Target FPR: 10%

**ROW Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 8.00 | 6.55 | 871.23 | 36922.04 | 4.95% |
| | Ribbon | 1 | 4.50 | 3.69 | 1085.90 | 37694.59 | 10.52% |
| | **Diff** | | **-43.8%** | | **+24.6%** | **+2.1%** | |
| 100K | Bloom | 1 | 64.00 | 5.24 | 373.65 | 37876.42 | 8.06% |
| | Ribbon | 1 | 44.66 | 3.66 | 576.34 | 36865.26 | 9.21% |
| | **Diff** | | **-30.2%** | | **+54.2%** | **-2.7%** | |
| 1M | Bloom | 5 | 640.00 | 5.24 | 379.89 | 37368.42 | 9.03% |
| | Ribbon | 5 | 446.41 | 3.66 | 559.96 | 35826.76 | 10.05% |
| | **Diff** | | **-30.2%** | | **+47.4%** | **-4.1%** | |
| 10M | Bloom | 46 | 5888.00 | 4.82 | 476.82 | 38398.37 | 9.56% |
| | Ribbon | 42 | 4463.87 | 3.66 | 677.70 | 36016.49 | 10.22% |
| | **Diff** | | **-24.2%** | | **+42.1%** | **-6.2%** | |

**ROWCOL Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 8.00 | 6.55 | 1710.33 | 43041.37 | 8.62% |
| | Ribbon | 1 | 5.77 | 4.72 | 1367.91 | 43548.51 | 10.15% |
| | **Diff** | | **-27.9%** | | **-20.0%** | **+1.2%** | |
| 100K | Bloom | 1 | 128.00 | 10.49 | 1246.45 | 43918.14 | 1.67% |
| | Ribbon | 1 | 57.45 | 4.71 | 850.77 | 43814.79 | 10.30% |
| | **Diff** | | **-55.1%** | | **-31.7%** | **-0.2%** | |
| 1M | Bloom | 6 | 768.00 | 6.29 | 1174.11 | 42474.99 | 9.98% |
| | Ribbon | 6 | 574.24 | 4.70 | 838.11 | 42034.38 | 9.94% |
| | **Diff** | | **-25.2%** | | **-28.6%** | **-1.0%** | |
| 10M | Bloom | 60 | 7616.00 | 6.24 | 1322.78 | 43453.65 | 10.46% |
| | Ribbon | 52 | 5742.04 | 4.70 | 962.02 | 43946.52 | 10.53% |
| | **Diff** | | **-24.6%** | | **-27.3%** | **+1.1%** | |

#### 3.2.3 Target FPR: 50%

**ROW Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 2.00 | 1.64 | 805.30 | 35839.56 | 46.51% |
| | Ribbon | 1 | 1.31 | 1.08 | 961.38 | 35417.70 | 49.60% |
| | **Diff** | | **-34.4%** | | **+19.4%** | **-1.2%** | |
| 100K | Bloom | 1 | 32.00 | 2.62 | 354.80 | 36912.93 | 31.36% |
| | Ribbon | 1 | 13.03 | 1.07 | 530.20 | 36664.64 | 50.08% |
| | **Diff** | | **-59.3%** | | **+49.4%** | **-0.7%** | |
| 1M | Bloom | 2 | 192.00 | 1.57 | 423.09 | 37484.77 | 48.46% |
| | Ribbon | 2 | 130.20 | 1.07 | 649.38 | 35373.39 | 50.97% |
| | **Diff** | | **-32.2%** | | **+53.5%** | **-5.6%** | |
| 10M | Bloom | 14 | 1792.00 | 1.47 | 462.93 | 41062.10 | 49.79% |
| | Ribbon | 11 | 1301.85 | 1.07 | 690.15 | 36613.71 | 50.89% |
| | **Diff** | | **-27.4%** | | **+49.1%** | **-10.8%** | |

**ROWCOL Type**

| Keys | Filter | Chunks | Size (KB) | Bits/Key | Write (ns/key) | Lookup (ns/key) | FPR |
|------|--------|--------|-----------|----------|----------------|-----------------|-----|
| 10K | Bloom | 1 | 4.00 | 3.28 | 1716.57 | 42368.17 | 36.39% |
| | Ribbon | 1 | 2.41 | 1.97 | 1289.84 | 41235.97 | 50.18% |
| | **Diff** | | **-39.8%** | | **-24.9%** | **-2.7%** | |
| 100K | Bloom | 1 | 32.00 | 2.62 | 1206.07 | 45521.77 | 48.74% |
| | Ribbon | 1 | 23.88 | 1.96 | 864.66 | 42148.72 | 50.59% |
| | **Diff** | | **-25.4%** | | **-28.3%** | **-7.4%** | |
| 1M | Bloom | 3 | 320.00 | 2.62 | 1207.71 | 40918.14 | 49.38% |
| | Ribbon | 3 | 238.71 | 1.96 | 880.76 | 41136.89 | 49.90% |
| | **Diff** | | **-25.4%** | | **-27.1%** | **+0.5%** | |
| 10M | Bloom | 25 | 3136.00 | 2.57 | 1299.62 | 42997.68 | 49.94% |
| | Ribbon | 21 | 2387.02 | 1.96 | 971.37 | 41283.98 | 49.66% |
| | **Diff** | | **-23.9%** | | **-25.3%** | **-4.0%** | |

### 3.3 Analysis

- **Space Savings**: Ribbon achieves **24-30% smaller size** than Bloom across all configurations.
- **ROW Type**: Write performance is similar at 1% FPR, but slower at higher FPR (10%, 50%). Lookup performance is comparable.
- **ROWCOL Type**: Ribbon shows **25-30% faster writes** consistently. Bloom has per-byte overhead for composite keys, while Ribbon extracts the key at once.
- **Lookup**: Performance is comparable across all configurations as it is dominated by disk I/O.
- **FPR Accuracy**: Ribbon consistently achieves close to target FPR. Bloom sometimes shows lower FPR due to block size over-provisioning (e.g., ROWCOL 10% FPR with 100K keys shows 1.67% due to folding logic constraints).

While Bloom Filter typically offers faster construction and lookup times than Ribbon Filter in theory, this implementation shows comparable performance with no significant CPU overhead. Combined with 24-30% space savings, Ribbon Filter is a practical choice for most workloads—particularly ROWCOL type, where it outperforms Bloom in both space and write performance.

---

## 4. Compatibility

### 4.1 Backward Compatibility

- Ribbon filter reuses existing `BLOOM_CHUNK` block type, distinguished by VERSION field (101)
- Existing `BLOOMFILTER => 'ROW'` settings work without changes

### 4.2 Forward Compatibility

- Older HBase versions cannot read Ribbon filter metadata
- If rollback is required, change Ribbon filter to Bloom filter and run major compaction to ensure all HFiles use Bloom filter before rolling back

```ruby
# Change Ribbon to Bloom before rollback
alter 'mytable', {NAME => 'cf', BLOOMFILTER => 'ROW'}
major_compact 'mytable'
```

### 4.3 Rolling Upgrade

1. Upgrade all RegionServers to the new version
2. Enable Ribbon filter on specific tables
3. After major compaction, all HFiles will use Ribbon

```ruby
# Enable Ribbon filter
alter 'mytable', {NAME => 'cf', BLOOMFILTER_IMPL => 'RIBBON'}
major_compact 'mytable'
```

---

## 5. Future Improvements

### 5.1 Bandwidth 128 Support

Currently only bandwidth **64** is supported. Supporting **128** would enable additional space savings with lower overhead ratio, but increases computational complexity.

---

## 6. References

1. [Ribbon filter: practically smaller than Bloom and Xor](https://arxiv.org/abs/2103.02515) - Peter C. Dillinger, Stefan Walzer (ICLR 2021)
2. [FastFilter/fastfilter_cpp](https://github.com/FastFilter/fastfilter_cpp) - Ribbon Filter reference implementation
3. [RocksDB Ribbon Filter](https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter#ribbon-filter)

---

## Appendix A. How Ribbon Filter Works

Ribbon Filter uses **XOR probing**: a query returns `true` if the XOR of all probed memory locations equals the key's fingerprint.

**Key Concepts:**

1. **Ribbon Matrix Structure**: Each key is hashed to a *starting position* and a *coefficient vector* of width *w* (e.g., 64 bits). This creates a band-like ("ribbon") pattern in the matrix where non-zero entries are confined to a diagonal band.

```
        ←───────── m columns ─────────→
    ┌────────────────────────────────────┐
    │ ████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ ← key 1 (start=0, width=w)
    │ ░░░████░░░░░░░░░░░░░░░░░░░░░░░░░░░ │ ← key 2
    │ ░░░░░████░░░░░░░░░░░░░░░░░░░░░░░░░ │ ← key 3
n   │ ░░░░░░░░░████░░░░░░░░░░░░░░░░░░░░░ │
    │ ░░░░░░░░░░░░░████░░░░░░░░░░░░░░░░░ │
    │ ░░░░░░░░░░░░░░░░░░████░░░░░░░░░░░░ │
    │ ░░░░░░░░░░░░░░░░░░░░░░████░░░░░░░░ │
    └────────────────────────────────────┘
```

2. **Construction (Gaussian Elimination)**: Solve the linear system over GF(2) to find solution matrix Z such that for each key x: `h(x) · Z = fingerprint(x)`. The "Boolean Banding on the fly" algorithm processes keys incrementally without pre-sorting.

3. **Query**: For a key x, compute `h(x) · Z` by XORing the relevant rows of Z. If the result matches `fingerprint(x)`, return `true` (possibly a false positive); otherwise return `false` (definitely not in the set).

4. **Homogeneous Variant**: Sets all fingerprints to 0, simplifying construction (always succeeds) and eliminating fingerprint storage overhead. This is the variant implemented in HBase.

5. **Interleaved Column Major Layout (ICML)**: A storage format that interleaves result bits across columns rather than storing them row by row. This provides better cache locality during queries (accessing contiguous memory for a single key lookup) and allows flexible fingerprint bit widths without wasting space. HBase uses ICML for optimal space efficiency.
