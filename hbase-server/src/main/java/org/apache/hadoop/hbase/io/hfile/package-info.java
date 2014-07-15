/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Provides implementations of {@link org.apache.hadoop.hbase.io.hfile.HFile} and HFile
 * {@link org.apache.hadoop.hbase.io.hfile.BlockCache}.  Caches are configured (and instantiated)
 * by {@link org.apache.hadoop.hbase.io.hfile.CacheConfig}.  See head of the
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig} class for constants that define
 * cache options and configuration keys to use setting cache options. Cache implementations
 * include the default, native on-heap {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache} and a
 * {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache} that has a bunch of deploy formats
 * including acting as a L2 for LruBlockCache -- when a block is evicted from LruBlockCache, it
 * goes to the BucketCache and when we search a block, we look in both places -- or
 * using {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}, BucketCache is used as
 * a host for data blocks with meta blocks in an instance of LruBlockCache.  BucketCache
 * can also be onheap, offheap, and file-backed.
 * 
 * <h1>Which BlockCache should I use?</h1>
 * By default LruBlockCache is on.  If you would like to cache more, and offheap, try enabling
 * BucketCache. Fetching will always
 * be slower when fetching from BucketCache but latencies tend to be less erratic over time
 * (roughly because GC is less). See Nick Dimiduk's
 * <a href="http://www.n10k.com/blog/blockcache-101/">BlockCache 101</a> for some numbers.
 *
 * <h1>Enabling {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}</h2>
 * Read the options and defaults for BucketCache in the head of the
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig}.
 * 
 * <p>Here is a simple example of how to enable a <code>4G</code> offheap bucket cache with 1G
 * onheap cache managed by {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}.
 * CombinedBlockCache will put DATA blocks in the BucketCache and META blocks -- INDEX and BLOOMS
 * -- in an instance of the LruBlockCache. For the
 * CombinedBlockCache (from the class comment), "[t]he smaller lruCache is used
 * to cache bloom blocks and index blocks, the larger bucketCache is used to
 * cache data blocks. getBlock reads first from the smaller lruCache before
 * looking for the block in the bucketCache. Metrics are the combined size and
 * hits and misses of both caches."  To disable CombinedBlockCache and have the BucketCache act
 * as a strict L2 cache to the L1 LruBlockCache (i.e. on eviction from L1, blocks go to L2), set
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig#BUCKET_CACHE_COMBINED_KEY} to false. By
 * default, hbase.bucketcache.combinedcache.enabled (BUCKET_CACHE_COMBINED_KEY) is true.
 * 
 * <p>Back to the example of setting an onheap cache of 1G and offheap of 4G with the BlockCache
 * deploy managed by CombinedBlockCache. Setting hbase.bucketcache.ioengine and
 * hbase.bucketcache.size > 0 enables CombinedBlockCache.
 * In <code>hbase-env.sh</code> ensure the environment
 * variable <code>-XX:MaxDirectMemorySize</code> is enabled and is bigger than 4G, say 5G in size:
 * e.g. <code>-XX:MaxDirectMemorySize=5G</code>. This setting allows the JVM use offheap memory
 * up to this upper limit.  Allocate more than you need because there are other consumers of
 * offheap memory other than BlockCache (for example DFSClient in the RegionServer uses offheap).
 * In <code>hbase-site.xml</code> add the following configurations:
<pre>&lt;property>
  &lt;name>hbase.bucketcache.ioengine&lt;/name>
  &lt;value>offheap&lt;/value>
&lt;/property>
&lt;property>
  &lt;name>hbase.bucketcache.percentage.in.combinedcache&lt;/name>
  &lt;value>0.8&lt;/value>
&lt;/property>
&lt;property>
  &lt;name>hbase.bucketcache.size&lt;/name>
  &lt;value>5120&lt;/value>
&lt;/property></pre>.  Above we set a cache of 5G, 80% of which will be offheap (4G) and 1G onheap
 * (with DATA blocks in BucketCache and INDEX blocks in the onheap LruBlockCache).
 * Restart (or rolling restart) your cluster for the configs to take effect.  Check logs to ensure
 * your configurations came out as expected.
 *
 */
package org.apache.hadoop.hbase.io.hfile;
