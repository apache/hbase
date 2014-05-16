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
 * cache options and configuration keys to use setting cache options.  Cache implementations
 * include the default, native on-heap {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache},
 * a {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} that can serve as an L2 for
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache} (hosted inside the class
 * {@link org.apache.hadoop.hbase.io.hfile.DoubleBlockCache} that caches blocks in BOTH L1 and L2,
 * and on evict, moves from L1 to L2, etc), and a
 * {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache} that has a bunch of deploy formats
 * including acting as a L2 for LruBlockCache -- when a block is evicted from LruBlockCache, it
 * goes to the BucketCache and when we search a block, we look in both places -- or using
 * {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}, as
 * a host for data blocks with meta blocks in the LRUBlockCache as well as onheap, offheap, and
 * file options.
 * 
 * <h1>Which BlockCache should I use?</h1>
 * BucketCache has seen more production deploys and has more deploy options.  Fetching will always
 * be slower when fetching from BucketCache but latencies tend to be less erratic over time
 * (roughly because GC is less).  SlabCache tends to do more GCs as blocks are moved between L1
 * and L2 always, at least given the way {@link org.apache.hadoop.hbase.io.hfile.DoubleBlockCache}
 * currently works. It is tough doing an apples to apples compare since their hosting classes,
 * {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache} for BucketCache vs
 * {@link org.apache.hadoop.hbase.io.hfile.DoubleBlockCache} operate so differently.
 * See Nick Dimiduk's
 * <a href="http://www.n10k.com/blog/blockcache-101/">BlockCache 101</a> for some numbers. See
 * also the description of <a href="https://issues.apache.org/jira/browse/HBASE-7404">HBASE-7404</a>
 * where Chunhui Shen lists issues he found with BlockCache (inefficent use of memory, doesn't
 * help w/ GC).
 *
 * <h1>Enabling {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache}</h2>
 * {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} is the original offheap block cache
 * but unfortunately has seen little use. It is originally described in
 * <a href="http://blog.cloudera.com/blog/2012/01/caching-in-hbase-slabcache/">Caching
 * in Apache HBase: SlabCache</a>.To enable it,
 * set the float <code>hbase.offheapcache.percentage</code>
 * ({@link CacheConfig#SLAB_CACHE_OFFHEAP_PERCENTAGE_KEY}) to some value between 0 and 1 in
 * your <code>hbase-site.xml</code> file. This
 * enables {@link org.apache.hadoop.hbase.io.hfile.DoubleBlockCache}, a facade over
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache} and
 * {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache}.  DoubleBlockCache works as follows.
 * When caching, it  
 * "...attempts to cache the block in both caches, while readblock reads first from the faster
 * onheap cache before looking for the block in the off heap cache. Metrics are the
 * combined size and hits and misses of both caches." The value set in
 * <code>hbase.offheapcache.percentage</code> will be
 * multiplied by whatever the setting for <code>-XX:MaxDirectMemorySize</code> is in
 * your <code>hbase-env.sh</code> configuration file and this is what
 * will be used by {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} as its offheap store.
 * Onheap store will be whatever the float {@link HConstants#HFILE_BLOCK_CACHE_SIZE_KEY} setting is
 * (some value between 0 and 1) times the size of the allocated java heap.
 * 
 * <p>Restart (or rolling restart) your cluster for the configs to take effect.  Check logs to
 * ensure your configurations came out as expected.
 *
 * <h1>Enabling {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}</h2>
 * Ensure the SlabCache config <code>hbase.offheapcache.percentage</code> is not set (or set to 0).
 * At this point, it is probably best to read the code to learn the list of bucket cache options
 * and how they combine (to be fixed).  Read the options and defaults for BucketCache in the
 * head of the {@link org.apache.hadoop.hbase.io.hfile.CacheConfig}.
 * 
 * <p>Here is a simple example of how to enable a <code>4G</code>
 * offheap bucket cache with 1G onheap cache.
 * The onheap/offheap caches
 * are managed by {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache} by default. For the
 * CombinedBlockCache (from the class comment), "The smaller lruCache is used
 * to cache bloom blocks and index blocks, the larger bucketCache is used to
 * cache data blocks. getBlock reads first from the smaller lruCache before
 * looking for the block in the bucketCache. Metrics are the combined size and
 * hits and misses of both caches."  To disable CombinedBlockCache and have the BucketCache act
 * as a strict L2 cache to the L1 LruBlockCache (i.e. on eviction from L1, blocks go to L2), set
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig#BUCKET_CACHE_COMBINED_KEY} to false.
 * Also by default, unless you change it,
 * {@link CacheConfig#BUCKET_CACHE_COMBINED_PERCENTAGE_KEY} defaults to <code>0.9</code> (see
 * the top of the CacheConfig in the BucketCache defaults section).  This means that whatever
 * size you set for the bucket cache with
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig#BUCKET_CACHE_SIZE_KEY},
 * <code>90%</code> will be used for offheap and <code>10%</code> of the size will be used
 * by the onheap {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache}.
 * <p>Back to the example of setting an onheap cache of 1G and ofheap of 4G, in 
 * <code>hbase-env.sh</code> ensure the java option <code>-XX:MaxDirectMemorySize</code> is
 * enabled and 5G in size: e.g. <code>-XX:MaxDirectMemorySize=5G</code>.  Then in
 * <code>hbase-site.xml</code> add the following configurations:
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
&lt;/property></pre>.  Above we set a cache of 5G, 80% of which will be offheap (4G) and 1G onheap.
 * Restart (or rolling restart) your cluster for the configs to take effect.  Check logs to ensure
 * your configurations came out as expected.
 *
 */
package org.apache.hadoop.hbase.io.hfile;
