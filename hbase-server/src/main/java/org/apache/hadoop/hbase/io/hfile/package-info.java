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
 * goes to the BucketCache and when we search a block, we look in both places -- or, the
 * most common deploy type,
 * using {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}, BucketCache is used as
 * a host for data blocks with meta blocks in an instance of LruBlockCache.  BucketCache
 * can also be onheap, offheap, and file-backed.
 * 
 * <h1>Which BlockCache should I use?</h1>
 * By default LruBlockCache is on.  If you would like to cache more, and offheap (offheap
 * usually means less GC headache), try enabling * BucketCache. Fetching will always
 * be slower when fetching from BucketCache but latencies tend to be less erratic over time
 * (roughly because GC is less). See Nick Dimiduk's
 * <a href="http://www.n10k.com/blog/blockcache-101/">BlockCache 101</a> for some numbers.
 *
 * <h1>Enabling {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache}</h1>
 * See the HBase Reference Guide <a href="http://hbase.apache.org/book.html#enable.bucketcache">Enable BucketCache</a>.
 *
 */
package org.apache.hadoop.hbase.io.hfile;
