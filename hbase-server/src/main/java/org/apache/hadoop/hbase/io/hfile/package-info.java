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
 * Provides implementations of {@link HFile} and HFile
 * {@link org.apache.hadoop.hbase.io.hfile.BlockCache}.  Caches are configured (and instantiated)
 * by {@link org.apache.hadoop.hbase.io.hfile.CacheConfig}.  See head of the
 * {@link org.apache.hadoop.hbase.io.hfile.CacheConfig} class for constants that define
 * cache options and configuration keys to use setting cache options.  Cache implementations
 * include the on-heap {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache},
 * a {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} that can serve as an L2 for
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache}, and a
 * {@link org.apache.hadoop.hbase.io.hfile.bucket.BucketCache} that has a bunch of deploy types
 * including L2 for LRUBlockCache or using
 * {@link org.apache.hadoop.hbase.io.hfile.CombinedBlockCache}, as
 * host for data blocks with meta blocks in the LRUBlockCache as well as onheap, offheap, and
 * file options).
 * 
 * <h1>Enabling {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache}</h2>
 * {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} has seen little use and will likely
 * be deprecated in the near future.  To enable it,
 * set the float <code>hbase.offheapcache.percentage</code> to some value between 0 and 1. This
 * enables {@link org.apache.hadoop.hbase.io.hfile.DoubleBlockCache}, a facade over
 * {@link org.apache.hadoop.hbase.io.hfile.LruBlockCache} and
 * {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache}.  The value set here will be
 * multiplied by whatever the setting for <code>-XX:MaxDirectMemorySize</code> is and this is what
 * will be used by {@link org.apache.hadoop.hbase.io.hfile.slab.SlabCache} as its offheap store.
 */
package org.apache.hadoop.hbase.io.hfile;
