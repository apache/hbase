/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CompositeBucketCache extends CompositeBlockCache {
  public static final String IOENGINE_L1 = "hbase.bucketcache.l1.ioengine";
  public static final String IOENGINE_L2 = "hbase.bucketcache.l2.ioengine";
  public static final String CACHESIZE_L1 = "hbase.bucketcache.l1.size";
  public static final String CACHESIZE_L2 = "hbase.bucketcache.l2.size";
  public static final String WRITER_THREADS_L1 = "hbase.bucketcache.l1.writer.threads";
  public static final String WRITER_THREADS_L2 = "hbase.bucketcache.l2.writer.threads";
  public static final String WRITER_QUEUE_LENGTH_L1 = "hbase.bucketcache.l1.writer.queuelength";
  public static final String WRITER_QUEUE_LENGTH_L2 = "hbase.bucketcache.l2.writer.queuelength";
  public static final String PERSISTENT_PATH_L1 = "hbase.bucketcache.l1.persistent.path";
  public static final String PERSISTENT_PATH_L2 = "hbase.bucketcache.l2.persistent.path";

  public CompositeBucketCache(BucketCache l1Cache, BucketCache l2Cache) {
    super(l1Cache, l2Cache);
  }
}
