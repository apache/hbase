/*
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
package org.apache.hadoop.hbase.io.hfile.cache;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.concurrent.Executor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.LruAdaptiveBlockCache;
import org.apache.hadoop.hbase.io.hfile.LruBlockCache;
import org.apache.hadoop.hbase.io.hfile.TinyLfuBlockCache;
import org.apache.hadoop.hbase.io.hfile.bucket.BucketCache;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CacheAccessServiceTestFactory}.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
public class TestCacheAccessServiceTestFactory {

  private static final long MAX_SIZE = 1024L * 1024L;
  private static final long BUCKET_CACHE_SIZE = 64L * 1024L * 1024L;
  private static final long BLOCK_SIZE = 4096L;
  private static final Executor DIRECT_EXECUTOR = Runnable::run;

  @Test
  void testDisabled() {
    CacheAccessService service = CacheAccessServiceTestFactory.disabled();

    assertNotNull(service);
    assertFalse(service.isCacheEnabled());
  }

  @Test
  void testFromBlockCacheRejectsNull() {
    assertThrows(NullPointerException.class,
      () -> CacheAccessServiceTestFactory.fromBlockCache(null));
  }

  @Test
  void testFromConfigurationRejectsNull() {
    assertThrows(NullPointerException.class,
      () -> CacheAccessServiceTestFactory.fromConfiguration(null));
  }

  @Test
  void testLruServiceOnlyFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessService service =
      CacheAccessServiceTestFactory.lru(MAX_SIZE, BLOCK_SIZE, true, conf);

    try {
      assertNotNull(service);
      assertTrue(service.isCacheEnabled());
    } finally {
      service.shutdown();
    }
  }

  @Test
  void testLruInstanceFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessServiceTestInstance<LruBlockCache> instance =
      CacheAccessServiceTestFactory.lruInstance(MAX_SIZE, BLOCK_SIZE, true, conf);

    try {
      assertNotNull(instance);
      assertNotNull(instance.blockCache());
      assertNotNull(instance.service());
      assertInstanceOf(LruBlockCache.class, instance.blockCache());
      assertTrue(instance.service().isCacheEnabled());
    } finally {
      instance.service().shutdown();
    }
  }

  @Test
  void testLruAdaptiveServiceOnlyFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessService service =
      CacheAccessServiceTestFactory.lruAdaptive(MAX_SIZE, BLOCK_SIZE, true, conf);

    try {
      assertNotNull(service);
      assertTrue(service.isCacheEnabled());
    } finally {
      service.shutdown();
    }
  }

  @Test
  void testLruAdaptiveInstanceFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessServiceTestInstance<LruAdaptiveBlockCache> instance =
      CacheAccessServiceTestFactory.lruAdaptiveInstance(MAX_SIZE, BLOCK_SIZE, true, conf);

    try {
      assertNotNull(instance);
      assertNotNull(instance.blockCache());
      assertNotNull(instance.service());
      assertInstanceOf(LruAdaptiveBlockCache.class, instance.blockCache());
      assertTrue(instance.service().isCacheEnabled());
    } finally {
      instance.service().shutdown();
    }
  }

  @Test
  void testTinyLfuServiceOnlyFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessService service =
      CacheAccessServiceTestFactory.tinyLfu(MAX_SIZE, BLOCK_SIZE, DIRECT_EXECUTOR, conf);

    try {
      assertNotNull(service);
      assertTrue(service.isCacheEnabled());
    } finally {
      service.shutdown();
    }
  }

  @Test
  void testTinyLfuInstanceFactory() {
    Configuration conf = HBaseConfiguration.create();
    CacheAccessServiceTestInstance<TinyLfuBlockCache> instance =
      CacheAccessServiceTestFactory.tinyLfuInstance(MAX_SIZE, BLOCK_SIZE, DIRECT_EXECUTOR, conf);

    try {
      assertNotNull(instance);
      assertNotNull(instance.blockCache());
      assertNotNull(instance.service());
      assertInstanceOf(TinyLfuBlockCache.class, instance.blockCache());
      assertTrue(instance.service().isCacheEnabled());
    } finally {
      instance.service().shutdown();
    }
  }

  @Test
  void testTinyLfuInstanceFactoryRejectsNullExecutor() {
    assertThrows(NullPointerException.class, () -> CacheAccessServiceTestFactory
      .tinyLfuInstance(MAX_SIZE, BLOCK_SIZE, null, HBaseConfiguration.create()));
  }

  @Test
  void testTinyLfuInstanceFactoryRejectsNullConfiguration() {
    assertThrows(NullPointerException.class, () -> CacheAccessServiceTestFactory
      .tinyLfuInstance(MAX_SIZE, BLOCK_SIZE, DIRECT_EXECUTOR, null));
  }

  @Test
  void testBucketServiceOnlyFactory() throws IOException {
    CacheAccessService service = CacheAccessServiceTestFactory.bucket("offheap", BUCKET_CACHE_SIZE,
      (int) BLOCK_SIZE, null, 1, 1, null);

    try {
      assertNotNull(service);
      assertTrue(service.isCacheEnabled());
    } finally {
      service.shutdown();
    }
  }

  @Test
  void testBucketInstanceFactory() throws IOException {
    CacheAccessServiceTestInstance<BucketCache> instance = CacheAccessServiceTestFactory
      .bucketInstance("offheap", BUCKET_CACHE_SIZE, (int) BLOCK_SIZE, null, 1, 1, null);

    try {
      assertNotNull(instance);
      assertNotNull(instance.blockCache());
      assertNotNull(instance.service());
      assertInstanceOf(BucketCache.class, instance.blockCache());
      assertTrue(instance.service().isCacheEnabled());
    } finally {
      instance.service().shutdown();
    }
  }

  @Test
  void testBucketInstanceFactoryRejectsNullIoEngineName() {
    assertThrows(NullPointerException.class, () -> CacheAccessServiceTestFactory
      .bucketInstance(null, MAX_SIZE, (int) BLOCK_SIZE, null, 1, 1, null));
  }

  @Test
  void testInstanceRejectsNullBlockCache() {
    assertThrows(NullPointerException.class, () -> new CacheAccessServiceTestInstance<>(null));
  }
}
