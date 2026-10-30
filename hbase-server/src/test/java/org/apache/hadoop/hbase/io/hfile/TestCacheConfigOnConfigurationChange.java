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
package org.apache.hadoop.hbase.io.hfile;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.params.provider.Arguments;

/**
 * Tests that {@link CacheConfig#onConfigurationChange(Configuration)} keeps the column family
 * settings applied at construction time.
 */
@Tag(IOTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(
    name = "{index}: withFamily={0}, blockCache={1}, cacheDataOnWrite={2}, evictOnClose={3}")
public class TestCacheConfigOnConfigurationChange {

  private static final boolean[] BOOLEANS = { false, true };

  private final boolean withFamily;
  private final boolean blockCacheEnabled;
  private final boolean familyCacheDataOnWrite;
  private final boolean familyEvictOnClose;

  private ConfigurationManager manager;
  private CacheConfig[] cacheConfigs;

  public static Stream<Arguments> parameters() {
    List<Arguments> params = new ArrayList<>();
    // Without a column family the block cache is enabled and there are no family settings.
    params.add(Arguments.of(false, true, false, false));
    for (boolean blockCacheEnabled : BOOLEANS) {
      for (boolean cacheDataOnWrite : BOOLEANS) {
        for (boolean evictOnClose : BOOLEANS) {
          params.add(Arguments.of(true, blockCacheEnabled, cacheDataOnWrite, evictOnClose));
        }
      }
    }
    return params.stream();
  }

  public TestCacheConfigOnConfigurationChange(boolean withFamily, boolean blockCacheEnabled,
    boolean familyCacheDataOnWrite, boolean familyEvictOnClose) {
    this.withFamily = withFamily;
    this.blockCacheEnabled = blockCacheEnabled;
    this.familyCacheDataOnWrite = familyCacheDataOnWrite;
    this.familyEvictOnClose = familyEvictOnClose;
  }

  @BeforeEach
  public void setUp() {
    ColumnFamilyDescriptor family = withFamily
      ? ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cf"))
        .setBlockCacheEnabled(blockCacheEnabled).setCacheDataOnWrite(familyCacheDataOnWrite)
        .setEvictBlocksOnClose(familyEvictOnClose).build()
      : null;
    CacheConfig original =
      new CacheConfig(new Configuration(false), family, null, ByteBuffAllocator.HEAP);
    // A copy must carry the column family settings as well.
    cacheConfigs = new CacheConfig[] { original, new CacheConfig(original) };
    manager = new ConfigurationManager();
    for (CacheConfig cacheConfig : cacheConfigs) {
      manager.registerObserver(cacheConfig);
    }
  }

  @TestTemplate
  public void testReloadUnchangedConfiguration() {
    assertCacheSettings(CacheConfig.DEFAULT_CACHE_DATA_ON_READ,
      CacheConfig.DEFAULT_CACHE_DATA_ON_WRITE, CacheConfig.DEFAULT_EVICT_ON_CLOSE);
    manager.notifyAllObservers(new Configuration(false));
    assertCacheSettings(CacheConfig.DEFAULT_CACHE_DATA_ON_READ,
      CacheConfig.DEFAULT_CACHE_DATA_ON_WRITE, CacheConfig.DEFAULT_EVICT_ON_CLOSE);
  }

  @TestTemplate
  public void testReloadChangedConfiguration() {
    // Every global setting is enabled and disabled independently of the other two.
    for (boolean cacheDataOnRead : BOOLEANS) {
      for (boolean cacheDataOnWrite : BOOLEANS) {
        for (boolean evictOnClose : BOOLEANS) {
          Configuration conf = new Configuration(false);
          conf.setBoolean(CacheConfig.CACHE_DATA_ON_READ_KEY, cacheDataOnRead);
          conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, cacheDataOnWrite);
          conf.setBoolean(CacheConfig.EVICT_BLOCKS_ON_CLOSE_KEY, evictOnClose);
          manager.notifyAllObservers(conf);
          assertCacheSettings(cacheDataOnRead, cacheDataOnWrite, evictOnClose);
        }
      }
    }
  }

  private void assertCacheSettings(boolean globalCacheDataOnRead, boolean globalCacheDataOnWrite,
    boolean globalEvictOnClose) {
    for (CacheConfig cacheConfig : cacheConfigs) {
      assertEquals(blockCacheEnabled && globalCacheDataOnRead, cacheConfig.shouldCacheDataOnRead());
      assertEquals(blockCacheEnabled && (globalCacheDataOnWrite || familyCacheDataOnWrite),
        cacheConfig.shouldCacheDataOnWrite());
      assertEquals(blockCacheEnabled && (globalEvictOnClose || familyEvictOnClose),
        cacheConfig.shouldEvictOnClose());
    }
  }
}
