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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestNullReaderCompactionPolicies {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestNullReaderCompactionPolicies.class);

  @Test
  public void testExploringSelectCompactionSkipsNullReaderFiles() throws IOException {
    ExploringCompactionPolicy policy = new ExploringCompactionPolicy(createConf(), createStore());
    HStoreFile nullReaderA = createStoreFile("nullA", 1L, null, null, null);
    HStoreFile nullReaderB = createStoreFile("nullB", 2L, null, null, null);
    HStoreFile nullReaderC = createStoreFile("nullC", 3L, null, null, null);
    HStoreFile newerA = createStoreFile("newerA", 4L, 10L, 10L, 1L);
    HStoreFile newerB = createStoreFile("newerB", 5L, 10L, 10L, 1L);
    HStoreFile newerC = createStoreFile("newerC", 6L, 10L, 10L, 1L);

    CompactionRequestImpl request = policy.selectCompaction(
      Arrays.asList(nullReaderA, nullReaderB, nullReaderC, newerA, newerB, newerC),
      Collections.emptyList(), false, false, false);

    assertEquals(Arrays.asList(newerA, newerB, newerC), new ArrayList<>(request.getFiles()));
  }

  private static Configuration createConf() {
    Configuration conf = HBaseConfiguration.create();
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_SIZE_KEY, 100L);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, 1L);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 5);
    conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.0F);
    return conf;
  }

  private static StoreConfigInformation createStore() {
    StoreConfigInformation store = mock(StoreConfigInformation.class);
    when(store.getMemStoreFlushSize()).thenReturn(128L);
    when(store.getBlockingFileCount()).thenReturn(10L);
    when(store.getStoreFileTtl()).thenReturn(Long.MAX_VALUE);
    when(store.getRegionInfo()).thenReturn(HRegionInfo.FIRST_META_REGIONINFO);
    when(store.getColumnFamilyName()).thenReturn("cf");
    return store;
  }

  private static HStoreFile createStoreFile(String name, long sequenceId, Long length, Long maxTs,
    Long entries) {
    HStoreFile storeFile = mock(HStoreFile.class);
    when(storeFile.getPath()).thenReturn(new Path("/hbase/" + name));
    when(storeFile.getMaxSequenceId()).thenReturn(sequenceId);
    when(storeFile.isReference()).thenReturn(false);
    when(storeFile.excludeFromMinorCompaction()).thenReturn(false);
    if (length == null) {
      when(storeFile.getReader()).thenReturn(null);
    } else {
      StoreFileReader reader = mock(StoreFileReader.class);
      when(reader.length()).thenReturn(length);
      when(reader.getMaxTimestamp()).thenReturn(maxTs);
      when(reader.getEntries()).thenReturn(entries.longValue());
      when(storeFile.getReader()).thenReturn(reader);
    }
    return storeFile;
  }
}
