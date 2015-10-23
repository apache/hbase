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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactor;
import org.apache.hadoop.hbase.security.User;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestStripeStoreEngine {

  @Test
  public void testCreateBasedOnConfig() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, TestStoreEngine.class.getName());
    StripeStoreEngine se = createEngine(conf);
    assertTrue(se.getCompactionPolicy() instanceof StripeCompactionPolicy);
  }

  public static class TestStoreEngine extends StripeStoreEngine {
    public void setCompactorOverride(StripeCompactor compactorOverride) {
      this.compactor = compactorOverride;
    }
  }

  @Test
  public void testCompactionContextForceSelect() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int targetCount = 2;
    conf.setInt(StripeStoreConfig.INITIAL_STRIPE_COUNT_KEY, targetCount);
    conf.setInt(StripeStoreConfig.MIN_FILES_L0_KEY, 2);
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY, TestStoreEngine.class.getName());
    TestStoreEngine se = createEngine(conf);
    StripeCompactor mockCompactor = mock(StripeCompactor.class);
    se.setCompactorOverride(mockCompactor);
    when(mockCompactor.compact(any(CompactionRequest.class), anyInt(), anyLong(),
        any(byte[].class), any(byte[].class), any(byte[].class), any(byte[].class),any(User.class)))
        .thenReturn(new ArrayList<Path>());

    // Produce 3 L0 files.
    StoreFile sf = createFile();
    ArrayList<StoreFile> compactUs = al(sf, createFile(), createFile());
    se.getStoreFileManager().loadFiles(compactUs);
    // Create a compaction that would want to split the stripe.
    CompactionContext compaction = se.createCompaction();
    compaction.select(al(), false, false, false);
    assertEquals(3, compaction.getRequest().getFiles().size());
    // Override the file list. Granted, overriding this compaction in this manner will
    // break things in real world, but we only want to verify the override.
    compactUs.remove(sf);
    CompactionRequest req = new CompactionRequest(compactUs);
    compaction.forceSelect(req);
    assertEquals(2, compaction.getRequest().getFiles().size());
    assertFalse(compaction.getRequest().getFiles().contains(sf));
    // Make sure the correct method it called on compactor.
    compaction.compact();
    verify(mockCompactor, times(1)).compact(compaction.getRequest(), targetCount, 0L,
          StripeStoreFileManager.OPEN_KEY, StripeStoreFileManager.OPEN_KEY, null, null, null);
  }

  private static StoreFile createFile() throws Exception {
    StoreFile sf = mock(StoreFile.class);
    when(sf.getMetadataValue(any(byte[].class)))
      .thenReturn(StripeStoreFileManager.INVALID_KEY);
    when(sf.getReader()).thenReturn(mock(StoreFile.Reader.class));
    when(sf.getPath()).thenReturn(new Path("moo"));
    return sf;
  }

  private static TestStoreEngine createEngine(Configuration conf) throws Exception {
    Store store = mock(Store.class);
    KVComparator kvComparator = mock(KVComparator.class);
    return (TestStoreEngine)StoreEngine.create(store, conf, kvComparator);
  }

  private static ArrayList<StoreFile> al(StoreFile... sfs) {
    return new ArrayList<StoreFile>(Arrays.asList(sfs));
  }
}
