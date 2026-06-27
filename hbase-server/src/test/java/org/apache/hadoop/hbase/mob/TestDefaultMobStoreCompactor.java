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
package org.apache.hadoop.hbase.mob;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.HMobStore;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
public class TestDefaultMobStoreCompactor {

  @Test
  public void testCacheMobBlocksOnCompactionDefaultsToTrue() {
    DefaultMobStoreCompactor compactor = newCompactor(new Configuration());

    assertTrue(compactor.cacheMobBlocksOnCompaction);
  }

  @Test
  public void testCacheMobBlocksOnCompactionCanBeDisabled() {
    Configuration conf = new Configuration();
    conf.setBoolean(MobConstants.MOB_COMPACTION_READ_CACHE_BLOCKS, false);
    DefaultMobStoreCompactor compactor = newCompactor(conf);

    assertFalse(compactor.cacheMobBlocksOnCompaction);
  }

  @Test
  public void testResolveMobCellClosesMobCellAndReturnsIndependentCopy() throws Exception {
    Configuration conf = new Configuration();
    DefaultMobStoreCompactor compactor = newCompactor(conf);
    ExtendedCell reference = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
      Bytes.toBytes("qualifier"), Bytes.toBytes("mob-reference"));
    ExtendedCell resolved = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"),
      Bytes.toBytes("qualifier"), Bytes.toBytes("mob-value"));
    MobCell mobCell = mock(MobCell.class);
    when(mobCell.getCell()).thenReturn(resolved);
    when(compactor.mobStore.resolve(reference, compactor.cacheMobBlocksOnCompaction, false))
      .thenReturn(mobCell);

    ExtendedCell copied = compactor.resolveMobCell(reference);

    assertNotSame(resolved, copied);
    assertTrue(CellUtil.matchingValue(resolved, copied));
    verify(compactor.mobStore).resolve(reference, compactor.cacheMobBlocksOnCompaction, false);
    verify(mobCell).close();
  }

  private DefaultMobStoreCompactor newCompactor(Configuration conf) {
    HMobStore store = mock(HMobStore.class);
    ColumnFamilyDescriptor family = mock(ColumnFamilyDescriptor.class);
    when(store.getColumnFamilyDescriptor()).thenReturn(family);
    when(family.getMajorCompactionCompressionType()).thenReturn(Compression.Algorithm.NONE);
    when(family.getMinorCompactionCompressionType()).thenReturn(Compression.Algorithm.NONE);
    when(family.getMobThreshold()).thenReturn(123L);
    return new DefaultMobStoreCompactor(conf, store);
  }
}
