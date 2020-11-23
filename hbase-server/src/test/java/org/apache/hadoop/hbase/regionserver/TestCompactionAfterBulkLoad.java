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
package org.apache.hadoop.hbase.regionserver;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequester;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

@Category(SmallTests.class)
public class TestCompactionAfterBulkLoad extends TestBulkloadBase {
  private final RegionServerServices regionServerServices = mock(RegionServerServices.class);
  private final CompactionRequester compactionRequester = mock(CompactSplit.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestCompactionAfterBulkLoad.class);

  @Override
  protected HRegion testRegionWithFamiliesAndSpecifiedTableName(TableName tableName,
      byte[]... families) throws IOException {
    RegionInfo hRegionInfo = RegionInfoBuilder.newBuilder(tableName).build();
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);

    for (byte[] family : families) {
      builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(family));
    }
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
    // TODO We need a way to do this without creating files
    return HRegion.createHRegion(hRegionInfo, new Path(testFolder.newFolder().toURI()), conf,
      builder.build(), log, true, regionServerServices);

  }

  @Test
  public void shouldRequestCompactionAfterBulkLoad() throws IOException {
    List<Pair<byte[], String>> familyPaths = new ArrayList<>();
    // enough hfile to request compaction
    for (int i = 0; i < 5; i++) {
      familyPaths.addAll(withFamilyPathsFor(family1, family2, family3));
    }
    when(regionServerServices.getConfiguration()).thenReturn(conf);
    when(regionServerServices.getCompactionRequestor()).thenReturn(compactionRequester);
    when(log.appendMarker(any(), any(), argThat(bulkLogWalEditType(WALEdit.BULK_LOAD))))
        .thenAnswer(new Answer() {
          @Override
          public Object answer(InvocationOnMock invocation) {
            WALKeyImpl walKey = invocation.getArgument(1);
            MultiVersionConcurrencyControl mvcc = walKey.getMvcc();
            if (mvcc != null) {
              MultiVersionConcurrencyControl.WriteEntry we = mvcc.begin();
              walKey.setWriteEntry(we);
            }
            return 01L;
          }
        });

    Mockito.doNothing().when(compactionRequester).requestCompaction(any(), any(), any(), anyInt(),
      any(), any());
    testRegionWithFamilies(family1, family2, family3).bulkLoadHFiles(familyPaths, false, null);
    // invoke three times for 3 families
    verify(compactionRequester, times(3)).requestCompaction(isA(HRegion.class), isA(HStore.class),
      isA(String.class), anyInt(), eq(CompactionLifeCycleTracker.DUMMY), eq(null));
  }
}
