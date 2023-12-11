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

import static org.apache.hadoop.hbase.regionserver.DefaultStoreEngine.DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY;
import static org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.createDummyRequest;
import static org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.createDummyStoreFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.CreateStoreFileWriterParams;
import org.apache.hadoop.hbase.regionserver.HStore;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.StoreEngine;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.Scanner;
import org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.StoreFileWritersCapture;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, SmallTests.class })
public class TestDualFileWriter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestDualFileWriter.class);

  private static final byte[] NAME_OF_THINGS = Bytes.toBytes("foo");

  private static final TableName TABLE_NAME = TableName.valueOf(NAME_OF_THINGS, NAME_OF_THINGS);

  private static final KeyValue KV_A_DeleteFamilyVersion = new KeyValue(Bytes.toBytes("123"),
    Bytes.toBytes("0"), null, 300L, KeyValue.Type.DeleteFamilyVersion);
  private static final KeyValue KV_A_1 = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("a"), 300L, KeyValue.Type.Put);
  private static final KeyValue KV_A_2 = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("a"), 200L, KeyValue.Type.Put);
  private static final KeyValue KV_A_3 = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("a"), 100L, KeyValue.Type.Put);

  private static final KeyValue KV_B_DeleteColumn = new KeyValue(Bytes.toBytes("123"),
    Bytes.toBytes("0"), Bytes.toBytes("b"), 200L, KeyValue.Type.DeleteColumn);
  private static final KeyValue KV_B = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("b"), 100L, KeyValue.Type.Put);

  private static final KeyValue KV_C = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("c"), 100L, KeyValue.Type.Put);

  private static final KeyValue KV_D_1 = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("d"), 200L, KeyValue.Type.Put);
  private static final KeyValue KV_D_2 = new KeyValue(Bytes.toBytes("123"), Bytes.toBytes("0"),
    Bytes.toBytes("d"), 100L, KeyValue.Type.Put);

  private static final KeyValue KV_E_F_DeleteFamily =
    new KeyValue(Bytes.toBytes("456"), Bytes.toBytes("0"), null, 200L, KeyValue.Type.DeleteFamily);
  private static final KeyValue KV_E = new KeyValue(Bytes.toBytes("456"), Bytes.toBytes("0"),
    Bytes.toBytes("e"), 100L, KeyValue.Type.Put);
  private static final KeyValue KV_F = new KeyValue(Bytes.toBytes("456"), Bytes.toBytes("0"),
    Bytes.toBytes("f"), 100L, KeyValue.Type.Put);
  private static final KeyValue KV_G_DeleteFamily =
    new KeyValue(Bytes.toBytes("789"), Bytes.toBytes("0"), null, 400L, KeyValue.Type.DeleteFamily);
  private static final KeyValue KV_G_DeleteFamilyVersion = new KeyValue(Bytes.toBytes("789"),
    Bytes.toBytes("0"), null, 100L, KeyValue.Type.DeleteFamilyVersion);
  private static final KeyValue KV_G_1 = new KeyValue(Bytes.toBytes("789"), Bytes.toBytes("0"),
    Bytes.toBytes("g"), 500L, KeyValue.Type.Put);
  private static final KeyValue KV_G_DeleteColumn =
    new KeyValue(Bytes.toBytes("789"), Bytes.toBytes("0"), null, 300L, KeyValue.Type.DeleteColumn);
  private static final KeyValue KV_G_DeleteColumnVersion =
    new KeyValue(Bytes.toBytes("789"), Bytes.toBytes("0"), null, 200L, KeyValue.Type.Delete);
  private static final KeyValue KV_G_2 = new KeyValue(Bytes.toBytes("789"), Bytes.toBytes("0"),
    Bytes.toBytes("g"), 100L, KeyValue.Type.Put);

  @Parameters(name = "{index}: usePrivateReaders={0}, keepDeletedCells={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @Parameter(0)
  public boolean usePrivateReaders;

  private DefaultCompactor createCompactor(StoreFileWritersCapture writers, final KeyValue[] input,
    List<HStoreFile> storefiles) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.regionserver.compaction.private.readers", usePrivateReaders);
    conf.setBoolean(DEFAULT_COMPACTION_ENABLE_DUAL_FILE_WRITER_KEY, true);
    final Scanner scanner = new Scanner(input);
    // Create store mock that is satisfactory for compactor.
    ColumnFamilyDescriptor familyDescriptor = ColumnFamilyDescriptorBuilder.of(NAME_OF_THINGS);

    ScanInfo si =
      new ScanInfo(conf, familyDescriptor, Long.MAX_VALUE, 0, CellComparatorImpl.COMPARATOR);
    HStore store = mock(HStore.class);
    when(store.getStorefiles()).thenReturn(storefiles);
    when(store.getColumnFamilyDescriptor()).thenReturn(familyDescriptor);
    when(store.getScanInfo()).thenReturn(si);
    when(store.areWritesEnabled()).thenReturn(true);
    when(store.getFileSystem()).thenReturn(mock(FileSystem.class));
    when(store.getRegionInfo()).thenReturn(RegionInfoBuilder.newBuilder(TABLE_NAME).build());
    StoreEngine storeEngine = mock(StoreEngine.class);
    when(storeEngine.createWriter(any(CreateStoreFileWriterParams.class))).thenAnswer(writers);
    when(store.getStoreEngine()).thenReturn(storeEngine);
    when(store.getComparator()).thenReturn(CellComparatorImpl.COMPARATOR);
    OptionalLong maxSequenceId = StoreUtils.getMaxSequenceIdInList(storefiles);
    when(store.getMaxSequenceId()).thenReturn(maxSequenceId);

    return new DefaultCompactor(conf, store) {
      @Override
      protected InternalScanner createScanner(HStore store, ScanInfo scanInfo,
        List<StoreFileScanner> scanners, long smallestReadPoint, long earliestPutTs,
        byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
        return scanner;
      }

      @Override
      protected InternalScanner createScanner(HStore store, ScanInfo scanInfo,
        List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint,
        long earliestPutTs) throws IOException {
        return scanner;
      }
    };
  }

  private void verify(KeyValue[] input, KeyValue[][] output) throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    HStoreFile sf1 = createDummyStoreFile(1L);
    HStoreFile sf2 = createDummyStoreFile(2L);
    DefaultCompactor dfc = createCompactor(writers, input, Arrays.asList(sf1, sf2));
    List<Path> paths = dfc.compact(new CompactionRequestImpl(Arrays.asList(sf1)),
      NoLimitThroughputController.INSTANCE, null);
    writers.verifyKvs(output);
    assertEquals(output.length, paths.size());
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] a(T... a) {
    return a;
  }

  @Test
  public void test() throws Exception {
    verify(
      a(KV_A_DeleteFamilyVersion, KV_A_1, KV_A_2, KV_A_3, KV_B_DeleteColumn, KV_B, KV_C, KV_D_1,
        KV_D_2, // Row 123
        KV_E_F_DeleteFamily, KV_E, KV_F, // Row 456
        KV_G_DeleteFamily, KV_G_DeleteFamilyVersion, KV_G_1, KV_G_DeleteColumn,
        KV_G_DeleteColumnVersion, KV_G_2), // Row 789
      a(a(KV_A_DeleteFamilyVersion, KV_A_2, KV_B_DeleteColumn, KV_C, KV_D_1, KV_E_F_DeleteFamily,
        KV_G_DeleteFamily, KV_G_1), // Latest versions
        a(KV_A_1, KV_A_3, KV_B, KV_D_2, KV_E, KV_F, KV_G_DeleteFamilyVersion, KV_G_DeleteColumn,
          KV_G_DeleteColumnVersion, KV_G_2)));
  }

  @Test
  public void testEmptyOutputFile() throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    CompactionRequestImpl request = createDummyRequest();
    DefaultCompactor dfc =
      createCompactor(writers, new KeyValue[0], new ArrayList<>(request.getFiles()));
    List<Path> paths = dfc.compact(request, NoLimitThroughputController.INSTANCE, null);
    assertEquals(1, paths.size());
    List<StoreFileWritersCapture.Writer> dummyWriters = writers.getWriters();
    assertEquals(1, dummyWriters.size());
    StoreFileWritersCapture.Writer dummyWriter = dummyWriters.get(0);
    assertTrue(dummyWriter.kvs.isEmpty());
    assertTrue(dummyWriter.hasMetadata);
  }
}
