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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.createDummyRequest;
import static org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.createDummyStoreFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.Scanner;
import org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.StoreFileWritersCapture;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, SmallTests.class })
public class TestDateTieredCompactor {

  private static final byte[] NAME_OF_THINGS = Bytes.toBytes("foo");

  private static final TableName TABLE_NAME = TableName.valueOf(NAME_OF_THINGS, NAME_OF_THINGS);

  private static final KeyValue KV_A = new KeyValue(Bytes.toBytes("aaa"), 100L);

  private static final KeyValue KV_B = new KeyValue(Bytes.toBytes("bbb"), 200L);

  private static final KeyValue KV_C = new KeyValue(Bytes.toBytes("ccc"), 300L);

  private static final KeyValue KV_D = new KeyValue(Bytes.toBytes("ddd"), 400L);

  @Parameters(name = "{index}: usePrivateReaders={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @Parameter
  public boolean usePrivateReaders;

  private DateTieredCompactor createCompactor(StoreFileWritersCapture writers,
      final KeyValue[] input, List<StoreFile> storefiles) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.regionserver.compaction.private.readers", usePrivateReaders);
    final Scanner scanner = new Scanner(input);
    // Create store mock that is satisfactory for compactor.
    HColumnDescriptor col = new HColumnDescriptor(NAME_OF_THINGS);
    ScanInfo si = new ScanInfo(conf, col, Long.MAX_VALUE, 0, new KVComparator());
    final Store store = mock(Store.class);
    when(store.getStorefiles()).thenReturn(storefiles);
    when(store.getFamily()).thenReturn(col);
    when(store.getScanInfo()).thenReturn(si);
    when(store.areWritesEnabled()).thenReturn(true);
    when(store.getFileSystem()).thenReturn(mock(FileSystem.class));
    when(store.getRegionInfo()).thenReturn(new HRegionInfo(TABLE_NAME));
    when(store.createWriterInTmp(anyLong(), any(Compression.Algorithm.class), anyBoolean(),
      anyBoolean(), anyBoolean(), anyBoolean())).thenAnswer(writers);
    when(store.getComparator()).thenReturn(new KVComparator());
    long maxSequenceId = StoreFile.getMaxSequenceIdInList(storefiles);
    when(store.getMaxSequenceId()).thenReturn(maxSequenceId);

    return new DateTieredCompactor(conf, store) {
      @Override
      protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
          long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow,
          byte[] dropDeletesToRow) throws IOException {
        return scanner;
      }

      @Override
      protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners,
          ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        return scanner;
      }
    };
  }

  private void verify(KeyValue[] input, List<Long> boundaries, KeyValue[][] output,
      boolean allFiles) throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StoreFile sf1 = createDummyStoreFile(1L);
    StoreFile sf2 = createDummyStoreFile(2L);
    DateTieredCompactor dtc = createCompactor(writers, input, Arrays.asList(sf1, sf2));
    List<Path> paths = dtc.compact(new CompactionRequest(Arrays.asList(sf1)),
      boundaries.subList(0, boundaries.size() - 1), NoLimitThroughputController.INSTANCE, null);
    writers.verifyKvs(output, allFiles, boundaries);
    if (allFiles) {
      assertEquals(output.length, paths.size());
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] a(T... a) {
    return a;
  }

  @Test
  public void test() throws Exception {
    verify(a(KV_A, KV_B, KV_C, KV_D), Arrays.asList(100L, 200L, 300L, 400L, 500L),
      a(a(KV_A), a(KV_B), a(KV_C), a(KV_D)), true);
    verify(a(KV_A, KV_B, KV_C, KV_D), Arrays.asList(Long.MIN_VALUE, 200L, Long.MAX_VALUE),
      a(a(KV_A), a(KV_B, KV_C, KV_D)), false);
    verify(a(KV_A, KV_B, KV_C, KV_D), Arrays.asList(Long.MIN_VALUE, Long.MAX_VALUE),
      new KeyValue[][] { a(KV_A, KV_B, KV_C, KV_D) }, false);
  }

  @Test
  public void testEmptyOutputFile() throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    CompactionRequest request = createDummyRequest();
    DateTieredCompactor dtc = createCompactor(writers, new KeyValue[0],
      new ArrayList<StoreFile>(request.getFiles()));
    List<Path> paths = dtc.compact(request, Arrays.asList(Long.MIN_VALUE, Long.MAX_VALUE),
      NoLimitThroughputController.INSTANCE, null);
    assertEquals(1, paths.size());
    List<StoreFileWritersCapture.Writer> dummyWriters = writers.getWriters();
    assertEquals(1, dummyWriters.size());
    StoreFileWritersCapture.Writer dummyWriter = dummyWriters.get(0);
    assertTrue(dummyWriter.kvs.isEmpty());
    assertTrue(dummyWriter.hasMetadata);
  }
}
