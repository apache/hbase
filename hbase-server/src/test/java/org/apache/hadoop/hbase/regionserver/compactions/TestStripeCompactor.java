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

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;
import static org.apache.hadoop.hbase.regionserver.compactions.TestCompactor.createDummyRequest;
import static org.junit.Assert.assertEquals;
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
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanType;
import org.apache.hadoop.hbase.regionserver.Store;
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
public class TestStripeCompactor {
  private static final byte[] NAME_OF_THINGS = Bytes.toBytes("foo");
  private static final TableName TABLE_NAME = TableName.valueOf(NAME_OF_THINGS, NAME_OF_THINGS);

  private static final byte[] KEY_B = Bytes.toBytes("bbb");
  private static final byte[] KEY_C = Bytes.toBytes("ccc");
  private static final byte[] KEY_D = Bytes.toBytes("ddd");

  private static final KeyValue KV_A = kvAfter(Bytes.toBytes("aaa"));
  private static final KeyValue KV_B = kvAfter(KEY_B);
  private static final KeyValue KV_C = kvAfter(KEY_C);
  private static final KeyValue KV_D = kvAfter(KEY_D);

  @Parameters(name = "{index}: usePrivateReaders={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { true }, new Object[] { false });
  }

  @Parameter
  public boolean usePrivateReaders;

  private static KeyValue kvAfter(byte[] key) {
    return new KeyValue(Arrays.copyOf(key, key.length + 1), 0L);
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] a(T... a) {
    return a;
  }

  private static KeyValue[] e() {
    return TestStripeCompactor.<KeyValue> a();
  }

  @Test
  public void testBoundaryCompactions() throws Exception {
    // General verification
    verifyBoundaryCompaction(a(KV_A, KV_A, KV_B, KV_B, KV_C, KV_D),
      a(OPEN_KEY, KEY_B, KEY_D, OPEN_KEY), a(a(KV_A, KV_A), a(KV_B, KV_B, KV_C), a(KV_D)));
    verifyBoundaryCompaction(a(KV_B, KV_C), a(KEY_B, KEY_C, KEY_D), a(a(KV_B), a(KV_C)));
    verifyBoundaryCompaction(a(KV_B, KV_C), a(KEY_B, KEY_D), new KeyValue[][] { a(KV_B, KV_C) });
  }

  @Test
  public void testBoundaryCompactionEmptyFiles() throws Exception {
    // No empty file if there're already files.
    verifyBoundaryCompaction(a(KV_B), a(KEY_B, KEY_C, KEY_D, OPEN_KEY), a(a(KV_B), null, null),
      null, null, false);
    verifyBoundaryCompaction(a(KV_A, KV_C), a(OPEN_KEY, KEY_B, KEY_C, KEY_D),
      a(a(KV_A), null, a(KV_C)), null, null, false);
    // But should be created if there are no file.
    verifyBoundaryCompaction(e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(null, null, e()), null,
      null, false);
    // In major range if there's major range.
    verifyBoundaryCompaction(e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(null, e(), null), KEY_B,
      KEY_C, false);
    verifyBoundaryCompaction(e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(e(), e(), null), OPEN_KEY,
      KEY_C, false);
    // Major range should have files regardless of KVs.
    verifyBoundaryCompaction(a(KV_A), a(OPEN_KEY, KEY_B, KEY_C, KEY_D, OPEN_KEY),
      a(a(KV_A), e(), e(), null), KEY_B, KEY_D, false);
    verifyBoundaryCompaction(a(KV_C), a(OPEN_KEY, KEY_B, KEY_C, KEY_D, OPEN_KEY),
      a(null, null, a(KV_C), e()), KEY_C, OPEN_KEY, false);

  }

  private void verifyBoundaryCompaction(KeyValue[] input, byte[][] boundaries, KeyValue[][] output)
      throws Exception {
    verifyBoundaryCompaction(input, boundaries, output, null, null, true);
  }

  private void verifyBoundaryCompaction(KeyValue[] input, byte[][] boundaries, KeyValue[][] output,
      byte[] majorFrom, byte[] majorTo, boolean allFiles) throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StripeCompactor sc = createCompactor(writers, input);
    List<Path> paths = sc.compact(createDummyRequest(), Arrays.asList(boundaries), majorFrom,
      majorTo, NoLimitThroughputController.INSTANCE, null);
    writers.verifyKvs(output, allFiles, true);
    if (allFiles) {
      assertEquals(output.length, paths.size());
      writers.verifyBoundaries(boundaries);
    }
  }

  @Test
  public void testSizeCompactions() throws Exception {
    // General verification with different sizes.
    verifySizeCompaction(a(KV_A, KV_A, KV_B, KV_C, KV_D), 3, 2, OPEN_KEY, OPEN_KEY,
      a(a(KV_A, KV_A), a(KV_B, KV_C), a(KV_D)));
    verifySizeCompaction(a(KV_A, KV_B, KV_C, KV_D), 4, 1, OPEN_KEY, OPEN_KEY,
      a(a(KV_A), a(KV_B), a(KV_C), a(KV_D)));
    verifySizeCompaction(a(KV_B, KV_C), 2, 1, KEY_B, KEY_D, a(a(KV_B), a(KV_C)));
    // Verify row boundaries are preserved.
    verifySizeCompaction(a(KV_A, KV_A, KV_A, KV_C, KV_D), 3, 2, OPEN_KEY, OPEN_KEY,
      a(a(KV_A, KV_A, KV_A), a(KV_C, KV_D)));
    verifySizeCompaction(a(KV_A, KV_B, KV_B, KV_C), 3, 1, OPEN_KEY, OPEN_KEY,
      a(a(KV_A), a(KV_B, KV_B), a(KV_C)));
    // Too much data, count limits the number of files.
    verifySizeCompaction(a(KV_A, KV_B, KV_C, KV_D), 2, 1, OPEN_KEY, OPEN_KEY,
      a(a(KV_A), a(KV_B, KV_C, KV_D)));
    verifySizeCompaction(a(KV_A, KV_B, KV_C), 1, Long.MAX_VALUE, OPEN_KEY, KEY_D,
      new KeyValue[][] { a(KV_A, KV_B, KV_C) });
    // Too little data/large count, no extra files.
    verifySizeCompaction(a(KV_A, KV_B, KV_C, KV_D), Integer.MAX_VALUE, 2, OPEN_KEY, OPEN_KEY,
      a(a(KV_A, KV_B), a(KV_C, KV_D)));
  }

  private void verifySizeCompaction(KeyValue[] input, int targetCount, long targetSize, byte[] left,
      byte[] right, KeyValue[][] output) throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StripeCompactor sc = createCompactor(writers, input);
    List<Path> paths = sc.compact(createDummyRequest(), targetCount, targetSize, left, right, null,
      null, NoLimitThroughputController.INSTANCE, null);
    assertEquals(output.length, paths.size());
    writers.verifyKvs(output, true, true);
    List<byte[]> boundaries = new ArrayList<byte[]>();
    boundaries.add(left);
    for (int i = 1; i < output.length; ++i) {
      boundaries.add(CellUtil.cloneRow(output[i][0]));
    }
    boundaries.add(right);
    writers.verifyBoundaries(boundaries.toArray(new byte[][] {}));
  }

  private StripeCompactor createCompactor(StoreFileWritersCapture writers, KeyValue[] input)
      throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.regionserver.compaction.private.readers", usePrivateReaders);
    final Scanner scanner = new Scanner(input);

    // Create store mock that is satisfactory for compactor.
    HColumnDescriptor col = new HColumnDescriptor(NAME_OF_THINGS);
    ScanInfo si = new ScanInfo(conf, col, Long.MAX_VALUE, 0, CellComparator.COMPARATOR);
    Store store = mock(Store.class);
    when(store.getFamily()).thenReturn(col);
    when(store.getScanInfo()).thenReturn(si);
    when(store.areWritesEnabled()).thenReturn(true);
    when(store.getFileSystem()).thenReturn(mock(FileSystem.class));
    when(store.getRegionInfo()).thenReturn(new HRegionInfo(TABLE_NAME));
    when(store.createWriterInTmp(anyLong(), any(Compression.Algorithm.class), anyBoolean(),
      anyBoolean(), anyBoolean(), anyBoolean())).thenAnswer(writers);
    when(store.getComparator()).thenReturn(CellComparator.COMPARATOR);

    return new StripeCompactor(conf, store) {
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
}
