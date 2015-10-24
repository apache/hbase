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

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.OPEN_KEY;
import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.STRIPE_END_KEY;
import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.STRIPE_START_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.StripeCompactor;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


@Category(SmallTests.class)
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

  private static KeyValue kvAfter(byte[] key) {
    return new KeyValue(Arrays.copyOf(key, key.length + 1), 0L);
  }

  private static <T> T[] a(T... a) {
    return a;
  }

  private static KeyValue[] e() {
    return TestStripeCompactor.<KeyValue>a();
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
    verifyBoundaryCompaction(
        a(KV_B), a(KEY_B, KEY_C, KEY_D, OPEN_KEY), a(a(KV_B), null, null), null, null, false);
    verifyBoundaryCompaction(a(KV_A, KV_C),
        a(OPEN_KEY, KEY_B, KEY_C, KEY_D), a(a(KV_A), null, a(KV_C)), null, null, false);
    // But should be created if there are no file.
    verifyBoundaryCompaction(
        e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(null, null, e()), null, null, false);
    // In major range if there's major range.
    verifyBoundaryCompaction(
        e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(null, e(), null), KEY_B, KEY_C, false);
    verifyBoundaryCompaction(
        e(), a(OPEN_KEY, KEY_B, KEY_C, OPEN_KEY), a(e(), e(), null), OPEN_KEY, KEY_C, false);
    // Major range should have files regardless of KVs.
    verifyBoundaryCompaction(a(KV_A), a(OPEN_KEY, KEY_B, KEY_C, KEY_D, OPEN_KEY),
        a(a(KV_A), e(), e(), null), KEY_B, KEY_D, false);
    verifyBoundaryCompaction(a(KV_C), a(OPEN_KEY, KEY_B, KEY_C, KEY_D, OPEN_KEY),
        a(null, null, a(KV_C), e()), KEY_C, OPEN_KEY, false);

  }

  public static void verifyBoundaryCompaction(
      KeyValue[] input, byte[][] boundaries, KeyValue[][] output) throws Exception {
    verifyBoundaryCompaction(input, boundaries, output, null, null, true);
  }

  public static void verifyBoundaryCompaction(KeyValue[] input, byte[][] boundaries,
      KeyValue[][] output, byte[] majorFrom, byte[] majorTo, boolean allFiles)
          throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StripeCompactor sc = createCompactor(writers, input);
    List<Path> paths =
        sc.compact(createDummyRequest(), Arrays.asList(boundaries), majorFrom, majorTo,
          NoLimitCompactionThroughputController.INSTANCE);
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

  public static void verifySizeCompaction(KeyValue[] input, int targetCount, long targetSize,
      byte[] left, byte[] right, KeyValue[][] output) throws Exception {
    StoreFileWritersCapture writers = new StoreFileWritersCapture();
    StripeCompactor sc = createCompactor(writers, input);
    List<Path> paths =
        sc.compact(createDummyRequest(), targetCount, targetSize, left, right, null, null,
          NoLimitCompactionThroughputController.INSTANCE);
    assertEquals(output.length, paths.size());
    writers.verifyKvs(output, true, true);
    List<byte[]> boundaries = new ArrayList<byte[]>();
    boundaries.add(left);
    for (int i = 1; i < output.length; ++i) {
      boundaries.add(output[i][0].getRow());
    }
    boundaries.add(right);
    writers.verifyBoundaries(boundaries.toArray(new byte[][] {}));
  }

  private static StripeCompactor createCompactor(
      StoreFileWritersCapture writers, KeyValue[] input) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    final Scanner scanner = new Scanner(input);

    // Create store mock that is satisfactory for compactor.
    HColumnDescriptor col = new HColumnDescriptor(NAME_OF_THINGS);
    ScanInfo si = new ScanInfo(conf, col, Long.MAX_VALUE, 0, new KVComparator());
    Store store = mock(Store.class);
    when(store.getFamily()).thenReturn(col);
    when(store.getScanInfo()).thenReturn(si);
    when(store.areWritesEnabled()).thenReturn(true);
    when(store.getFileSystem()).thenReturn(mock(FileSystem.class));
    when(store.getRegionInfo()).thenReturn(new HRegionInfo(TABLE_NAME));
    when(store.createWriterInTmp(anyLong(), any(Compression.Algorithm.class),
        anyBoolean(), anyBoolean(), anyBoolean(), anyBoolean())).thenAnswer(writers);
    when(store.getComparator()).thenReturn(new KVComparator());

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

  private static CompactionRequest createDummyRequest() throws Exception {
    // "Files" are totally unused, it's Scanner class below that gives compactor fake KVs.
    // But compaction depends on everything under the sun, so stub everything with dummies.
    StoreFile sf = mock(StoreFile.class);
    StoreFile.Reader r = mock(StoreFile.Reader.class);
    when(r.length()).thenReturn(1L);
    when(r.getBloomFilterType()).thenReturn(BloomType.NONE);
    when(r.getHFileReader()).thenReturn(mock(HFile.Reader.class));
    when(r.getStoreFileScanner(anyBoolean(), anyBoolean(), anyBoolean(), anyLong()))
      .thenReturn(mock(StoreFileScanner.class));
    when(sf.getReader()).thenReturn(r);
    when(sf.createReader()).thenReturn(r);
    when(sf.createReader(anyBoolean())).thenReturn(r);
    return new CompactionRequest(Arrays.asList(sf));
  }

  private static class Scanner implements InternalScanner {
    private final ArrayList<KeyValue> kvs;
    public Scanner(KeyValue... kvs) {
      this.kvs = new ArrayList<KeyValue>(Arrays.asList(kvs));
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
      if (kvs.isEmpty()) return false;
      results.add(kvs.remove(0));
      return !kvs.isEmpty();
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext)
        throws IOException {
      return next(result);
    }

    @Override
    public void close() throws IOException {}
  }

  // StoreFile.Writer has private ctor and is unwieldy, so this has to be convoluted.
  public static class StoreFileWritersCapture implements
    Answer<StoreFile.Writer>, StripeMultiFileWriter.WriterFactory {
    public static class Writer {
      public ArrayList<KeyValue> kvs = new ArrayList<KeyValue>();
      public TreeMap<byte[], byte[]> data = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    }

    private List<Writer> writers = new ArrayList<Writer>();

    @Override
    public StoreFile.Writer createWriter() throws IOException {
      final Writer realWriter = new Writer();
      writers.add(realWriter);
      StoreFile.Writer writer = mock(StoreFile.Writer.class);
      doAnswer(new Answer<Object>() {
        public Object answer(InvocationOnMock invocation) {
          return realWriter.kvs.add((KeyValue)invocation.getArguments()[0]);
        }}).when(writer).append(any(KeyValue.class));
      doAnswer(new Answer<Object>() {
        public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          return realWriter.data.put((byte[])args[0], (byte[])args[1]);
        }}).when(writer).appendFileInfo(any(byte[].class), any(byte[].class));
      return writer;
    }

    @Override
    public StoreFile.Writer answer(InvocationOnMock invocation) throws Throwable {
      return createWriter();
    }

    public void verifyKvs(KeyValue[][] kvss, boolean allFiles, boolean requireMetadata) {
      if (allFiles) {
        assertEquals(kvss.length, writers.size());
      }
      int skippedWriters = 0;
      for (int i = 0; i < kvss.length; ++i) {
        KeyValue[] kvs = kvss[i];
        if (kvs != null) {
          Writer w = writers.get(i - skippedWriters);
          if (requireMetadata) {
            assertNotNull(w.data.get(STRIPE_START_KEY));
            assertNotNull(w.data.get(STRIPE_END_KEY));
          } else {
            assertNull(w.data.get(STRIPE_START_KEY));
            assertNull(w.data.get(STRIPE_END_KEY));
          }
          assertEquals(kvs.length, w.kvs.size());
          for (int j = 0; j < kvs.length; ++j) {
            assertEquals(kvs[j], w.kvs.get(j));
          }
        } else {
          assertFalse(allFiles);
          ++skippedWriters;
        }
      }
    }

    public void verifyBoundaries(byte[][] boundaries) {
      assertEquals(boundaries.length - 1, writers.size());
      for (int i = 0; i < writers.size(); ++i) {
        assertArrayEquals("i = " + i, boundaries[i], writers.get(i).data.get(STRIPE_START_KEY));
        assertArrayEquals("i = " + i, boundaries[i + 1], writers.get(i).data.get(STRIPE_END_KEY));
      }
    }
  }
}
