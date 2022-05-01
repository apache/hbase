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

import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.STRIPE_END_KEY;
import static org.apache.hadoop.hbase.regionserver.StripeStoreFileManager.STRIPE_START_KEY;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.ScannerContext;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.regionserver.StripeMultiFileWriter;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestCompactor {

  public static HStoreFile createDummyStoreFile(long maxSequenceId) throws Exception {
    // "Files" are totally unused, it's Scanner class below that gives compactor fake KVs.
    // But compaction depends on everything under the sun, so stub everything with dummies.
    HStoreFile sf = mock(HStoreFile.class);
    StoreFileReader r = mock(StoreFileReader.class);
    when(r.length()).thenReturn(1L);
    when(r.getBloomFilterType()).thenReturn(BloomType.NONE);
    when(r.getHFileReader()).thenReturn(mock(HFile.Reader.class));
    when(r.getStoreFileScanner(anyBoolean(), anyBoolean(), anyBoolean(), anyLong(), anyLong(),
      anyBoolean())).thenReturn(mock(StoreFileScanner.class));
    when(sf.getReader()).thenReturn(r);
    when(sf.getMaxSequenceId()).thenReturn(maxSequenceId);
    return sf;
  }

  public static CompactionRequestImpl createDummyRequest() throws Exception {
    return new CompactionRequestImpl(Arrays.asList(createDummyStoreFile(1L)));
  }

  // StoreFile.Writer has private ctor and is unwieldy, so this has to be convoluted.
  public static class StoreFileWritersCapture
    implements Answer<StoreFileWriter>, StripeMultiFileWriter.WriterFactory {
    public static class Writer {
      public ArrayList<KeyValue> kvs = new ArrayList<>();
      public TreeMap<byte[], byte[]> data = new TreeMap<>(Bytes.BYTES_COMPARATOR);
      public boolean hasMetadata;
    }

    private List<Writer> writers = new ArrayList<>();

    @Override
    public StoreFileWriter createWriter() throws IOException {
      final Writer realWriter = new Writer();
      writers.add(realWriter);
      StoreFileWriter writer = mock(StoreFileWriter.class);
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) {
          return realWriter.kvs.add((KeyValue) invocation.getArgument(0));
        }
      }).when(writer).append(any());
      doAnswer(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          return realWriter.data.put((byte[]) args[0], (byte[]) args[1]);
        }
      }).when(writer).appendFileInfo(any(), any());
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          realWriter.hasMetadata = true;
          return null;
        }
      }).when(writer).appendMetadata(anyLong(), anyBoolean());
      doAnswer(new Answer<Void>() {
        @Override
        public Void answer(InvocationOnMock invocation) throws Throwable {
          realWriter.hasMetadata = true;
          return null;
        }
      }).when(writer).appendMetadata(anyLong(), anyBoolean(), anyCollection());
      doAnswer(new Answer<Path>() {
        @Override
        public Path answer(InvocationOnMock invocation) throws Throwable {
          return new Path("foo");
        }
      }).when(writer).getPath();
      return writer;
    }

    @Override
    public StoreFileWriter answer(InvocationOnMock invocation) throws Throwable {
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

    public void verifyKvs(KeyValue[][] kvss, boolean allFiles, List<Long> boundaries) {
      if (allFiles) {
        assertEquals(kvss.length, writers.size());
      }
      int skippedWriters = 0;
      for (int i = 0; i < kvss.length; ++i) {
        KeyValue[] kvs = kvss[i];
        if (kvs != null) {
          Writer w = writers.get(i - skippedWriters);
          assertEquals(kvs.length, w.kvs.size());
          for (int j = 0; j < kvs.length; ++j) {
            assertTrue(kvs[j].getTimestamp() >= boundaries.get(i));
            assertTrue(kvs[j].getTimestamp() < boundaries.get(i + 1));
            assertEquals(kvs[j], w.kvs.get(j));
          }
        } else {
          assertFalse(allFiles);
          ++skippedWriters;
        }
      }
    }

    public List<Writer> getWriters() {
      return writers;
    }
  }

  public static class Scanner implements InternalScanner {
    private final ArrayList<KeyValue> kvs;

    public Scanner(KeyValue... kvs) {
      this.kvs = new ArrayList<>(Arrays.asList(kvs));
    }

    @Override
    public boolean next(List<Cell> result, ScannerContext scannerContext) throws IOException {
      if (kvs.isEmpty()) return false;
      result.add(kvs.remove(0));
      return !kvs.isEmpty();
    }

    @Override
    public void close() throws IOException {
    }
  }
}
