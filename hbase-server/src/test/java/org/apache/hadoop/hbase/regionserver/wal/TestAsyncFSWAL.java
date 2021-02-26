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
package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.LogRoller;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.regionserver.SequenceId;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Provides AsyncFSWAL test cases.
 */
@Category({ RegionServerTests.class, LargeTests.class })
public class TestAsyncFSWAL extends AbstractTestFSWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncFSWAL.class);

  private static EventLoopGroup GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    GROUP =
      new NioEventLoopGroup(1, new ThreadFactoryBuilder().setNameFormat("TestAsyncFSWAL-pool-%d")
        .setDaemon(true).setUncaughtExceptionHandler(Threads.LOGGING_EXCEPTION_HANDLER).build());
    CHANNEL_CLASS = NioSocketChannel.class;
    AbstractTestFSWAL.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestFSWAL.tearDownAfterClass();
    GROUP.shutdownGracefully();
  }

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    AsyncFSWAL asyncFSWAL = new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf,
      listeners, failIfWALExists, prefix, suffix, GROUP, CHANNEL_CLASS);
    asyncFSWAL.init();
    return asyncFSWAL;
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    AsyncFSWAL asyncFSWAL = new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf, listeners,
      failIfWALExists, prefix, suffix, GROUP, CHANNEL_CLASS) {
      @Override
      protected void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }

    };
    asyncFSWAL.init();
    return asyncFSWAL;
  }

  @Test
  public void testBrokenWriter() throws Exception {
    RegionServerServices services = mock(RegionServerServices.class);
    when(services.getConfiguration()).thenReturn(CONF);
    TableDescriptor td = TableDescriptorBuilder.newBuilder(TableName.valueOf("table"))
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of("row")).build();
    RegionInfo ri = RegionInfoBuilder.newBuilder(td.getTableName()).build();
    MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : td.getColumnFamilyNames()) {
      scopes.put(fam, 0);
    }
    long timestamp = System.currentTimeMillis();
    String testName = currentTest.getMethodName();
    AtomicInteger failedCount = new AtomicInteger(0);
    try (LogRoller roller = new LogRoller(services);
        AsyncFSWAL wal = new AsyncFSWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
            testName, CONF, null, true, null, null, GROUP, CHANNEL_CLASS) {

          @Override
          protected AsyncWriter createWriterInstance(Path path) throws IOException {
            AsyncWriter writer = super.createWriterInstance(path);
            return new AsyncWriter() {

              @Override
              public void close() throws IOException {
                writer.close();
              }

              @Override
              public long getLength() {
                return writer.getLength();
              }

              @Override
              public long getSyncedLength() {
                return writer.getSyncedLength();
              }

              @Override
              public CompletableFuture<Long> sync(boolean forceSync) {
                CompletableFuture<Long> result = writer.sync(forceSync);
                if (failedCount.incrementAndGet() < 1000) {
                  CompletableFuture<Long> future = new CompletableFuture<>();
                  FutureUtils.addListener(result,
                    (r, e) -> future.completeExceptionally(new IOException("Inject Error")));
                  return future;
                } else {
                  return result;
                }
              }

              @Override
              public void append(Entry entry) {
                writer.append(entry);
              }
            };
          }
        }) {
      wal.init();
      roller.addWAL(wal);
      roller.start();
      int numThreads = 10;
      AtomicReference<Exception> error = new AtomicReference<>();
      Thread[] threads = new Thread[numThreads];
      for (int i = 0; i < 10; i++) {
        final int index = i;
        threads[index] = new Thread("Write-Thread-" + index) {

          @Override
          public void run() {
            byte[] row = Bytes.toBytes("row" + index);
            WALEdit cols = new WALEdit();
            cols.add(new KeyValue(row, row, row, timestamp + index, row));
            WALKeyImpl key = new WALKeyImpl(ri.getEncodedNameAsBytes(), td.getTableName(),
                SequenceId.NO_SEQUENCE_ID, timestamp, WALKey.EMPTY_UUIDS, HConstants.NO_NONCE,
                HConstants.NO_NONCE, mvcc, scopes);
            try {
              wal.append(ri, key, cols, true);
            } catch (IOException e) {
              // should not happen
              throw new UncheckedIOException(e);
            }
            try {
              wal.sync();
            } catch (IOException e) {
              error.set(e);
            }
          }
        };
      }
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join();
      }
      assertNull(error.get());
    }
  }
}
