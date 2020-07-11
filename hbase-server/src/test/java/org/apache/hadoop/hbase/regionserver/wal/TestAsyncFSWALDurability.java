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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ RegionServerServices.class, SmallTests.class })
public class TestAsyncFSWALDurability extends WALDurabilityTestBase<CustomAsyncFSWAL> {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestAsyncFSWALDurability.class);

  private static NioEventLoopGroup GROUP;

  @BeforeClass
  public static void setUpBeforeClass() {
    GROUP = new NioEventLoopGroup();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    GROUP.shutdownGracefully();
  }

  @Override
  protected CustomAsyncFSWAL getWAL(FileSystem fs, Path root, String logDir, Configuration conf)
    throws IOException {
    CustomAsyncFSWAL wal =
      new CustomAsyncFSWAL(fs, root, logDir, conf, GROUP, NioSocketChannel.class);
    wal.init();
    return wal;
  }

  @Override
  protected void resetSyncFlag(CustomAsyncFSWAL wal) {
    wal.resetSyncFlag();
  }

  @Override
  protected Boolean getSyncFlag(CustomAsyncFSWAL wal) {
    return wal.getSyncFlag();
  }

  @Override
  protected Boolean getWriterSyncFlag(CustomAsyncFSWAL wal) {
    return wal.getWriterSyncFlag();
  }
}

class CustomAsyncFSWAL extends AsyncFSWAL {

  private Boolean syncFlag;

  private Boolean writerSyncFlag;

  public CustomAsyncFSWAL(FileSystem fs, Path rootDir, String logDir, Configuration conf,
    EventLoopGroup eventLoopGroup, Class<? extends Channel> channelClass)
    throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, HConstants.HREGION_OLDLOGDIR_NAME, conf, null, true, null, null,
      eventLoopGroup, channelClass);
  }

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
        writerSyncFlag = forceSync;
        return writer.sync(forceSync);
      }

      @Override
      public void append(Entry entry) {
        writer.append(entry);
      }
    };
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    syncFlag = forceSync;
    super.sync(forceSync);
  }

  @Override
  public void sync(long txid, boolean forceSync) throws IOException {
    syncFlag = forceSync;
    super.sync(txid, forceSync);
  }

  void resetSyncFlag() {
    this.syncFlag = null;
    this.writerSyncFlag = null;
  }

  Boolean getSyncFlag() {
    return syncFlag;
  }

  Boolean getWriterSyncFlag() {
    return writerSyncFlag;
  }
}
