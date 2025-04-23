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
package org.apache.hadoop.hbase.master.region;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.AbstractFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.AsyncFSWAL;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALSyncTimeoutIOException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;

@Category({ MasterTests.class, MediumTests.class })
public class TestMasterRegionWALSyncTimeoutIOException extends MasterRegionTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMasterRegionWALSyncTimeoutIOException.class);

  private static final Duration WAL_SYNC_TIMEOUT = Duration.ofSeconds(3);

  private static volatile boolean testWalTimeout = false;

  @Override
  protected void configure(Configuration conf) throws IOException {
    conf.setClass(WALFactory.WAL_PROVIDER, SlowAsyncFSWALProvider.class, WALProvider.class);
    conf.setLong(AbstractFSWAL.WAL_SYNC_TIMEOUT_MS, WAL_SYNC_TIMEOUT.toMillis());
  }

  @Override
  protected void configure(MasterRegionParams params) {
    params.flushIntervalMs(Duration.ofSeconds(1).toMillis());
  }

  @Test
  public void testUpdateWalSyncWriteException() {
    testWalTimeout = true;
    assertThrows(WALSyncTimeoutIOException.class, () -> {
      for (int i = 0; i < 10; i++) {
        region.update(
          r -> r.put(new Put(Bytes.toBytes("0")).addColumn(CF1, QUALIFIER, Bytes.toBytes("0"))));
        Thread.sleep(Duration.ofSeconds(1).toMillis());
      }
    });
  }

  public static class SlowAsyncFSWAL extends AsyncFSWAL {

    public SlowAsyncFSWAL(FileSystem fs, Abortable abortable, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, EventLoopGroup eventLoopGroup,
      Class<? extends Channel> channelClass, StreamSlowMonitor monitor) throws IOException {
      super(fs, abortable, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix, null, null, eventLoopGroup, channelClass, monitor);
    }

    @Override
    protected void atHeadOfRingBufferEventHandlerAppend() {
      if (testWalTimeout) {
        try {
          Thread.sleep(WAL_SYNC_TIMEOUT.plusSeconds(1).toMillis());
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      super.atHeadOfRingBufferEventHandlerAppend();
    }
  }

  public static class SlowAsyncFSWALProvider extends AsyncFSWALProvider {

    @Override
    protected AsyncFSWAL createWAL() throws IOException {
      return new SlowAsyncFSWAL(CommonFSUtils.getWALFileSystem(conf), this.abortable,
        CommonFSUtils.getWALRootDir(conf), getWALDirectoryName(factory.getFactoryId()),
        getWALArchiveDirectoryName(conf, factory.getFactoryId()), conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null, eventLoopGroup,
        channelClass, factory.getExcludeDatanodeManager().getStreamSlowMonitor(providerId));
    }
  }
}
