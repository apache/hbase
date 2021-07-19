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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.SimpleRpcServer;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLogCorruptionWithMultiPutDueToDanglingByteBuffer
  extends WALCorruptionWithMultiPutDueToDanglingByteBufferTestBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule
    .forClass(TestFSHLogCorruptionWithMultiPutDueToDanglingByteBuffer.class);

  public static final class PauseWAL extends FSHLog {

    private int testTableWalAppendsCount = 0;

    public PauseWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    }

    @Override
    protected void atHeadOfRingBufferEventHandlerAppend() {
      // Let the 1st Append go through. The write thread will wait for this to go through before
      // calling further put()
      if (ARRIVE != null) { // Means appends as part of puts in testcase
        // Sleep for a second so that RS handler thread put all the mini batch WAL appends to ring
        // buffer.
        if (testTableWalAppendsCount == 0) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
          }
        }
        // Let the first minibatch write go through. When 2nd one comes, notify the waiting test
        // case for doing further batch puts and make this WAL append thread to pause
        if (testTableWalAppendsCount == 1) {
          ARRIVE.countDown();
          try {
            RESUME.await();
          } catch (InterruptedException e) {
          }
        }
        testTableWalAppendsCount++;
      }
    }
  }

  public static final class PauseWALProvider extends AbstractFSWALProvider<PauseWAL> {

    @Override
    protected PauseWAL createWAL() throws IOException {
      return new PauseWAL(CommonFSUtils.getWALFileSystem(conf), CommonFSUtils.getWALRootDir(conf),
        getWALDirectoryName(factory.factoryId), getWALArchiveDirectoryName(conf, factory.factoryId),
        conf, listeners, true, logPrefix,
        META_WAL_PROVIDER_ID.equals(providerId) ? META_WAL_PROVIDER_ID : null);
    }

    @Override
    protected void doInit(Configuration conf) throws IOException {
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(WALFactory.WAL_PROVIDER, PauseWALProvider.class,
      WALProvider.class);
    UTIL.getConfiguration().setInt(HRegion.HBASE_REGIONSERVER_MINIBATCH_SIZE, 1);
    UTIL.getConfiguration().set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY,
      SimpleRpcServer.class.getName());
    UTIL.getConfiguration().setInt(ByteBuffAllocator.MAX_BUFFER_COUNT_KEY, 1);
    UTIL.getConfiguration().setInt(ByteBuffAllocator.BUFFER_SIZE_KEY, 1024);
    UTIL.getConfiguration().setInt(ByteBuffAllocator.MIN_ALLOCATE_SIZE_KEY, 500);
    UTIL.startMiniCluster(1);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}

