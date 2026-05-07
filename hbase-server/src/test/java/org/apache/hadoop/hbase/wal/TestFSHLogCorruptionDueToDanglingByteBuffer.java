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
package org.apache.hadoop.hbase.wal;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.FSHLog;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

/**
 * Testcase for HBASE-22539
 */
@Tag(RegionServerTests.TAG)
@Tag(MediumTests.TAG)
public class TestFSHLogCorruptionDueToDanglingByteBuffer
  extends WALCorruptionDueToDanglingByteBufferTestBase {

  public static final class PauseWAL extends FSHLog {

    public PauseWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
      super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    }

    @Override
    protected void atHeadOfRingBufferEventHandlerAppend() {
      if (ARRIVE != null) {
        ARRIVE.countDown();
        try {
          RESUME.await();
        } catch (InterruptedException e) {
        }
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

  @BeforeAll
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setClass(WALFactory.WAL_PROVIDER, PauseWALProvider.class,
      WALProvider.class);
    UTIL.startMiniCluster(1);
    UTIL.createTable(TABLE_NAME, CF);
    UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }
}
