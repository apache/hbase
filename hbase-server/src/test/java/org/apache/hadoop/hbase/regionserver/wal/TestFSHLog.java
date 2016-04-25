/**
 *
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
import java.lang.reflect.Field;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Provides FSHLog test cases.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLog extends AbstractTestFSWAL {

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    return new FSHLog(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix);
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    return new FSHLog(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix,
        suffix) {

      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
  }

  @Test
  public void testSyncRunnerIndexOverflow() throws IOException, NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    final String name = "testSyncRunnerIndexOverflow";
    FSHLog log = new FSHLog(FS, FSUtils.getRootDir(CONF), name, HConstants.HREGION_OLDLOGDIR_NAME,
        CONF, null, true, null, null);
    try {
      Field ringBufferEventHandlerField = FSHLog.class.getDeclaredField("ringBufferEventHandler");
      ringBufferEventHandlerField.setAccessible(true);
      FSHLog.RingBufferEventHandler ringBufferEventHandler =
          (FSHLog.RingBufferEventHandler) ringBufferEventHandlerField.get(log);
      Field syncRunnerIndexField =
          FSHLog.RingBufferEventHandler.class.getDeclaredField("syncRunnerIndex");
      syncRunnerIndexField.setAccessible(true);
      syncRunnerIndexField.set(ringBufferEventHandler, Integer.MAX_VALUE - 1);
      HTableDescriptor htd =
          new HTableDescriptor(TableName.valueOf("t1")).addFamily(new HColumnDescriptor("row"));
      NavigableMap<byte[], Integer> scopes = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
      for (byte[] fam : htd.getFamiliesKeys()) {
        scopes.put(fam, 0);
      }
      HRegionInfo hri =
          new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      MultiVersionConcurrencyControl mvcc = new MultiVersionConcurrencyControl();
      for (int i = 0; i < 10; i++) {
        addEdits(log, hri, htd, 1, mvcc, scopes);
      }
    } finally {
      log.close();
    }
  }
}
