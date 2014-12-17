/*
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
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationSource {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSource.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static FileSystem FS;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = HBaseConfiguration.create();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    Path rootDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (FS.exists(oldLogDir)) FS.delete(oldLogDir, true);
    logDir = new Path(rootDir, HConstants.HREGION_LOGDIR_NAME);
    if (FS.exists(logDir)) FS.delete(logDir, true);
  }

  /**
   * Sanity check that we can move logs around while we are reading
   * from them. Should this test fail, ReplicationSource would have a hard
   * time reading logs that are being archived.
   * @throws Exception
   */
  @Test
  public void testLogMoving() throws Exception{
    Path logPath = new Path(logDir, "log");
    if (!FS.exists(logDir)) FS.mkdirs(logDir);
    if (!FS.exists(oldLogDir)) FS.mkdirs(oldLogDir);
    WALProvider.Writer writer = WALFactory.createWALWriter(FS, logPath,
        TEST_UTIL.getConfiguration());
    for(int i = 0; i < 3; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      WALKey key = new WALKey(b, TableName.valueOf(b), 0, 0,
          HConstants.DEFAULT_CLUSTER_ID);
      writer.append(new WAL.Entry(key, edit));
      writer.sync();
    }
    writer.close();

    WAL.Reader reader = WALFactory.createReader(FS, logPath, TEST_UTIL.getConfiguration());
    WAL.Entry entry = reader.next();
    assertNotNull(entry);

    Path oldLogPath = new Path(oldLogDir, "log");
    FS.rename(logPath, oldLogPath);

    entry = reader.next();
    assertNotNull(entry);

    entry = reader.next();
    entry = reader.next();

    assertNull(entry);
    reader.close();
  }

}

