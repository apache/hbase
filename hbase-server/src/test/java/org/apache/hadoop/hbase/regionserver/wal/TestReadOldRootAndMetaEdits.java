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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALFactory;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests to read old ROOT, Meta edits.
 */
@Category(MediumTests.class)

public class TestReadOldRootAndMetaEdits {

  private final static Log LOG = LogFactory.getLog(TestReadOldRootAndMetaEdits.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf;
  private static FileSystem fs;
  private static Path dir;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setClass("hbase.regionserver.hlog.writer.impl",
      SequenceFileLogWriter.class, WALProvider.Writer.class);
    fs = TEST_UTIL.getTestFileSystem();
    dir = new Path(TEST_UTIL.createRootDir(), "testReadOldRootAndMetaEdits");
    fs.mkdirs(dir);

  }
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  /**
   * Inserts three waledits in the wal file, and reads them back. The first edit is of a regular
   * table, second waledit is for the ROOT table (it will be ignored while reading),
   * and last waledit is for the hbase:meta table, which will be linked to the new system:meta table.
   * @throws IOException
   */
  @Test
  public void testReadOldRootAndMetaEdits() throws IOException {
    LOG.debug("testReadOldRootAndMetaEdits");
    // kv list to be used for all WALEdits.
    byte[] row = Bytes.toBytes("row");
    KeyValue kv = new KeyValue(row, row, row, row);
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    kvs.add(kv);

    WALProvider.Writer writer = null;
    WAL.Reader reader = null;
    // a regular table
    TableName t = TableName.valueOf("t");
    HRegionInfo tRegionInfo = null;
    int logCount = 0;
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "t");
    try {
      tRegionInfo = new HRegionInfo(t, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      WAL.Entry tEntry = createAEntry(new HLogKey(tRegionInfo.getEncodedNameAsBytes(), t,
          ++logCount, timestamp, HConstants.DEFAULT_CLUSTER_ID), kvs);

      // create a old root edit (-ROOT-).
      WAL.Entry rootEntry = createAEntry(new HLogKey(Bytes.toBytes(TableName.OLD_ROOT_STR),
          TableName.OLD_ROOT_TABLE_NAME, ++logCount, timestamp,
          HConstants.DEFAULT_CLUSTER_ID), kvs);

      // create a old meta edit (hbase:meta).
      WAL.Entry oldMetaEntry = createAEntry(new HLogKey(Bytes.toBytes(TableName.OLD_META_STR),
          TableName.OLD_META_TABLE_NAME, ++logCount, timestamp,
          HConstants.DEFAULT_CLUSTER_ID), kvs);

      // write above entries
      writer = WALFactory.createWALWriter(fs, path, conf);
      writer.append(tEntry);
      writer.append(rootEntry);
      writer.append(oldMetaEntry);

      // sync/close the writer
      writer.sync();
      writer.close();

      // read the log and see things are okay.
      reader = WALFactory.createReader(fs, path, conf);
      WAL.Entry entry = reader.next();
      assertNotNull(entry);
      assertTrue(entry.getKey().getTablename().equals(t));
      assertEquals(Bytes.toString(entry.getKey().getEncodedRegionName()),
        Bytes.toString(tRegionInfo.getEncodedNameAsBytes()));

      // read the ROOT waledit, but that will be ignored, and hbase:meta waledit will be read instead.
      entry = reader.next();
      assertEquals(entry.getKey().getTablename(), TableName.META_TABLE_NAME);
      // should reach end of log
      assertNull(reader.next());
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
}
  /**
   * Creates a WALEdit for the passed KeyValues and returns a WALProvider.Entry instance composed of
   * the WALEdit and passed WALKey.
   * @return WAL.Entry instance for the passed WALKey and KeyValues
   */
  private WAL.Entry createAEntry(WALKey walKey, List<KeyValue> kvs) {
    WALEdit edit = new WALEdit();
    for (KeyValue kv : kvs )
    edit.add(kv);
    return new WAL.Entry(walKey, edit);
  }

}
