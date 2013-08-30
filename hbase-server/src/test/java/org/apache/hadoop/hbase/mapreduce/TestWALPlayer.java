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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for the WALPlayer M/R tool
 */
@Category(LargeTests.class)
public class TestWALPlayer {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster();
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Simple end-to-end test
   * @throws Exception
   */
  @Test
  public void testWALPlayer() throws Exception {
    final byte[] TABLENAME1 = Bytes.toBytes("testWALPlayer1");
    final byte[] TABLENAME2 = Bytes.toBytes("testWALPlayer2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");
    final byte[] COLUMN2 = Bytes.toBytes("c2");
    final byte[] ROW = Bytes.toBytes("row");
    HTable t1 = TEST_UTIL.createTable(TABLENAME1, FAMILY);
    HTable t2 = TEST_UTIL.createTable(TABLENAME2, FAMILY);

    // put a row into the first table
    Put p = new Put(ROW);
    p.add(FAMILY, COLUMN1, COLUMN1);
    p.add(FAMILY, COLUMN2, COLUMN2);
    t1.put(p);
    // delete one column
    Delete d = new Delete(ROW);
    d.deleteColumns(FAMILY, COLUMN1);
    t1.delete(d);

    // replay the WAL, map table 1 to table 2
    HLog log = cluster.getRegionServer(0).getWAL();
    log.rollWriter();
    String walInputDir = new Path(cluster.getMaster().getMasterFileSystem()
        .getRootDir(), HConstants.HREGION_LOGDIR_NAME).toString();

    WALPlayer player = new WALPlayer(TEST_UTIL.getConfiguration());
    assertEquals(0, player.run(new String[] { walInputDir, Bytes.toString(TABLENAME1),
        Bytes.toString(TABLENAME2) }));

    // verify the WAL was player into table 2
    Get g = new Get(ROW);
    Result r = t2.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.raw()[0], COLUMN2));
  }
}
