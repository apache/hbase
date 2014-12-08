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
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.WALPlayer.HLogKeyValueMapper;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

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

    Configuration configuration= TEST_UTIL.getConfiguration();
    WALPlayer player = new WALPlayer(configuration);
    String optionName="_test_.name";
    configuration.set(optionName, "1000");
    player.setupTime(configuration, optionName);
    assertEquals(1000,configuration.getLong(optionName,0));
    assertEquals(0, player.run(new String[] { walInputDir, Bytes.toString(TABLENAME1),
        Bytes.toString(TABLENAME2) }));

    
    // verify the WAL was player into table 2
    Get g = new Get(ROW);
    Result r = t2.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN2));
  }

  /**
   * Test HLogKeyValueMapper setup and map
   */
  @Test
  public void testHLogKeyValueMapper() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(WALPlayer.TABLES_KEY, "table");
    HLogKeyValueMapper mapper = new HLogKeyValueMapper();
    HLogKey key = mock(HLogKey.class);
    when(key.getTablename()).thenReturn(TableName.valueOf("table"));
    @SuppressWarnings("unchecked")
    Mapper<HLogKey, WALEdit, ImmutableBytesWritable, KeyValue>.Context context =
        mock(Context.class);
    when(context.getConfiguration()).thenReturn(configuration);

    WALEdit value = mock(WALEdit.class);
    ArrayList<KeyValue> values = new ArrayList<KeyValue>();
    KeyValue kv1 = mock(KeyValue.class);
    when(kv1.getFamily()).thenReturn(Bytes.toBytes("family"));
    when(kv1.getRow()).thenReturn(Bytes.toBytes("row"));
    values.add(kv1);
    when(value.getKeyValues()).thenReturn(values);
    mapper.setup(context);

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ImmutableBytesWritable writer = (ImmutableBytesWritable) invocation.getArguments()[0];
        KeyValue key = (KeyValue) invocation.getArguments()[1];
        assertEquals("row", Bytes.toString(writer.get()));
        assertEquals("row", Bytes.toString(key.getRow()));
        return null;
      }
    }).when(context).write(any(ImmutableBytesWritable.class), any(KeyValue.class));

    mapper.map(key, value, context);

  }

  /**
   * Test main method
   */
  @Test
  public void testMainMethod() throws Exception {

    PrintStream oldPrintStream = System.err;
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    String[] args = {};
    System.setErr(new PrintStream(data));
    try {
      System.setErr(new PrintStream(data));
      try {
        WALPlayer.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(-1, newSecurityManager.getExitCode());
        assertTrue(data.toString().contains("ERROR: Wrong number of arguments:"));
        assertTrue(data.toString().contains("Usage: WALPlayer [options] <wal inputdir>" +
            " <tables> [<tableMappings>]"));
        assertTrue(data.toString().contains("-Dhlog.bulk.output=/path/for/output"));
      }

    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }

  }

}
