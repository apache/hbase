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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.WALPlayer.WALKeyValueMapper;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Basic test for the WALPlayer M/R tool
 */
@Category({MapReduceTests.class, LargeTests.class})
public class TestWALPlayer {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestWALPlayer.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private static Path rootDir;
  private static Path walRootDir;
  private static FileSystem fs;
  private static FileSystem logFs;
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf= TEST_UTIL.getConfiguration();
    rootDir = TEST_UTIL.createRootDir();
    walRootDir = TEST_UTIL.createWALRootDir();
    fs = FSUtils.getRootDirFileSystem(conf);
    logFs = FSUtils.getWALFileSystem(conf);
    cluster = TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
    fs.delete(rootDir, true);
    logFs.delete(walRootDir, true);
  }

  /**
   * Simple end-to-end test
   * @throws Exception
   */
  @Test
  public void testWALPlayer() throws Exception {
    final TableName tableName1 = TableName.valueOf(name.getMethodName() + "1");
    final TableName tableName2 = TableName.valueOf(name.getMethodName() + "2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");
    final byte[] COLUMN2 = Bytes.toBytes("c2");
    final byte[] ROW = Bytes.toBytes("row");
    Table t1 = TEST_UTIL.createTable(tableName1, FAMILY);
    Table t2 = TEST_UTIL.createTable(tableName2, FAMILY);

    // put a row into the first table
    Put p = new Put(ROW);
    p.addColumn(FAMILY, COLUMN1, COLUMN1);
    p.addColumn(FAMILY, COLUMN2, COLUMN2);
    t1.put(p);
    // delete one column
    Delete d = new Delete(ROW);
    d.addColumns(FAMILY, COLUMN1);
    t1.delete(d);

    // replay the WAL, map table 1 to table 2
    WAL log = cluster.getRegionServer(0).getWAL(null);
    log.rollWriter();
    String walInputDir = new Path(cluster.getMaster().getMasterFileSystem()
        .getWALRootDir(), HConstants.HREGION_LOGDIR_NAME).toString();

    Configuration configuration= TEST_UTIL.getConfiguration();
    WALPlayer player = new WALPlayer(configuration);
    String optionName="_test_.name";
    configuration.set(optionName, "1000");
    player.setupTime(configuration, optionName);
    assertEquals(1000,configuration.getLong(optionName,0));
    assertEquals(0, ToolRunner.run(configuration, player,
        new String[] {walInputDir, tableName1.getNameAsString(),
        tableName2.getNameAsString() }));


    // verify the WAL was player into table 2
    Get g = new Get(ROW);
    Result r = t2.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN2));
  }

  /**
   * Test WALKeyValueMapper setup and map
   */
  @Test
  public void testWALKeyValueMapper() throws Exception {
    testWALKeyValueMapper(WALPlayer.TABLES_KEY);
  }

  @Test
  public void testWALKeyValueMapperWithDeprecatedConfig() throws Exception {
    testWALKeyValueMapper("hlog.input.tables");
  }

  private void testWALKeyValueMapper(final String tableConfigKey) throws Exception {
    Configuration configuration = new Configuration();
    configuration.set(tableConfigKey, "table");
    WALKeyValueMapper mapper = new WALKeyValueMapper();
    WALKey key = mock(WALKey.class);
    when(key.getTableName()).thenReturn(TableName.valueOf("table"));
    @SuppressWarnings("unchecked")
    Mapper<WALKey, WALEdit, ImmutableBytesWritable, Cell>.Context context = mock(Context.class);
    when(context.getConfiguration()).thenReturn(configuration);

    WALEdit value = mock(WALEdit.class);
    ArrayList<Cell> values = new ArrayList<>();
    KeyValue kv1 = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("family"), null);

    values.add(kv1);
    when(value.getCells()).thenReturn(values);
    mapper.setup(context);

    doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        ImmutableBytesWritable writer = (ImmutableBytesWritable) invocation.getArgument(0);
        MapReduceExtendedCell key = (MapReduceExtendedCell) invocation.getArgument(1);
        assertEquals("row", Bytes.toString(writer.get()));
        assertEquals("row", Bytes.toString(CellUtil.cloneRow(key)));
        return null;
      }
    }).when(context).write(any(), any());

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
        assertTrue(data.toString().contains("-Dwal.bulk.output=/path/for/output"));
      }

    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }

  }

}
