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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for the CopyTable M/R tool
 */
@Category(LargeTests.class)
public class TestCopyTable {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final String FAMILY_A_STRING = "a";
  private static final String FAMILY_B_STRING = "b";
  private static final byte[] FAMILY_A = Bytes.toBytes(FAMILY_A_STRING);
  private static final byte[] FAMILY_B = Bytes.toBytes(FAMILY_B_STRING);
  private static final byte[] QUALIFIER = Bytes.toBytes("q");


  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  private void doCopyTableTest(boolean bulkload) throws Exception {
    final TableName TABLENAME1 = TableName.valueOf("testCopyTable1");
    final TableName TABLENAME2 = TableName.valueOf("testCopyTable2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");

    Table t1 = TEST_UTIL.createTable(TABLENAME1, FAMILY);
    Table t2 = TEST_UTIL.createTable(TABLENAME2, FAMILY);

    // put rows into the first table
    for (int i = 0; i < 10; i++) {
      Put p = new Put(Bytes.toBytes("row" + i));
      p.add(FAMILY, COLUMN1, COLUMN1);
      t1.put(p);
    }

    CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());

    int code;
    if (bulkload) {
      code = copy.run(new String[] { "--new.name=" + TABLENAME2.getNameAsString(),
          "--bulkload", TABLENAME1.getNameAsString() });
    } else {
      code = copy.run(new String[] { "--new.name=" + TABLENAME2.getNameAsString(),
          TABLENAME1.getNameAsString() });
    }
    assertEquals("copy job failed", 0, code);

    // verify the data was copied into table 2
    for (int i = 0; i < 10; i++) {
      Get g = new Get(Bytes.toBytes("row" + i));
      Result r = t2.get(g);
      assertEquals(1, r.size());
      assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));
    }
    
    t1.close();
    t2.close();
    TEST_UTIL.deleteTable(TABLENAME1);
    TEST_UTIL.deleteTable(TABLENAME2);
  }

  /**
   * Simple end-to-end test
   * @throws Exception
   */
  @Test
  public void testCopyTable() throws Exception {
    doCopyTableTest(false);
  }
  
  /**
   * Simple end-to-end test with bulkload.
   */
  @Test
  public void testCopyTableWithBulkload() throws Exception {
    doCopyTableTest(true);
  }
  
  @Test
  public void testStartStopRow() throws Exception {
    final TableName TABLENAME1 = TableName.valueOf("testStartStopRow1");
    final TableName TABLENAME2 = TableName.valueOf("testStartStopRow2");
    final byte[] FAMILY = Bytes.toBytes("family");
    final byte[] COLUMN1 = Bytes.toBytes("c1");
    final byte[] ROW0 = Bytes.toBytes("row0");
    final byte[] ROW1 = Bytes.toBytes("row1");
    final byte[] ROW2 = Bytes.toBytes("row2");

    Table t1 = TEST_UTIL.createTable(TABLENAME1, FAMILY);
    Table t2 = TEST_UTIL.createTable(TABLENAME2, FAMILY);

    // put rows into the first table
    Put p = new Put(ROW0);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);
    p = new Put(ROW1);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);
    p = new Put(ROW2);
    p.add(FAMILY, COLUMN1, COLUMN1);
    t1.put(p);

    CopyTable copy = new CopyTable(TEST_UTIL.getConfiguration());
    assertEquals(
      0,
      copy.run(new String[] { "--new.name=" + TABLENAME2, "--startrow=row1",
          "--stoprow=row2", TABLENAME1.getNameAsString() }));

    // verify the data was copied into table 2
    // row1 exist, row0, row2 do not exist
    Get g = new Get(ROW1);
    Result r = t2.get(g);
    assertEquals(1, r.size());
    assertTrue(CellUtil.matchingQualifier(r.rawCells()[0], COLUMN1));

    g = new Get(ROW0);
    r = t2.get(g);
    assertEquals(0, r.size());
    
    g = new Get(ROW2);
    r = t2.get(g);
    assertEquals(0, r.size());
    
    t1.close();
    t2.close();
    TEST_UTIL.deleteTable(TABLENAME1);
    TEST_UTIL.deleteTable(TABLENAME2);
  }

  /**
   * Test copy of table from sourceTable to targetTable all rows from family a
   */
  @Test
  public void testRenameFamily() throws Exception {
    String sourceTable = "sourceTable";
    String targetTable = "targetTable";

    byte[][] families = { FAMILY_A, FAMILY_B };

    Table t = TEST_UTIL.createTable(Bytes.toBytes(sourceTable), families);
    Table t2 = TEST_UTIL.createTable(Bytes.toBytes(targetTable), families);
    Put p = new Put(ROW1);
    p.add(FAMILY_A, QUALIFIER,  Bytes.toBytes("Data11"));
    p.add(FAMILY_B, QUALIFIER,  Bytes.toBytes("Data12"));
    p.add(FAMILY_A, QUALIFIER,  Bytes.toBytes("Data13"));
    t.put(p);
    p = new Put(ROW2);
    p.add(FAMILY_B, QUALIFIER, Bytes.toBytes("Dat21"));
    p.add(FAMILY_A, QUALIFIER, Bytes.toBytes("Data22"));
    p.add(FAMILY_B, QUALIFIER, Bytes.toBytes("Data23"));
    t.put(p);

    long currentTime = System.currentTimeMillis();
    String[] args = new String[] { "--new.name=" + targetTable, "--families=a:b", "--all.cells",
        "--starttime=" + (currentTime - 100000), "--endtime=" + (currentTime + 100000),
        "--versions=1", sourceTable };
    assertNull(t2.get(new Get(ROW1)).getRow());

    assertTrue(runCopy(args));

    assertNotNull(t2.get(new Get(ROW1)).getRow());
    Result res = t2.get(new Get(ROW1));
    byte[] b1 = res.getValue(FAMILY_B, QUALIFIER);
    assertEquals("Data13", new String(b1));
    assertNotNull(t2.get(new Get(ROW2)).getRow());
    res = t2.get(new Get(ROW2));
    b1 = res.getValue(FAMILY_A, QUALIFIER);
    // Data from the family of B is not copied
    assertNull(b1);

  }

  /**
   * Test main method of CopyTable.
   */
  @Test
  public void testMainMethod() throws Exception {
    String[] emptyArgs = { "-h" };
    PrintStream oldWriter = System.err;
    ByteArrayOutputStream data = new ByteArrayOutputStream();
    PrintStream writer = new PrintStream(data);
    System.setErr(writer);
    SecurityManager SECURITY_MANAGER = System.getSecurityManager();
    LauncherSecurityManager newSecurityManager= new LauncherSecurityManager();
    System.setSecurityManager(newSecurityManager);
    try {
      CopyTable.main(emptyArgs);
      fail("should be exit");
    } catch (SecurityException e) {
      assertEquals(1, newSecurityManager.getExitCode());
    } finally {
      System.setErr(oldWriter);
      System.setSecurityManager(SECURITY_MANAGER);
    }
    assertTrue(data.toString().contains("rs.class"));
    // should print usage information
    assertTrue(data.toString().contains("Usage:"));
  }

  private boolean runCopy(String[] args) throws IOException, InterruptedException,
      ClassNotFoundException {
    GenericOptionsParser opts = new GenericOptionsParser(
        new Configuration(TEST_UTIL.getConfiguration()), args);
    Configuration configuration = opts.getConfiguration();
    args = opts.getRemainingArgs();
    Job job = new CopyTable(configuration).createSubmittableJob(args);
    job.waitForCompletion(false);
    return job.isSuccessful();
  }
}
