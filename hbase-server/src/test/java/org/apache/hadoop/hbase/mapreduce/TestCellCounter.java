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

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LauncherSecurityManager;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.*;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category({MapReduceTests.class, LargeTests.class})
public class TestCellCounter {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final byte[] ROW1 = Bytes.toBytes("row1");
  private static final byte[] ROW2 = Bytes.toBytes("row2");
  private static final String FAMILY_A_STRING = "a";
  private static final String FAMILY_B_STRING = "b";
  private static final byte[] FAMILY_A = Bytes.toBytes(FAMILY_A_STRING);
  private static final byte[] FAMILY_B = Bytes.toBytes(FAMILY_B_STRING);
  private static final byte[] QUALIFIER = Bytes.toBytes("q");

  private static Path FQ_OUTPUT_DIR;
  private static final String OUTPUT_DIR = "target" + File.separator + "test-data" + File.separator
      + "output";
  private static long now = System.currentTimeMillis();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    UTIL.startMiniMapReduceCluster();
    FQ_OUTPUT_DIR = new Path(OUTPUT_DIR).makeQualified(new LocalFileSystem());
    FileUtil.fullyDelete(new File(OUTPUT_DIR));
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  /**
   * Test CellCounter all data should print to output
   * 
   */
  @Test (timeout=300000)
  public void testCellCounter() throws Exception {
    String sourceTable = "sourceTable";
    byte[][] families = { FAMILY_A, FAMILY_B };
    Table t = UTIL.createTable(Bytes.toBytes(sourceTable), families);
    try{
    Put p = new Put(ROW1);
    p.add(FAMILY_A, QUALIFIER, now, Bytes.toBytes("Data11"));
    p.add(FAMILY_B, QUALIFIER, now + 1, Bytes.toBytes("Data12"));
    p.add(FAMILY_A, QUALIFIER, now + 2, Bytes.toBytes("Data13"));
    t.put(p);
    p = new Put(ROW2);
    p.add(FAMILY_B, QUALIFIER, now, Bytes.toBytes("Dat21"));
    p.add(FAMILY_A, QUALIFIER, now + 1, Bytes.toBytes("Data22"));
    p.add(FAMILY_B, QUALIFIER, now + 2, Bytes.toBytes("Data23"));
    t.put(p);
    String[] args = { sourceTable, FQ_OUTPUT_DIR.toString(), ";", "^row1" };
    runCount(args);
    FileInputStream inputStream = new FileInputStream(OUTPUT_DIR + File.separator + 
        "part-r-00000");
    String data = IOUtils.toString(inputStream);
    inputStream.close();
    assertTrue(data.contains("Total Families Across all Rows" + "\t" + "2"));
    assertTrue(data.contains("Total Qualifiers across all Rows" + "\t" + "2"));
    assertTrue(data.contains("Total ROWS" + "\t" + "1"));
    assertTrue(data.contains("b;q" + "\t" + "1"));
    assertTrue(data.contains("a;q" + "\t" + "1"));
    assertTrue(data.contains("row1;a;q_Versions" + "\t" + "1"));
    assertTrue(data.contains("row1;b;q_Versions" + "\t" + "1"));
    }finally{
      t.close();
    }

  }

  private boolean runCount(String[] args) throws IOException, InterruptedException,
      ClassNotFoundException {
    // need to make a copy of the configuration because to make sure
    // different temp dirs are used.
    GenericOptionsParser opts = new GenericOptionsParser(
        new Configuration(UTIL.getConfiguration()), args);
    Configuration configuration = opts.getConfiguration();
    args = opts.getRemainingArgs();
    Job job = CellCounter.createSubmittableJob(configuration, args);
    job.waitForCompletion(false);
    return job.isSuccessful();
  }

  /**
   * Test main method of CellCounter
   */
  @Test (timeout=300000)
  public void testCellCounterMain() throws Exception {

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
        CellCounter.main(args);
        fail("should be SecurityException");
      } catch (SecurityException e) {
        assertEquals(-1, newSecurityManager.getExitCode());
        assertTrue(data.toString().contains("ERROR: Wrong number of parameters:"));
        // should be information about usage
        assertTrue(data.toString().contains("Usage:"));
      }

    } finally {
      System.setErr(oldPrintStream);
      System.setSecurityManager(SECURITY_MANAGER);
    }
  }
}