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

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestBase;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.IntegrationTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;

/**
 * An integration test to test {@link TableSnapshotInputFormat} which enables
 * reading directly from snapshot files without going through hbase servers.
 *
 * This test creates a table and loads the table with the rows ranging from
 * 'aaa' to 'zzz', and for each row, sets the columns f1:(null) and f2:(null) to be
 * the the same as the row value.
 * <pre>
 * aaa, f1: => aaa
 * aaa, f2: => aaa
 * aab, f1: => aab
 * ....
 * zzz, f2: => zzz
 * </pre>
 *
 * Then the test creates a snapshot from this table, and overrides the values in the original
 * table with values 'after_snapshot_value'. The test, then runs a mapreduce job over the snapshot
 * with a scan start row 'bbb' and stop row 'yyy'. The data is saved in a single reduce output
 * file, and inspected later to verify that the MR job has seen all the values from the snapshot.
 *
 * <p> These parameters can be used to configure the job:
 * <br>"IntegrationTestTableSnapshotInputFormat.table" =&gt; the name of the table
 * <br>"IntegrationTestTableSnapshotInputFormat.snapshot" =&gt; the name of the snapshot
 * <br>"IntegrationTestTableSnapshotInputFormat.numRegions" =&gt; number of regions in the table
 * to be created (default, 32).
 * <br>"IntegrationTestTableSnapshotInputFormat.tableDir" =&gt; temporary directory to restore the
 * snapshot files
 * <br>"IntegrationTestTableSnapshotInputFormat.tableDir" =&gt; temporary directory to restore the
 * snapshot files
 */
@Category(IntegrationTests.class)
// Not runnable as a unit test. See TestTableSnapshotInputFormat
public class IntegrationTestTableSnapshotInputFormat extends IntegrationTestBase {

  private static final Log LOG = LogFactory.getLog(IntegrationTestTableSnapshotInputFormat.class);

  private static final String TABLE_NAME_KEY = "IntegrationTestTableSnapshotInputFormat.table";
  private static final String DEFAULT_TABLE_NAME = "IntegrationTestTableSnapshotInputFormat";

  private static final String SNAPSHOT_NAME_KEY = "IntegrationTestTableSnapshotInputFormat.snapshot";

  private static final String MR_IMPLEMENTATION_KEY =
    "IntegrationTestTableSnapshotInputFormat.API";
  private static final String MAPRED_IMPLEMENTATION = "mapred";
  private static final String MAPREDUCE_IMPLEMENTATION = "mapreduce";

  private static final String NUM_REGIONS_KEY =
    "IntegrationTestTableSnapshotInputFormat.numRegions";
  private static final int DEFAULT_NUM_REGIONS = 32;
  private static final String TABLE_DIR_KEY = "IntegrationTestTableSnapshotInputFormat.tableDir";

  private static final byte[] START_ROW = Bytes.toBytes("bbb");
  private static final byte[] END_ROW = Bytes.toBytes("yyy");

  // mapred API missing feature pairity with mapreduce. See comments in
  // mapred.TestTableSnapshotInputFormat
  private static final byte[] MAPRED_START_ROW = Bytes.toBytes("aaa");
  private static final byte[] MAPRED_END_ROW = Bytes.toBytes("zz{"); // 'z' + 1 => '{'

  private IntegrationTestingUtility util;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    util = getTestingUtil(conf);
  }

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    util = getTestingUtil(getConf());
    util.initializeCluster(1);
    this.setConf(util.getConfiguration());
  }

  @Override
  @After
  public void cleanUp() throws Exception {
    util.restoreCluster();
  }

  @Override
  public void setUpCluster() throws Exception {
  }

  @Override
  public int runTestFromCommandLine() throws Exception {
    Configuration conf = getConf();
    TableName tableName = TableName.valueOf(conf.get(TABLE_NAME_KEY, DEFAULT_TABLE_NAME));
    String snapshotName = conf.get(SNAPSHOT_NAME_KEY, tableName.getQualifierAsString()
      + "_snapshot_" + System.currentTimeMillis());
    int numRegions = conf.getInt(NUM_REGIONS_KEY, DEFAULT_NUM_REGIONS);
    String tableDirStr = conf.get(TABLE_DIR_KEY);
    Path tableDir;
    if (tableDirStr == null) {
      tableDir = util.getDataTestDirOnTestFS(tableName.getQualifierAsString());
    } else {
      tableDir = new Path(tableDirStr);
    }

    final String mr = conf.get(MR_IMPLEMENTATION_KEY, MAPREDUCE_IMPLEMENTATION);
    if (mr.equalsIgnoreCase(MAPREDUCE_IMPLEMENTATION)) {
      /*
       * We create the table using HBaseAdmin#createTable(), which will create the table
       * with desired number of regions. We pass bbb as startKey and yyy as endKey, so if
       * desiredNumRegions is > 2, we create regions empty - bbb and yyy - empty, and we
       * create numRegions - 2 regions between bbb - yyy. The test uses a Scan with startRow
       * bbb and endRow yyy, so, we expect the first and last region to be filtered out in
       * the input format, and we expect numRegions - 2 splits between bbb and yyy.
       */
      LOG.debug("Running job with mapreduce API.");
      int expectedNumSplits = numRegions > 2 ? numRegions - 2 : numRegions;

      org.apache.hadoop.hbase.mapreduce.TestTableSnapshotInputFormat.doTestWithMapReduce(util,
        tableName, snapshotName, START_ROW, END_ROW, tableDir, numRegions,
        expectedNumSplits, false);
    } else if (mr.equalsIgnoreCase(MAPRED_IMPLEMENTATION)) {
      /*
       * Similar considerations to above. The difference is that mapred API does not support
       * specifying start/end rows (or a scan object at all). Thus the omission of first and
       * last regions are not performed. See comments in mapred.TestTableSnapshotInputFormat
       * for details of how that test works around the problem. This feature should be added
       * in follow-on work.
       */
      LOG.debug("Running job with mapred API.");
      int expectedNumSplits = numRegions;

      org.apache.hadoop.hbase.mapred.TestTableSnapshotInputFormat.doTestWithMapReduce(util,
        tableName, snapshotName, MAPRED_START_ROW, MAPRED_END_ROW, tableDir, numRegions,
        expectedNumSplits, false);
    } else {
      throw new IllegalArgumentException("Unrecognized mapreduce implementation: " + mr +".");
    }

    return 0;
  }

  @Override // CM is not intended to be run with this test
  public String getTablename() {
    return null;
  }

  @Override
  protected Set<String> getColumnFamilies() {
    return null;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestTableSnapshotInputFormat(), args);
    System.exit(ret);
  }

}
