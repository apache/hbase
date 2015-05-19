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

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Validate ImportTsv + LoadIncrementalHFiles on a distributed cluster.
 */
@Category(IntegrationTests.class)
public class IntegrationTestImportTsv implements Configurable, Tool {

  private static final String NAME = IntegrationTestImportTsv.class.getSimpleName();
  private static final Log LOG = LogFactory.getLog(IntegrationTestImportTsv.class);

  protected static final String simple_tsv =
      "row1\t1\tc1\tc2\n" +
      "row2\t1\tc1\tc2\n" +
      "row3\t1\tc1\tc2\n" +
      "row4\t1\tc1\tc2\n" +
      "row5\t1\tc1\tc2\n" +
      "row6\t1\tc1\tc2\n" +
      "row7\t1\tc1\tc2\n" +
      "row8\t1\tc1\tc2\n" +
      "row9\t1\tc1\tc2\n" +
      "row10\t1\tc1\tc2\n";

  protected static final Set<KeyValue> simple_expected =
      new TreeSet<KeyValue>(CellComparator.COMPARATOR) {
    private static final long serialVersionUID = 1L;
    {
      byte[] family = Bytes.toBytes("d");
      for (String line : simple_tsv.split("\n")) {
        String[] row = line.split("\t");
        byte[] key = Bytes.toBytes(row[0]);
        long ts = Long.parseLong(row[1]);
        byte[][] fields = { Bytes.toBytes(row[2]), Bytes.toBytes(row[3]) };
        add(new KeyValue(key, family, fields[0], ts, Type.Put, fields[0]));
        add(new KeyValue(key, family, fields[1], ts, Type.Put, fields[1]));
      }
    }
  };

  // this instance is initialized on first access when the test is run from
  // JUnit/Maven or by main when run from the CLI.
  protected static IntegrationTestingUtility util = null;

  public Configuration getConf() {
    return util.getConfiguration();
  }

  public void setConf(Configuration conf) {
    throw new IllegalArgumentException("setConf not supported");
  }

  @BeforeClass
  public static void provisionCluster() throws Exception {
    if (null == util) {
      util = new IntegrationTestingUtility();
    }
    util.initializeCluster(1);
    if (!util.isDistributedCluster()) {
      // also need MR when running without a real cluster
      util.startMiniMapReduceCluster();
    }
  }

  @AfterClass
  public static void releaseCluster() throws Exception {
    util.restoreCluster();
    if (!util.isDistributedCluster()) {
      util.shutdownMiniMapReduceCluster();
    }
    util = null;
  }

  /**
   * Verify the data described by <code>simple_tsv</code> matches
   * <code>simple_expected</code>.
   */
  protected void doLoadIncrementalHFiles(Path hfiles, TableName tableName)
      throws Exception {

    String[] args = { hfiles.toString(), tableName.getNameAsString() };
    LOG.info(format("Running LoadIncrememntalHFiles with args: %s", Arrays.asList(args)));
    assertEquals("Loading HFiles failed.",
      0, ToolRunner.run(new LoadIncrementalHFiles(new Configuration(getConf())), args));

    Table table = null;
    Scan scan = new Scan() {{
      setCacheBlocks(false);
      setCaching(1000);
    }};
    try {
      table = util.getConnection().getTable(tableName);
      Iterator<Result> resultsIt = table.getScanner(scan).iterator();
      Iterator<KeyValue> expectedIt = simple_expected.iterator();
      while (resultsIt.hasNext() && expectedIt.hasNext()) {
        Result r = resultsIt.next();
        for (Cell actual : r.rawCells()) {
          assertTrue(
            "Ran out of expected values prematurely!",
            expectedIt.hasNext());
          KeyValue expected = expectedIt.next();
          assertTrue(
            format("Scan produced surprising result. expected: <%s>, actual: %s",
              expected, actual),
            CellComparator.COMPARATOR.compare(expected, actual) == 0);
        }
      }
      assertFalse("Did not consume all expected values.", expectedIt.hasNext());
      assertFalse("Did not consume all scan results.", resultsIt.hasNext());
    } finally {
      if (null != table) table.close();
    }
  }

  /**
   * Confirm the absence of the {@link TotalOrderPartitioner} partitions file.
   */
  protected static void validateDeletedPartitionsFile(Configuration conf) throws IOException {
    if (!conf.getBoolean(IntegrationTestingUtility.IS_DISTRIBUTED_CLUSTER, false))
      return;

    FileSystem fs = FileSystem.get(conf);
    Path partitionsFile = new Path(TotalOrderPartitioner.getPartitionFile(conf));
    assertFalse("Failed to clean up partitions file.", fs.exists(partitionsFile));
  }

  @Test
  public void testGenerateAndLoad() throws Exception {
    LOG.info("Running test testGenerateAndLoad.");
    TableName table = TableName.valueOf(NAME + "-" + UUID.randomUUID());
    String cf = "d";
    Path hfiles = new Path(
        util.getDataTestDirOnTestFS(table.getNameAsString()), "hfiles");

    String[] args = {
        format("-D%s=%s", ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles),
        format("-D%s=HBASE_ROW_KEY,HBASE_TS_KEY,%s:c1,%s:c2",
          ImportTsv.COLUMNS_CONF_KEY, cf, cf),
        // configure the test harness to NOT delete the HFiles after they're
        // generated. We need those for doLoadIncrementalHFiles
        format("-D%s=false", TestImportTsv.DELETE_AFTER_LOAD_CONF),
        table.getNameAsString()
    };

    // run the job, complete the load.
    util.createTable(table, new String[]{cf});
    Tool t = TestImportTsv.doMROnTableTest(util, cf, simple_tsv, args);
    doLoadIncrementalHFiles(hfiles, table);

    // validate post-conditions
    validateDeletedPartitionsFile(t.getConf());

    // clean up after ourselves.
    util.deleteTable(table);
    util.cleanupDataTestDirOnTestFS(table.getNameAsString());
    LOG.info("testGenerateAndLoad completed successfully.");
  }

  public int run(String[] args) throws Exception {
    if (args.length != 0) {
      System.err.println(format("%s [genericOptions]", NAME));
      System.err.println("  Runs ImportTsv integration tests against a distributed cluster.");
      System.err.println();
      GenericOptionsParser.printGenericCommandUsage(System.err);
      return 1;
    }

    // adding more test methods? Don't forget to add them here... or consider doing what
    // IntegrationTestsDriver does.
    provisionCluster();
    testGenerateAndLoad();
    releaseCluster();

    return 0;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    util = new IntegrationTestingUtility(conf);
    // not using ToolRunner to avoid unnecessary call to setConf()
    args = new GenericOptionsParser(conf, args).getRemainingArgs();
    int status = new IntegrationTestImportTsv().run(args);
    System.exit(status);
  }
}
