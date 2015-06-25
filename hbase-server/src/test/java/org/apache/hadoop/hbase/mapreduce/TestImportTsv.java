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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestImportTsv implements Configurable {

  private static final Log LOG = LogFactory.getLog(TestImportTsv.class);
  protected static final String NAME = TestImportTsv.class.getSimpleName();
  protected static HBaseTestingUtility util = new HBaseTestingUtility();

  // Delete the tmp directory after running doMROnTableTest. Boolean. Default is true.
  protected static final String DELETE_AFTER_LOAD_CONF = NAME + ".deleteAfterLoad";

  /**
   * Force use of combiner in doMROnTableTest. Boolean. Default is true.
   */
  protected static final String FORCE_COMBINER_CONF = NAME + ".forceCombiner";

  private final String FAMILY = "FAM";
  private String table;
  private Map<String, String> args;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  public Configuration getConf() {
    return util.getConfiguration();
  }

  public void setConf(Configuration conf) {
    throw new IllegalArgumentException("setConf not supported");
  }

  @BeforeClass
  public static void provisionCluster() throws Exception {
    util.startMiniCluster();
    util.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void releaseCluster() throws Exception {
    util.shutdownMiniMapReduceCluster();
    util.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    table = "test-" + UUID.randomUUID();
    args = new HashMap<String, String>();
    // Prepare the arguments required for the test.
    args.put(ImportTsv.COLUMNS_CONF_KEY, "HBASE_ROW_KEY,FAM:A,FAM:B");
    args.put(ImportTsv.SEPARATOR_CONF_KEY, "\u001b");
  }

  @Test
  public void testMROnTable() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(null, 1);
    util.deleteTable(table);
  }
  
  @Test
  public void testMROnTableWithTimestamp() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);
    args.put(ImportTsv.COLUMNS_CONF_KEY, "HBASE_ROW_KEY,HBASE_TS_KEY,FAM:A,FAM:B");
    args.put(ImportTsv.SEPARATOR_CONF_KEY, ",");
    String data = "KEY,1234,VALUE1,VALUE2\n";

    doMROnTableTest(data, 1);
    util.deleteTable(table);
  }

  @Test
  public void testMROnTableWithCustomMapper()
  throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);
    args.put(ImportTsv.MAPPER_CONF_KEY,
        "org.apache.hadoop.hbase.mapreduce.TsvImporterCustomTestMapper");

    doMROnTableTest(null, 3);
    util.deleteTable(table);
  }
  
  @Test
  public void testBulkOutputWithoutAnExistingTable() throws Exception {
    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());

    doMROnTableTest(null, 3);
    util.deleteTable(table);
  }

  @Test
  public void testBulkOutputWithAnExistingTable() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);

    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());

    doMROnTableTest(null, 3);
    util.deleteTable(table);
  }
  
  @Test
  public void testBulkOutputWithAnExistingTableNoStrictTrue() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);

    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());
    args.put(ImportTsv.NO_STRICT_COL_FAMILY, "true");
    doMROnTableTest(null, 3);
    util.deleteTable(table);
  }

  @Test
  public void testJobConfigurationsWithTsvImporterTextMapper() throws Exception {
    Path bulkOutputPath = new Path(util.getDataTestDirOnTestFS(table),"hfiles");
    String INPUT_FILE = "InputFile1.csv";
    // Prepare the arguments required for the test.
    String[] args =
        new String[] {
            "-D" + ImportTsv.MAPPER_CONF_KEY
                + "=org.apache.hadoop.hbase.mapreduce.TsvImporterTextMapper",
            "-D" + ImportTsv.COLUMNS_CONF_KEY
                + "=HBASE_ROW_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=,",
            "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + bulkOutputPath.toString(), table,
            INPUT_FILE
            };
    assertEquals("running test job configuration failed.", 0, ToolRunner.run(
        new Configuration(util.getConfiguration()),
        new ImportTsv() {
          @Override
          public int run(String[] args) throws Exception {
            Job job = createSubmittableJob(getConf(), args);
            assertTrue(job.getMapperClass().equals(TsvImporterTextMapper.class));
            assertTrue(job.getReducerClass().equals(TextSortReducer.class));
            assertTrue(job.getMapOutputValueClass().equals(Text.class));
            return 0;
          }
        }, args));
    // Delete table created by createSubmittableJob.
    util.deleteTable(table);
  }

  @Test
  public void testBulkOutputWithTsvImporterTextMapper() throws Exception {
    Path bulkOutputPath = new Path(util.getDataTestDirOnTestFS(table),"hfiles");
    args.put(ImportTsv.MAPPER_CONF_KEY, "org.apache.hadoop.hbase.mapreduce.TsvImporterTextMapper");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, bulkOutputPath.toString());
    String data = "KEY\u001bVALUE4\u001bVALUE8\n";
    doMROnTableTest(data, 4);
    util.deleteTable(table);
  }

  @Test
  public void testWithoutAnExistingTableAndCreateTableSetToNo() throws Exception {
    String[] args = new String[] { table, "/inputFile" };

    Configuration conf = new Configuration(util.getConfiguration());
    conf.set(ImportTsv.COLUMNS_CONF_KEY, "HBASE_ROW_KEY,FAM:A");
    conf.set(ImportTsv.BULK_OUTPUT_CONF_KEY, "/output");
    conf.set(ImportTsv.CREATE_TABLE_CONF_KEY, "no");
    exception.expect(TableNotFoundException.class);
    assertEquals("running test job configuration failed.", 0,
        ToolRunner.run(new Configuration(util.getConfiguration()), new ImportTsv() {
              @Override public int run(String[] args) throws Exception {
                createSubmittableJob(getConf(), args);
                return 0;
              }
            }, args));
  }

  @Test
  public void testMRWithoutAnExistingTable() throws Exception {
    String[] args =
        new String[] { table, "/inputFile" };

    exception.expect(TableNotFoundException.class);
    assertEquals("running test job configuration failed.", 0, ToolRunner.run(
        new Configuration(util.getConfiguration()),
        new ImportTsv() {
          @Override
          public int run(String[] args) throws Exception {
            createSubmittableJob(getConf(), args);
            return 0;
          }
        }, args));
  }

  @Test
  public void testJobConfigurationsWithDryMode() throws Exception {
    Path bulkOutputPath = new Path(util.getDataTestDirOnTestFS(table),"hfiles");
    String INPUT_FILE = "InputFile1.csv";
    // Prepare the arguments required for the test.
    String[] argsArray = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=,",
        "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + bulkOutputPath.toString(),
        "-D" + ImportTsv.DRY_RUN_CONF_KEY + "=true",
        table,
        INPUT_FILE };
    assertEquals("running test job configuration failed.", 0, ToolRunner.run(
        new Configuration(util.getConfiguration()),
        new ImportTsv() {
          @Override
          public int run(String[] args) throws Exception {
            Job job = createSubmittableJob(getConf(), args);
            assertTrue(job.getOutputFormatClass().equals(NullOutputFormat.class));
            return 0;
          }
        }, argsArray));
    // Delete table created by createSubmittableJob.
    util.deleteTable(table);
  }

  @Test
  public void testDryModeWithoutBulkOutputAndTableExists() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);
    args.put(ImportTsv.DRY_RUN_CONF_KEY, "true");
    doMROnTableTest(null, 1);
    // Dry mode should not delete an existing table. If it's not present,
    // this will throw TableNotFoundException.
    util.deleteTable(table);
  }

  /**
   * If table is not present in non-bulk mode, dry run should fail just like
   * normal mode.
   */
  @Test
  public void testDryModeWithoutBulkOutputAndTableDoesNotExists() throws Exception {
    args.put(ImportTsv.DRY_RUN_CONF_KEY, "true");
    exception.expect(TableNotFoundException.class);
    doMROnTableTest(null, 1);
  }

  @Test public void testDryModeWithBulkOutputAndTableExists() throws Exception {
    util.createTable(TableName.valueOf(table), FAMILY);
    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());
    args.put(ImportTsv.DRY_RUN_CONF_KEY, "true");
    doMROnTableTest(null, 1);
    // Dry mode should not delete an existing table. If it's not present,
    // this will throw TableNotFoundException.
    util.deleteTable(table);
  }

  /**
   * If table is not present in bulk mode and create.table is not set to yes,
   * import should fail with TableNotFoundException.
   */
  @Test
  public void testDryModeWithBulkOutputAndTableDoesNotExistsCreateTableSetToNo() throws
      Exception {
    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());
    args.put(ImportTsv.DRY_RUN_CONF_KEY, "true");
    args.put(ImportTsv.CREATE_TABLE_CONF_KEY, "no");
    exception.expect(TableNotFoundException.class);
    doMROnTableTest(null, 1);
  }

  @Test
  public void testDryModeWithBulkModeAndTableDoesNotExistsCreateTableSetToYes() throws Exception {
    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    args.put(ImportTsv.BULK_OUTPUT_CONF_KEY, hfiles.toString());
    args.put(ImportTsv.DRY_RUN_CONF_KEY, "true");
    args.put(ImportTsv.CREATE_TABLE_CONF_KEY, "yes");
    doMROnTableTest(null, 1);
    // Verify temporary table was deleted.
    exception.expect(TableNotFoundException.class);
    util.deleteTable(table);
  }

  private Tool doMROnTableTest(String data, int valueMultiplier) throws Exception {
    return doMROnTableTest(util, table, FAMILY, data, args, valueMultiplier);
  }

  protected static Tool doMROnTableTest(HBaseTestingUtility util, String table,
      String family, String data, Map<String, String> args) throws Exception {
    return doMROnTableTest(util, table, family, data, args, 1);
  }

  /**
   * Run an ImportTsv job and perform basic validation on the results.
   * Returns the ImportTsv <code>Tool</code> instance so that other tests can
   * inspect it for further validation as necessary. This method is static to
   * insure non-reliance on instance's util/conf facilities.
   * @param args Any arguments to pass BEFORE inputFile path is appended.
   * @return The Tool instance used to run the test.
   */
  protected static Tool doMROnTableTest(HBaseTestingUtility util, String table,
      String family, String data, Map<String, String> args, int valueMultiplier)
  throws Exception {
    Configuration conf = new Configuration(util.getConfiguration());

    // populate input file
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = fs.makeQualified(new Path(util.getDataTestDirOnTestFS(table), "input.dat"));
    FSDataOutputStream op = fs.create(inputPath, true);
    if (data == null) {
      data = "KEY\u001bVALUE1\u001bVALUE2\n";
    }
    op.write(Bytes.toBytes(data));
    op.close();
    LOG.debug(String.format("Wrote test data to file: %s", inputPath));

    if (conf.getBoolean(FORCE_COMBINER_CONF, true)) {
      LOG.debug("Forcing combiner.");
      conf.setInt("mapreduce.map.combine.minspills", 1);
    }

    // Build args array.
    String[] argsArray = new String[args.size() + 2];
    Iterator it = args.entrySet().iterator();
    int i = 0;
    while (it.hasNext()) {
      Map.Entry pair = (Map.Entry) it.next();
      argsArray[i] = "-D" + pair.getKey() + "=" + pair.getValue();
      i++;
    }
    argsArray[i] = table;
    argsArray[i + 1] = inputPath.toString();

    // run the import
    Tool tool = new ImportTsv();
    LOG.debug("Running ImportTsv with arguments: " + argsArray);
    assertEquals(0, ToolRunner.run(conf, tool, argsArray));

    // Perform basic validation. If the input args did not include
    // ImportTsv.BULK_OUTPUT_CONF_KEY then validate data in the table.
    // Otherwise, validate presence of hfiles.
    boolean isDryRun = args.containsKey(ImportTsv.DRY_RUN_CONF_KEY) &&
        "true".equalsIgnoreCase(args.get(ImportTsv.DRY_RUN_CONF_KEY));
    if (args.containsKey(ImportTsv.BULK_OUTPUT_CONF_KEY)) {
      if (isDryRun) {
        assertFalse(String.format("Dry run mode, %s should not have been created.",
                 ImportTsv.BULK_OUTPUT_CONF_KEY),
            fs.exists(new Path(ImportTsv.BULK_OUTPUT_CONF_KEY)));
      } else {
        validateHFiles(fs, args.get(ImportTsv.BULK_OUTPUT_CONF_KEY), family);
      }
    } else {
      validateTable(conf, TableName.valueOf(table), family, valueMultiplier, isDryRun);
    }

    if (conf.getBoolean(DELETE_AFTER_LOAD_CONF, true)) {
      LOG.debug("Deleting test subdirectory");
      util.cleanupDataTestDirOnTestFS(table);
    }
    return tool;
  }

  /**
   * Confirm ImportTsv via data in online table.
   */
  private static void validateTable(Configuration conf, TableName tableName,
      String family, int valueMultiplier, boolean isDryRun) throws IOException {

    LOG.debug("Validating table.");
    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(tableName);
    boolean verified = false;
    long pause = conf.getLong("hbase.client.pause", 5 * 1000);
    int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    for (int i = 0; i < numRetries; i++) {
      try {
        Scan scan = new Scan();
        // Scan entire family.
        scan.addFamily(Bytes.toBytes(family));
        ResultScanner resScanner = table.getScanner(scan);
        int numRows = 0;
        for (Result res : resScanner) {
          numRows++;
          assertEquals(2, res.size());
          List<Cell> kvs = res.listCells();
          assertTrue(CellUtil.matchingRow(kvs.get(0), Bytes.toBytes("KEY")));
          assertTrue(CellUtil.matchingRow(kvs.get(1), Bytes.toBytes("KEY")));
          assertTrue(CellUtil.matchingValue(kvs.get(0), Bytes.toBytes("VALUE" + valueMultiplier)));
          assertTrue(CellUtil.matchingValue(kvs.get(1), Bytes.toBytes("VALUE" + 2 * valueMultiplier)));
          // Only one result set is expected, so let it loop.
        }
        if (isDryRun) {
          assertEquals(0, numRows);
        } else {
          assertEquals(1, numRows);
        }
        verified = true;
        break;
      } catch (NullPointerException e) {
        // If here, a cell was empty. Presume its because updates came in
        // after the scanner had been opened. Wait a while and retry.
      }
      try {
        Thread.sleep(pause);
      } catch (InterruptedException e) {
        // continue
      }
    }
    table.close();
    connection.close();
    assertTrue(verified);
  }

  /**
   * Confirm ImportTsv via HFiles on fs.
   */
  private static void validateHFiles(FileSystem fs, String outputPath, String family)
      throws IOException {
    // validate number and content of output columns
    LOG.debug("Validating HFiles.");
    Set<String> configFamilies = new HashSet<String>();
    configFamilies.add(family);
    Set<String> foundFamilies = new HashSet<String>();
    for (FileStatus cfStatus : fs.listStatus(new Path(outputPath), new OutputFilesFilter())) {
      String[] elements = cfStatus.getPath().toString().split(Path.SEPARATOR);
      String cf = elements[elements.length - 1];
      foundFamilies.add(cf);
      assertTrue(
        String.format(
          "HFile output contains a column family (%s) not present in input families (%s)",
          cf, configFamilies),
          configFamilies.contains(cf));
      for (FileStatus hfile : fs.listStatus(cfStatus.getPath())) {
        assertTrue(
          String.format("HFile %s appears to contain no data.", hfile.getPath()),
          hfile.getLen() > 0);
      }
    }
    assertTrue(String.format("HFile output does not contain the input family '%s'.", family),
        foundFamilies.contains(family));
  }
}

