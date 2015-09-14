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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Utils.OutputFileUtils.OutputFilesFilter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestImportTsv implements Configurable {

  protected static final Log LOG = LogFactory.getLog(TestImportTsv.class);
  protected static final String NAME = TestImportTsv.class.getSimpleName();
  protected static HBaseTestingUtility util = new HBaseTestingUtility();

  /**
   * Delete the tmp directory after running doMROnTableTest. Boolean. Default is
   * false.
   */
  protected static final String DELETE_AFTER_LOAD_CONF = NAME + ".deleteAfterLoad";

  /**
   * Force use of combiner in doMROnTableTest. Boolean. Default is true.
   */
  protected static final String FORCE_COMBINER_CONF = NAME + ".forceCombiner";

  private final String FAMILY = "FAM";

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

  @Test
  public void testMROnTable() throws Exception {
    String table = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
        table
    };

    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(util, FAMILY, null, args, 1);
    util.deleteTable(table);
  }
  
  @Test
  public void testMROnTableWithTimestamp() throws Exception {
    String table = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY
            + "=HBASE_ROW_KEY,HBASE_TS_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=,",
        table
    };
    String data = "KEY,1234,VALUE1,VALUE2\n";

    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(util, FAMILY, data, args, 1);
    util.deleteTable(table);
  }
  

  @Test
  public void testMROnTableWithCustomMapper()
  throws Exception {
    String table = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.MAPPER_CONF_KEY + "=org.apache.hadoop.hbase.mapreduce.TsvImporterCustomTestMapper",
        table
    };

    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(util, FAMILY, null, args, 3);
    util.deleteTable(table);
  }
  
  @Test
  public void testBulkOutputWithoutAnExistingTable() throws Exception {
    String table = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
        "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + hfiles.toString(),
        table
    };

    doMROnTableTest(util, FAMILY, null, args, 3);
    util.deleteTable(table);
  }

  @Test
  public void testBulkOutputWithAnExistingTable() throws Exception {
    String table = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
        "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + hfiles.toString(),
        table
    };

    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(util, FAMILY, null, args, 3);
    util.deleteTable(table);
  }
  
  @Test
  public void testBulkOutputWithAnExistingTableNoStrictTrue() throws Exception {
    String table = "test-" + UUID.randomUUID();
    // Prepare the arguments required for the test.
    Path hfiles = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    String[] args = new String[] {
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
        "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + hfiles.toString(),
        "-D" + ImportTsv.NO_STRICT_COL_FAMILY + "=true",
        table
    };
    util.createTable(TableName.valueOf(table), FAMILY);
    doMROnTableTest(util, FAMILY, null, args, 3);
    util.deleteTable(table);
  }

  @Test
  public void testJobConfigurationsWithTsvImporterTextMapper() throws Exception {
    String table = "test-" + UUID.randomUUID();
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
    GenericOptionsParser opts = new GenericOptionsParser(util.getConfiguration(), args);
    args = opts.getRemainingArgs();
    Job job = ImportTsv.createSubmittableJob(util.getConfiguration(), args);
    assertTrue(job.getMapperClass().equals(TsvImporterTextMapper.class));
    assertTrue(job.getReducerClass().equals(TextSortReducer.class));
    assertTrue(job.getMapOutputValueClass().equals(Text.class));
  }

  @Test
  public void testBulkOutputWithTsvImporterTextMapper() throws Exception {
    String table = "test-" + UUID.randomUUID();
    String FAMILY = "FAM";
    Path bulkOutputPath = new Path(util.getDataTestDirOnTestFS(table),"hfiles");
    // Prepare the arguments required for the test.
    String[] args =
        new String[] {
            "-D" + ImportTsv.MAPPER_CONF_KEY
                + "=org.apache.hadoop.hbase.mapreduce.TsvImporterTextMapper",
            "-D" + ImportTsv.COLUMNS_CONF_KEY
                + "=HBASE_ROW_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b",
            "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + bulkOutputPath.toString(), table 
            };
    String data = "KEY\u001bVALUE4\u001bVALUE8\n";
    doMROnTableTest(util, FAMILY, data, args, 4);
  }

  @Test(expected = TableNotFoundException.class)
  public void testWithoutAnExistingTableAndCreateTableSetToNo() throws Exception {
    String table = "test-" + UUID.randomUUID();
    String[] args =
        new String[] { table, "/inputFile" };

    Configuration conf = new Configuration(util.getConfiguration());
    conf.set(ImportTsv.COLUMNS_CONF_KEY, "HBASE_ROW_KEY,FAM:A");
    conf.set(ImportTsv.BULK_OUTPUT_CONF_KEY, "/output");
    conf.set(ImportTsv.CREATE_TABLE_CONF_KEY, "no");
    ImportTsv.createSubmittableJob(conf, args);
  }

  @Test(expected = TableNotFoundException.class)
  public void testMRWithoutAnExistingTable() throws Exception {
    String table = "test-" + UUID.randomUUID();
    String[] args =
        new String[] { table, "/inputFile" };

    Configuration conf = new Configuration(util.getConfiguration());
    ImportTsv.createSubmittableJob(conf, args);
  }

  /**
   * If there are invalid data rows as inputs, then only those rows should be ignored.
   */
  @Test
  public void testTsvImporterTextMapperWithInvalidData() throws Exception {
    String table = "test-" + UUID.randomUUID();
    String FAMILY = "FAM";
    Path bulkOutputPath = new Path(util.getDataTestDirOnTestFS(table), "hfiles");
    // Prepare the arguments required for the test.
    String[] args =
        new String[] {
            "-D" + ImportTsv.MAPPER_CONF_KEY
                + "=org.apache.hadoop.hbase.mapreduce.TsvImporterTextMapper",
            "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,HBASE_TS_KEY,FAM:A,FAM:B",
            "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=,",
            "-D" + ImportTsv.BULK_OUTPUT_CONF_KEY + "=" + bulkOutputPath.toString(), table };
    // 3 Rows of data as input. 2 Rows are valid and 1 row is invalid as it doesn't have TS
    String data = "KEY,1234,VALUE1,VALUE2\nKEY\nKEY,1235,VALUE1,VALUE2\n";
    doMROnTableTest(util, FAMILY, data, args, 1, 4);
    util.deleteTable(table);
  }

  protected static Tool doMROnTableTest(HBaseTestingUtility util, String family,
      String data, String[] args) throws Exception {
    return doMROnTableTest(util, family, data, args, 1);
  }

  protected static Tool doMROnTableTest(HBaseTestingUtility util, String family, String data,
      String[] args, int valueMultiplier) throws Exception {
    return doMROnTableTest(util, family, data, args, 1, -1);
  }

  /**
   * Run an ImportTsv job and perform basic validation on the results.
   * Returns the ImportTsv <code>Tool</code> instance so that other tests can
   * inspect it for further validation as necessary. This method is static to
   * insure non-reliance on instance's util/conf facilities.
   * @param args Any arguments to pass BEFORE inputFile path is appended.
   * @return The Tool instance used to run the test.
   */
  protected static Tool doMROnTableTest(HBaseTestingUtility util, String family, String data,
      String[] args, int valueMultiplier, int expectedKVCount)
  throws Exception {
    String table = args[args.length - 1];
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

    // run the import
    List<String> argv = new ArrayList<String>(Arrays.asList(args));
    argv.add(inputPath.toString());
    Tool tool = new ImportTsv();
    LOG.debug("Running ImportTsv with arguments: " + argv);
    assertEquals(0, ToolRunner.run(conf, tool, argv.toArray(args)));

    // Perform basic validation. If the input args did not include
    // ImportTsv.BULK_OUTPUT_CONF_KEY then validate data in the table.
    // Otherwise, validate presence of hfiles.
    boolean createdHFiles = false;
    String outputPath = null;
    for (String arg : argv) {
      if (arg.contains(ImportTsv.BULK_OUTPUT_CONF_KEY)) {
        createdHFiles = true;
        // split '-Dfoo=bar' on '=' and keep 'bar'
        outputPath = arg.split("=")[1];
        break;
      }
    }

    if (createdHFiles)
      validateHFiles(fs, outputPath, family, expectedKVCount);
    else
      validateTable(conf, TableName.valueOf(table), family, valueMultiplier);

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
      String family, int valueMultiplier) throws IOException {

    LOG.debug("Validating table.");
    Table table = new HTable(conf, tableName);
    boolean verified = false;
    long pause = conf.getLong("hbase.client.pause", 5 * 1000);
    int numRetries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    for (int i = 0; i < numRetries; i++) {
      try {
        Scan scan = new Scan();
        // Scan entire family.
        scan.addFamily(Bytes.toBytes(family));
        ResultScanner resScanner = table.getScanner(scan);
        for (Result res : resScanner) {
          assertTrue(res.size() == 2);
          List<Cell> kvs = res.listCells();
          assertTrue(CellUtil.matchingRow(kvs.get(0), Bytes.toBytes("KEY")));
          assertTrue(CellUtil.matchingRow(kvs.get(1), Bytes.toBytes("KEY")));
          assertTrue(CellUtil.matchingValue(kvs.get(0), Bytes.toBytes("VALUE" + valueMultiplier)));
          assertTrue(CellUtil.matchingValue(kvs.get(1), Bytes.toBytes("VALUE" + 2 * valueMultiplier)));
          // Only one result set is expected, so let it loop.
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
    assertTrue(verified);
  }

  /**
   * Confirm ImportTsv via HFiles on fs.
   */
  private static void validateHFiles(FileSystem fs, String outputPath, String family,
      int expectedKVCount) throws IOException {
    // validate number and content of output columns
    LOG.debug("Validating HFiles.");
    Set<String> configFamilies = new HashSet<String>();
    configFamilies.add(family);
    Set<String> foundFamilies = new HashSet<String>();
    int actualKVCount = 0;
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
        // count the number of KVs from all the hfiles
        if (expectedKVCount > -1) {
          actualKVCount += getKVCountFromHfile(fs, hfile.getPath());
        }
      }
    }
    if (expectedKVCount > -1) {
      assertTrue(String.format(
        "KV count in output hfile=<%d> doesn't match with expected KV count=<%d>", actualKVCount,
        expectedKVCount), actualKVCount == expectedKVCount);
    }
  }

  /**
   * Method returns the total KVs in given hfile
   * @param fs File System
   * @param p HFile path
   * @return KV count in the given hfile
   * @throws IOException
   */
  private static int getKVCountFromHfile(FileSystem fs, Path p) throws IOException {
    Configuration conf = util.getConfiguration();
    HFile.Reader reader = HFile.createReader(fs, p, new CacheConfig(conf), conf);
    reader.loadFileInfo();
    HFileScanner scanner = reader.getScanner(false, false);
    scanner.seekTo();
    int count = 0;
    do {
      count++;
    } while (scanner.next());
    reader.close();
    return count;
  }
}

