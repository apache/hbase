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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestImportTSVWithOperationAttributes implements Configurable {

  private static final Log LOG = LogFactory.getLog(TestImportTSVWithOperationAttributes.class);
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

  private static Configuration conf;

  private static final String TEST_ATR_KEY = "test";

  private final String FAMILY = "FAM";

  public Configuration getConf() {
    return util.getConfiguration();
  }

  public void setConf(Configuration conf) {
    throw new IllegalArgumentException("setConf not supported");
  }

  @BeforeClass
  public static void provisionCluster() throws Exception {
    conf = util.getConfiguration();
    conf.set("hbase.coprocessor.master.classes", OperationAttributesTestController.class.getName());
    conf.set("hbase.coprocessor.region.classes", OperationAttributesTestController.class.getName());
    util.startMiniCluster();
    Admin admin = new HBaseAdmin(util.getConfiguration());
    util.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void releaseCluster() throws Exception {
    util.shutdownMiniMapReduceCluster();
    util.shutdownMiniCluster();
  }

  @Test
  public void testMROnTable() throws Exception {
    String tableName = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.MAPPER_CONF_KEY
            + "=org.apache.hadoop.hbase.mapreduce.TsvImporterCustomTestMapperForOprAttr",
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B,HBASE_ATTRIBUTES_KEY",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b", tableName };
    String data = "KEY\u001bVALUE1\u001bVALUE2\u001btest=>myvalue\n";
    util.createTable(TableName.valueOf(tableName), FAMILY);
    doMROnTableTest(util, FAMILY, data, args, 1, true);
    util.deleteTable(tableName);
  }

  @Test
  public void testMROnTableWithInvalidOperationAttr() throws Exception {
    String tableName = "test-" + UUID.randomUUID();

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.MAPPER_CONF_KEY
            + "=org.apache.hadoop.hbase.mapreduce.TsvImporterCustomTestMapperForOprAttr",
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B,HBASE_ATTRIBUTES_KEY",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b", tableName };
    String data = "KEY\u001bVALUE1\u001bVALUE2\u001btest1=>myvalue\n";
    util.createTable(TableName.valueOf(tableName), FAMILY);
    doMROnTableTest(util, FAMILY, data, args, 1, false);
    util.deleteTable(tableName);
  }

  /**
   * Run an ImportTsv job and perform basic validation on the results. Returns
   * the ImportTsv <code>Tool</code> instance so that other tests can inspect it
   * for further validation as necessary. This method is static to insure
   * non-reliance on instance's util/conf facilities.
   * 
   * @param args
   *          Any arguments to pass BEFORE inputFile path is appended.
   * @param dataAvailable
   * @return The Tool instance used to run the test.
   */
  private Tool doMROnTableTest(HBaseTestingUtility util, String family, String data, String[] args,
      int valueMultiplier, boolean dataAvailable) throws Exception {
    String table = args[args.length - 1];
    Configuration conf = new Configuration(util.getConfiguration());

    // populate input file
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = fs.makeQualified(new Path(util.getDataTestDirOnTestFS(table), "input.dat"));
    FSDataOutputStream op = fs.create(inputPath, true);
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

    validateTable(conf, TableName.valueOf(table), family, valueMultiplier, dataAvailable);

    if (conf.getBoolean(DELETE_AFTER_LOAD_CONF, true)) {
      LOG.debug("Deleting test subdirectory");
      util.cleanupDataTestDirOnTestFS(table);
    }
    return tool;
  }

  /**
   * Confirm ImportTsv via data in online table.
   * 
   * @param dataAvailable
   */
  private static void validateTable(Configuration conf, TableName tableName, String family,
      int valueMultiplier, boolean dataAvailable) throws IOException {

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
        if (dataAvailable) {
          ResultScanner resScanner = table.getScanner(scan);
          for (Result res : resScanner) {
            LOG.debug("Getting results " + res.size());
            assertTrue(res.size() == 2);
            List<Cell> kvs = res.listCells();
            assertTrue(CellUtil.matchingRow(kvs.get(0), Bytes.toBytes("KEY")));
            assertTrue(CellUtil.matchingRow(kvs.get(1), Bytes.toBytes("KEY")));
            assertTrue(CellUtil.matchingValue(kvs.get(0), Bytes.toBytes("VALUE" + valueMultiplier)));
            assertTrue(CellUtil.matchingValue(kvs.get(1),
                Bytes.toBytes("VALUE" + 2 * valueMultiplier)));
            // Only one result set is expected, so let it loop.
            verified = true;
          }
        } else {
          ResultScanner resScanner = table.getScanner(scan);
          Result[] next = resScanner.next(2);
          assertEquals(0, next.length);
          verified = true;
        }

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

  public static class OperationAttributesTestController extends BaseRegionObserver {

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
        Durability durability) throws IOException {
      Region region = e.getEnvironment().getRegion();
      if (!region.getRegionInfo().isMetaTable()
          && !region.getRegionInfo().getTable().isSystemTable()) {
        if (put.getAttribute(TEST_ATR_KEY) != null) {
          LOG.debug("allow any put to happen " + region.getRegionInfo().getRegionNameAsString());
        } else {
          e.bypass();
        }
      }
    }
  }
}
