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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MapReduceTests.class, LargeTests.class})
public class TestImportTSVWithTTLs implements Configurable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestImportTSVWithTTLs.class);

  protected static final Logger LOG = LoggerFactory.getLogger(TestImportTSVWithTTLs.class);
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
  private static Configuration conf;

  @Rule
  public TestName name = new TestName();

  @Override
  public Configuration getConf() {
    return util.getConfiguration();
  }

  @Override
  public void setConf(Configuration conf) {
    throw new IllegalArgumentException("setConf not supported");
  }

  @BeforeClass
  public static void provisionCluster() throws Exception {
    conf = util.getConfiguration();
    // We don't check persistence in HFiles in this test, but if we ever do we will
    // need this where the default hfile version is not 3 (i.e. 0.98)
    conf.setInt("hfile.format.version", 3);
    conf.set("hbase.coprocessor.region.classes", TTLCheckingObserver.class.getName());
    util.startMiniCluster();
  }

  @AfterClass
  public static void releaseCluster() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testMROnTable() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName() + util.getRandomUUID());

    // Prepare the arguments required for the test.
    String[] args = new String[] {
        "-D" + ImportTsv.MAPPER_CONF_KEY
            + "=org.apache.hadoop.hbase.mapreduce.TsvImporterMapper",
        "-D" + ImportTsv.COLUMNS_CONF_KEY + "=HBASE_ROW_KEY,FAM:A,FAM:B,HBASE_CELL_TTL",
        "-D" + ImportTsv.SEPARATOR_CONF_KEY + "=\u001b", tableName.getNameAsString() };
    String data = "KEY\u001bVALUE1\u001bVALUE2\u001b1000000\n";
    util.createTable(tableName, FAMILY);
    doMROnTableTest(util, FAMILY, data, args, 1);
    util.deleteTable(tableName);
  }

  protected static Tool doMROnTableTest(HBaseTestingUtility util, String family, String data,
      String[] args, int valueMultiplier) throws Exception {
    TableName table = TableName.valueOf(args[args.length - 1]);
    Configuration conf = new Configuration(util.getConfiguration());

    // populate input file
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = fs.makeQualified(new Path(util
        .getDataTestDirOnTestFS(table.getNameAsString()), "input.dat"));
    FSDataOutputStream op = fs.create(inputPath, true);
    op.write(Bytes.toBytes(data));
    op.close();
    LOG.debug(String.format("Wrote test data to file: %s", inputPath));

    if (conf.getBoolean(FORCE_COMBINER_CONF, true)) {
      LOG.debug("Forcing combiner.");
      conf.setInt("mapreduce.map.combine.minspills", 1);
    }

    // run the import
    List<String> argv = new ArrayList<>(Arrays.asList(args));
    argv.add(inputPath.toString());
    Tool tool = new ImportTsv();
    LOG.debug("Running ImportTsv with arguments: " + argv);
    try {
      // Job will fail if observer rejects entries without TTL
      assertEquals(0, ToolRunner.run(conf, tool, argv.toArray(args)));
    } finally {
      // Clean up
      if (conf.getBoolean(DELETE_AFTER_LOAD_CONF, true)) {
        LOG.debug("Deleting test subdirectory");
        util.cleanupDataTestDirOnTestFS(table.getNameAsString());
      }
    }

    return tool;
  }

  public static class TTLCheckingObserver implements RegionCoprocessor, RegionObserver {

    @Override
    public Optional<RegionObserver> getRegionObserver() {
      return Optional.of(this);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit,
        Durability durability) throws IOException {
      Region region = e.getEnvironment().getRegion();
      if (!region.getRegionInfo().isMetaRegion()
          && !region.getRegionInfo().getTable().isSystemTable()) {
        // The put carries the TTL attribute
        if (put.getTTL() != Long.MAX_VALUE) {
          return;
        }
        throw new IOException("Operation does not have TTL set");
      }
    }
  }
}
