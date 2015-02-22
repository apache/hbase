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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.LoadTestDataGeneratorWithMOB;
import org.apache.hadoop.hbase.util.LoadTestTool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration Test for MOB ingest.
 */
@Category(IntegrationTests.class)
public class IntegrationTestIngestWithMOB extends IntegrationTestIngest {
  private static final char COLON = ':';

  private byte[] mobColumnFamily = LoadTestTool.COLUMN_FAMILY;
  public static final String THRESHOLD = "threshold";
  public static final String MIN_MOB_DATA_SIZE = "minMobDataSize";
  public static final String MAX_MOB_DATA_SIZE = "maxMobDataSize";
  private int threshold = 1024; // 1KB
  private int minMobDataSize = 512; // 512B
  private int maxMobDataSize = threshold * 5; // 5KB
  private static final long JUNIT_RUN_TIME = 2 * 60 * 1000; // 2 minutes

  //similar to LOAD_TEST_TOOL_INIT_ARGS except OPT_IN_MEMORY is removed
  protected String[] LOAD_TEST_TOOL_MOB_INIT_ARGS = {
      LoadTestTool.OPT_COMPRESSION,
      LoadTestTool.OPT_DATA_BLOCK_ENCODING,
      LoadTestTool.OPT_ENCRYPTION,
      LoadTestTool.OPT_NUM_REGIONS_PER_SERVER,
      LoadTestTool.OPT_REGION_REPLICATION,
  };

  @Override
  protected String[] getArgsForLoadTestToolInitTable() {
    List<String> args = new ArrayList<String>();
    args.add("-tn");
    args.add(getTablename().getNameAsString());
    // pass all remaining args from conf with keys <test class name>.<load test tool arg>
    String clazz = this.getClass().getSimpleName();
    for (String arg : LOAD_TEST_TOOL_MOB_INIT_ARGS) {
      String val = conf.get(String.format("%s.%s", clazz, arg));
      if (val != null) {
        args.add("-" + arg);
        args.add(val);
      }
    }
    args.add("-init_only");
    return args.toArray(new String[args.size()]);
  }

  @Override
  protected void addOptions() {
    super.addOptions();
    super.addOptWithArg(THRESHOLD, "The threshold to classify cells to mob data");
    super.addOptWithArg(MIN_MOB_DATA_SIZE, "Minimum value size for mob data");
    super.addOptWithArg(MAX_MOB_DATA_SIZE, "Maximum value size for mob data");
  }

  @Override
  protected void processOptions(CommandLine cmd) {
    super.processOptions(cmd);
    if (cmd.hasOption(THRESHOLD)) {
      threshold = Integer.parseInt(cmd.getOptionValue(THRESHOLD));
    }
    if (cmd.hasOption(MIN_MOB_DATA_SIZE)) {
      minMobDataSize = Integer.parseInt(cmd.getOptionValue(MIN_MOB_DATA_SIZE));
    }
    if (cmd.hasOption(MAX_MOB_DATA_SIZE)) {
      maxMobDataSize = Integer.parseInt(cmd.getOptionValue(MAX_MOB_DATA_SIZE));
    }
    if (minMobDataSize > maxMobDataSize) {
      throw new IllegalArgumentException(
          "The minMobDataSize should not be larger than minMobDataSize");
    }
  }

  @Test
  public void testIngest() throws Exception {
    runIngestTest(JUNIT_RUN_TIME, 100, 10, 1024, 10, 20);
  };

  @Override
  protected void initTable() throws IOException {
    super.initTable();

    byte[] tableName = getTablename().getName();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor tableDesc = admin.getTableDescriptor(tableName);
    LOG.info("Disabling table " + getTablename());
    admin.disableTable(tableName);
    for (HColumnDescriptor columnDescriptor : tableDesc.getFamilies()) {
      if(Arrays.equals(columnDescriptor.getName(), mobColumnFamily)) {
        columnDescriptor.setMobEnabled(true);
        columnDescriptor.setMobThreshold((long) threshold);
        admin.modifyColumn(tableName, columnDescriptor);
      }
    }
    LOG.info("Enabling table " + getTablename());
    admin.enableTable(tableName);
    admin.close();
  }

  @Override
  protected String[] getArgsForLoadTestTool(String mode, String modeSpecificArg, long startKey,
      long numKeys) {
    String[] args = super.getArgsForLoadTestTool(mode, modeSpecificArg, startKey, numKeys);
    List<String> tmp = new ArrayList<String>(Arrays.asList(args));
    // LoadTestDataGeneratorMOB:mobColumnFamily:minMobDataSize:maxMobDataSize
    tmp.add(HIPHEN + LoadTestTool.OPT_GENERATOR);
    StringBuilder sb = new StringBuilder(LoadTestDataGeneratorWithMOB.class.getName());
    sb.append(COLON);
    sb.append(Bytes.toString(mobColumnFamily));
    sb.append(COLON);
    sb.append(minMobDataSize);
    sb.append(COLON);
    sb.append(maxMobDataSize);
    tmp.add(sb.toString());
    return tmp.toArray(new String[tmp.size()]);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    IntegrationTestingUtility.setUseDistributedCluster(conf);
    int ret = ToolRunner.run(conf, new IntegrationTestIngestWithMOB(), args);
    System.exit(ret);
  }
}
