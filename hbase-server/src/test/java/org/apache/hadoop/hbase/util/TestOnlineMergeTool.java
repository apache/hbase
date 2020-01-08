/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests merging a normal table's regions
 */
@Category({ MiscTests.class, MediumTests.class }) public class TestOnlineMergeTool {

  @ClassRule public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOnlineMergeTool.class);

  static final Logger LOG = LoggerFactory.getLogger(TestOnlineMergeTool.class);
  private final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private final byte[] COLUMN_NAME = Bytes.toBytes("f1");
  private static final byte[] VALUE;
  private Connection connection;
  private byte[][] splitKeys;

  static {
    // We will use the same value for the rows as that is not really important here
    String partialValue = String.valueOf(System.currentTimeMillis());
    StringBuilder val = new StringBuilder();
    while (val.length() < 100) {
      val.append(partialValue);
    }
    VALUE = Bytes.toBytes(val.toString());
  }

  @Before public void beforeSetUp() throws Exception {
    splitKeys =
      new byte[][] { Bytes.toBytes("a"), Bytes.toBytes("c"), Bytes.toBytes("b"), Bytes.toBytes("e"),
        Bytes.toBytes("f"), Bytes.toBytes("h"), Bytes.toBytes("i") };

    // Set maximum regionsize down.
    UTIL.getConfiguration().setLong(HConstants.HREGION_MAX_FILESIZE, 64L * 1024L * 1024L);
    // Make it so we don't split.
    UTIL.getConfiguration().setInt("hbase.regionserver.regionSplitLimit", 0);
    // Startup hdfs.  Its in here we'll be putting our manually made regions.
    UTIL.startMiniDFSCluster(1);
    // Create hdfs hbase rootdir.
    Path rootdir = UTIL.createRootDir();
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    if (fs.exists(rootdir)) {
      if (fs.delete(rootdir, true)) {
        LOG.info("Cleaned up existing " + rootdir);
      }
    }
    // Now create the root and meta regions and insert the data regions
    LOG.info("Starting mini zk cluster");
    UTIL.startMiniZKCluster();
    LOG.info("Starting mini hbase cluster");
    UTIL.startMiniHBaseCluster(1, 1);
    Configuration c = new Configuration(UTIL.getConfiguration());
    connection = ConnectionFactory.createConnection(c);
  }

  @After public void afterSetUp() throws Exception {
    UTIL.shutdownMiniCluster();
    LOG.info("After cluster shutdown");
  }

  /**
   * test merge help
   *
   * @throws ClassNotFoundException    Class not found exception
   * @throws NoSuchMethodException     exception
   * @throws IllegalAccessException    exception
   * @throws InvocationTargetException exception
   * @throws InstantiationException    exception
   */

  @Test public void testMergeHelp()
    throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
    InvocationTargetException, InstantiationException {
    String[] args = new String[] { "-h" };
    HBaseConfiguration hBaseConfiguration = new HBaseConfiguration(UTIL.getConfiguration());
    Class<?> aClass1 = Class.forName("org.apache.hadoop.hbase.util.OnlineMergeTool");
    Constructor<?> constructor = aClass1.getConstructor(HBaseConfiguration.class);
    OnlineMergeTool onlineMergeTool = (OnlineMergeTool) constructor.newInstance(hBaseConfiguration);
    try {
      onlineMergeTool.run(args);
      Assert.assertTrue(true);
    } catch (Exception e) {
      Assert.assertFalse(false);
    }
  }

  /**
   * test merge size based region
   *
   * @throws Exception exception
   */
  @Test(timeout = 500000) public void testMergeSizeBasedRegion() throws Exception {
    final String tableName = "test";
    // Table we are manually creating offline.
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(tableName)));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    Admin admin = connection.getAdmin();
    admin.createTable(desc, splitKeys);
    String[] args =
      new String[] { "--tableName=" + tableName, "--maxRegionSize=0", "--targetRegionCount=2",
        "--printExecutionPlan=false", "--configMergePauseTime=10000" };
    HBaseConfiguration hBaseConfiguration = new HBaseConfiguration(UTIL.getConfiguration());
    Class<?> aClass1 = Class.forName("org.apache.hadoop.hbase.util.OnlineMergeTool");
    Constructor<?> constructor = aClass1.getConstructor(HBaseConfiguration.class);
    OnlineMergeTool onlineMergeTool = (OnlineMergeTool) constructor.newInstance(hBaseConfiguration);
    onlineMergeTool.run(args);
    List<String> merge2regionName = getListRegionName(admin, tableName);
    Assert.assertEquals(Integer.parseInt(args[2].substring("--targetRegionCount=".length())),
      merge2regionName.size());
  }

  /**
   * test merge rang region
   *
   * @throws Exception exception
   */
  @Test(timeout = 500000) public void testMergeRangRegion() throws Exception {
    final String tableName = "testRangRegion";
    // Table we are manually creating offline.
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(tableName)));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    Admin admin = connection.getAdmin();
    admin.createTable(desc, splitKeys);
    String[] args = new String[7];
    args[0] = "--tableName=" + tableName;
    List<String> regionName = getListRegionName(admin, tableName);
    for (int i = 0; i < regionName.size(); i++) {
      if (0 == i) {
        args[1] = "--startRegion=" + regionName.get(i);
      }
      if (3 == i) {
        args[2] = "--stopRegion=" + regionName.get(i);
      }
    }
    args[3] = "--maxRegionSize=" + 0;
    args[4] = "--targetRegionCount=" + 2;
    args[5] = "--printExecutionPlan=false";
    args[6] = "--configMergePauseTime=10000";

    HBaseConfiguration hBaseConfiguration = new HBaseConfiguration(UTIL.getConfiguration());
    Class<?> aClass1 = Class.forName("org.apache.hadoop.hbase.util.OnlineMergeTool");
    Constructor<?> constructor = aClass1.getConstructor(HBaseConfiguration.class);
    OnlineMergeTool onlineMergeTool = (OnlineMergeTool) constructor.newInstance(hBaseConfiguration);
    onlineMergeTool.run(args);
    List<String> merge2regionName = getListRegionName(admin, tableName);
    Assert.assertEquals(regionName.size() - 1, merge2regionName.size());
  }

  /**
   * test merge time Based Region
   *
   * @throws Exception exception
   */
  @Test(timeout = 500000) public void testMergeTimeBasedRegion() throws Exception {
    final String tableName = "testTimeBasedRegion";
    // Table we are manually creating offline.
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(Bytes.toBytes(tableName)));
    desc.addFamily(new HColumnDescriptor(COLUMN_NAME));
    Admin admin = connection.getAdmin();
    admin.createTable(desc, splitKeys);
    String[] args = new String[6];
    SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    String format = DATE_FORMAT.format(System.currentTimeMillis() + 30 * 1000);
    args[0] = "--tableName=" + tableName;
    args[1] = "--maxRegionSize=" + 0;
    args[2] = "--maxRegionCreateTime=" + format;
    args[3] = "--targetRegionCount=" + 2;
    args[4] = "--printExecutionPlan=false";
    args[5] = "--configMergePauseTime=10000";
    HBaseConfiguration hBaseConfiguration = new HBaseConfiguration(UTIL.getConfiguration());
    Class<?> aClass1 = Class.forName("org.apache.hadoop.hbase.util.OnlineMergeTool");
    Constructor<?> constructor = aClass1.getConstructor(HBaseConfiguration.class);
    OnlineMergeTool onlineMergeTool = (OnlineMergeTool) constructor.newInstance(hBaseConfiguration);
    onlineMergeTool.run(args);
    List<String> merge2regionName = getListRegionName(admin, tableName);
    Assert.assertEquals(Integer.parseInt(args[3].substring("--targetRegionCount=".length())),
      merge2regionName.size());
  }

  private List<String> getListRegionName(Admin admin, String tableName) throws IOException {
    List<RegionInfo> tableRegions = admin.getRegions(TableName.valueOf(tableName));
    List<String> list = new ArrayList<String>();
    for (RegionInfo hri : tableRegions) {
      list.add(hri.getRegionNameAsString());
    }
    return list;
  }

}

