/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.hfile.HFileContextBuilder;
import org.apache.hadoop.hbase.master.MasterFileSystem;
import org.apache.hadoop.hbase.regionserver.StoreFileWriter;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * Testcase for HBASE-22632
 */
@Category({ MasterTests.class, MediumTests.class })
public class TestIgnoreUnknownFamily {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestIgnoreUnknownFamily.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private static final byte[] UNKNOWN_FAMILY = Bytes.toBytes("wrong_cf");

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDownAfterTest() throws IOException {
    Admin admin = UTIL.getAdmin();
    for (TableName tableName : admin.listTableNames()) {
      admin.disableTable(tableName);
      admin.deleteTable(tableName);
    }
  }

  private void addStoreFileToKnownFamily(RegionInfo region) throws IOException {
    MasterFileSystem mfs = UTIL.getMiniHBaseCluster().getMaster().getMasterFileSystem();
    Path regionDir =
      FSUtils.getRegionDirFromRootDir(CommonFSUtils.getRootDir(mfs.getConfiguration()), region);
    Path familyDir = new Path(regionDir, Bytes.toString(UNKNOWN_FAMILY));
    StoreFileWriter writer =
        new StoreFileWriter.Builder(mfs.getConfiguration(), mfs.getFileSystem())
            .withOutputDir(familyDir).withFileContext(new HFileContextBuilder().build()).build();
    writer.close();
  }

  @Test
  public void testSplit()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Admin admin = UTIL.getAdmin();
    admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build());
    RegionInfo region = admin.getRegions(tableName).get(0);
    addStoreFileToKnownFamily(region);
    admin.splitRegionAsync(region.getRegionName(), Bytes.toBytes(0)).get(30, TimeUnit.SECONDS);
  }

  @Test
  public void testMerge()
      throws IOException, InterruptedException, ExecutionException, TimeoutException {
    TableName tableName = TableName.valueOf(name.getMethodName());
    Admin admin = UTIL.getAdmin();
    admin.createTable(
      TableDescriptorBuilder.newBuilder(tableName)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build(),
      new byte[][] { Bytes.toBytes(0) });
    List<RegionInfo> regions = admin.getRegions(tableName);
    addStoreFileToKnownFamily(regions.get(0));
    admin.mergeRegionsAsync(regions.get(0).getEncodedNameAsBytes(),
      regions.get(1).getEncodedNameAsBytes(), false).get(30, TimeUnit.SECONDS);
  }
}
