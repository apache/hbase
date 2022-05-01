/*
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
package org.apache.hadoop.hbase.regionserver.storefiletracker;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.ChunkCreator;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MemStoreLAB;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@RunWith(Parameterized.class)
@Category({ RegionServerTests.class, MediumTests.class })
public class TestMigrationStoreFileTracker {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrationStoreFileTracker.class);

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final TableDescriptor TD =
    TableDescriptorBuilder.newBuilder(TableName.valueOf("file_based_tracker"))
      .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF)).build();

  private static final RegionInfo RI = RegionInfoBuilder.newBuilder(TD.getTableName()).build();

  @Rule
  public TestName name = new TestName();

  @Parameter(0)
  public StoreFileTrackerFactory.Trackers srcImpl;

  @Parameter(1)
  public StoreFileTrackerFactory.Trackers dstImpl;

  private HRegion region;

  private Path rootDir;

  private WAL wal;

  @Parameters(name = "{index}: src={0}, dst={1}")
  public static List<Object[]> params() {
    List<Object[]> params = new ArrayList<>();
    for (StoreFileTrackerFactory.Trackers src : StoreFileTrackerFactory.Trackers.values()) {
      for (StoreFileTrackerFactory.Trackers dst : StoreFileTrackerFactory.Trackers.values()) {
        if (
          src == StoreFileTrackerFactory.Trackers.MIGRATION
            || dst == StoreFileTrackerFactory.Trackers.MIGRATION
        ) {
          continue;
        }
        if (src.equals(dst)) {
          continue;
        }
        params.add(new Object[] { src, dst });
      }
    }
    return params;
  }

  @BeforeClass
  public static void setUpBeforeClass() {
    ChunkCreator.initialize(MemStoreLAB.CHUNK_SIZE_DEFAULT, false, 0, 0, 0, null,
      MemStoreLAB.INDEX_CHUNK_SIZE_PERCENTAGE_DEFAULT);
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    conf.set(MigrationStoreFileTracker.SRC_IMPL, srcImpl.name().toLowerCase());
    conf.set(MigrationStoreFileTracker.DST_IMPL, dstImpl.name().toLowerCase());
    rootDir = UTIL.getDataTestDir(name.getMethodName().replaceAll("[=:\\[ ]", "_"));
    wal = HBaseTestingUtility.createWal(conf, rootDir, RI);
  }

  @After
  public void tearDown() throws IOException {
    if (region != null) {
      region.close();
    }
    Closeables.close(wal, true);
    UTIL.cleanupTestDir();
  }

  private List<String> getStoreFiles() {
    return Iterables.getOnlyElement(region.getStores()).getStorefiles().stream()
      .map(s -> s.getFileInfo().getPath().getName()).collect(Collectors.toList());
  }

  private HRegion createRegion(Class<? extends StoreFileTrackerBase> trackerImplClass)
    throws IOException {
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setClass(StoreFileTrackerFactory.TRACKER_IMPL, trackerImplClass, StoreFileTracker.class);
    return HRegion.createHRegion(RI, rootDir, conf, TD, wal, true);
  }

  private void reopenRegion(Class<? extends StoreFileTrackerBase> trackerImplClass)
    throws IOException {
    region.flush(true);
    List<String> before = getStoreFiles();
    region.close();
    Configuration conf = new Configuration(UTIL.getConfiguration());
    conf.setClass(StoreFileTrackerFactory.TRACKER_IMPL, trackerImplClass, StoreFileTracker.class);
    region = HRegion.openHRegion(rootDir, RI, TD, wal, conf);
    List<String> after = getStoreFiles();
    assertEquals(before.size(), after.size());
    assertThat(after, hasItems(before.toArray(new String[0])));
  }

  private void putData(int start, int end) throws IOException {
    for (int i = start; i < end; i++) {
      region.put(new Put(Bytes.toBytes(i)).addColumn(CF, CQ, Bytes.toBytes(i)));
      if (i % 30 == 0) {
        region.flush(true);
      }
    }
  }

  private void verifyData(int start, int end) throws IOException {
    for (int i = start; i < end; i++) {
      Result result = region.get(new Get(Bytes.toBytes(i)));
      assertEquals(i, Bytes.toInt(result.getValue(CF, CQ)));
    }
  }

  @Test
  public void testMigration() throws IOException {
    region = createRegion(srcImpl.clazz.asSubclass(StoreFileTrackerBase.class));
    putData(0, 100);
    verifyData(0, 100);
    reopenRegion(MigrationStoreFileTracker.class);
    verifyData(0, 100);
    region.compact(true);
    putData(100, 200);
    reopenRegion(dstImpl.clazz.asSubclass(StoreFileTrackerBase.class));
    verifyData(0, 200);
  }
}
