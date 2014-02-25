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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.TestMetaReaderEditor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ServiceException;

/**
 * Tests for region replicas. Sad that we cannot isolate these without bringing up a whole
 * cluster. See {@link TestRegionServerNoMaster}.
 */
@Category(MediumTests.class)
public class TestRegionReplicas {
  private static final int NB_SERVERS = 1;
  private static HTable table;
  private static final byte[] row = "TestRegionReplicas".getBytes();

  private static HRegionInfo hriPrimary;
  private static HRegionInfo hriSecondary;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();
  private static final byte[] f = HConstants.CATALOG_FAMILY;

  @BeforeClass
  public static void before() throws Exception {
    HTU.startMiniCluster(NB_SERVERS);
    final byte[] tableName = Bytes.toBytes(TestRegionReplicas.class.getSimpleName());

    // Create table then get the single region for our new table.
    table = HTU.createTable(tableName, f);

    hriPrimary = table.getRegionLocation(row, false).getRegionInfo();

    // mock a secondary region info to open
    hriSecondary = new HRegionInfo(hriPrimary.getTable(), hriPrimary.getStartKey(),
        hriPrimary.getEndKey(), hriPrimary.isSplit(), hriPrimary.getRegionId(), 1);

    // No master
    HTU.getHBaseCluster().getMaster().stopMaster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    table.close();
    HTU.shutdownMiniCluster();
  }

  @After
  public void after() throws Exception {
    // Clean the state if the test failed before cleaning the znode
    // It does not manage all bad failures, so if there are multiple failures, only
    //  the first one should be looked at.
    ZKAssign.deleteNodeFailSilent(HTU.getZooKeeperWatcher(), hriPrimary);
  }

  private HRegionServer getRS() {
    return HTU.getMiniHBaseCluster().getRegionServer(0);
  }

  private void openRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeOffline(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());
    // first version is '0'
    AdminProtos.OpenRegionRequest orr = RequestConverter.buildOpenRegionRequest(hri, 0, null);
    AdminProtos.OpenRegionResponse responseOpen = getRS().openRegion(null, orr);
    Assert.assertTrue(responseOpen.getOpeningStateCount() == 1);
    Assert.assertTrue(responseOpen.getOpeningState(0).
        equals(AdminProtos.OpenRegionResponse.RegionOpeningState.OPENED));
    checkRegionIsOpened(hri.getEncodedName());
  }

  private void closeRegion(HRegionInfo hri) throws Exception {
    ZKAssign.createNodeClosing(HTU.getZooKeeperWatcher(), hri, getRS().getServerName());

    AdminProtos.CloseRegionRequest crr = RequestConverter.buildCloseRegionRequest(
        hri.getEncodedName(), true);
    AdminProtos.CloseRegionResponse responseClose = getRS().closeRegion(null, crr);
    Assert.assertTrue(responseClose.getClosed());

    checkRegionIsClosed(hri.getEncodedName());

    ZKAssign.deleteClosedNode(HTU.getZooKeeperWatcher(), hri.getEncodedName(), getRS().getServerName());
  }

  private void checkRegionIsOpened(String encodedRegionName) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    Assert.assertTrue(getRS().getRegionByEncodedName(encodedRegionName).isAvailable());

    Assert.assertTrue(
        ZKAssign.deleteOpenedNode(HTU.getZooKeeperWatcher(), encodedRegionName, getRS().getServerName()));
  }


  private void checkRegionIsClosed(String encodedRegionName) throws Exception {

    while (!getRS().getRegionsInTransitionInRS().isEmpty()) {
      Thread.sleep(1);
    }

    try {
      Assert.assertFalse(getRS().getRegionByEncodedName(encodedRegionName).isAvailable());
    } catch (NotServingRegionException expected) {
      // That's how it work: if the region is closed we have an exception.
    }

    // We don't delete the znode here, because there is not always a znode.
  }

  @Test(timeout = 60000)
  public void testOpenRegionReplica() throws Exception {
    openRegion(hriSecondary);
    try {
      //load some data to primary
      HTU.loadNumericRows(table, f, 0, 1000);

      // assert that we can read back from primary
      Assert.assertEquals(1000, HTU.countRows(table));
    } finally {
      HTU.deleteNumericRows(table, f, 0, 1000);
      closeRegion(hriSecondary);
    }
  }

  /** Tests that the meta location is saved for secondary regions */
  @Test(timeout = 60000)
  public void testRegionReplicaUpdatesMetaLocation() throws Exception {
    openRegion(hriSecondary);
    HTable meta = null;
    try {
      meta = new HTable(HTU.getConfiguration(), TableName.META_TABLE_NAME);
      TestMetaReaderEditor.assertMetaLocation(meta, hriPrimary.getRegionName()
          , getRS().getServerName(), -1, 1, false);
    } finally {
      if (meta != null ) meta.close();
      closeRegion(hriSecondary);
    }
  }

  @Test(timeout = 60000)
  public void testRegionReplicaGets() throws Exception {
    try {
      //load some data to primary
      HTU.loadNumericRows(table, f, 0, 1000);
      // assert that we can read back from primary
      Assert.assertEquals(1000, HTU.countRows(table));
      // flush so that region replica can read
      HTU.getHBaseAdmin().flush(table.getTableName());

      openRegion(hriSecondary);

      // first try directly against region
      HRegion region = getRS().getFromOnlineRegions(hriSecondary.getEncodedName());
      assertGet(region, 42, true);

      assertGetRpc(hriSecondary, 42, true);

    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 1000);
      closeRegion(hriSecondary);
    }
  }

  private void assertGet(HRegion region, int value, boolean expect) throws IOException {
    byte[] row = Bytes.toBytes(String.valueOf(value));
    Get get = new Get(row);
    Result result = region.get(get);
    if (expect) {
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } else {
      result.isEmpty();
    }
  }

  // build a mock rpc
  private void assertGetRpc(HRegionInfo info, int value, boolean expect) throws IOException, ServiceException {
    byte[] row = Bytes.toBytes(String.valueOf(value));
    Get get = new Get(row);
    ClientProtos.GetRequest getReq = RequestConverter.buildGetRequest(info.getRegionName(), get);
    ClientProtos.GetResponse getResp =  getRS().get(null, getReq);
    Result result = ProtobufUtil.toResult(getResp.getResult());
    if (expect) {
      Assert.assertArrayEquals(row, result.getValue(f, null));
    } else {
      result.isEmpty();
    }
  }

  private void restartRegionServer() throws Exception {
    afterClass();
    before();
  }

  @Test(timeout = 300000)
  public void testRefreshStoreFiles() throws Exception {
    // enable store file refreshing
    final int refreshPeriod = 2000; // 2 sec
    HTU.getConfiguration().setInt("hbase.hstore.compactionThreshold", 100);
    HTU.getConfiguration().setInt(StorefileRefresherChore.REGIONSERVER_STOREFILE_REFRESH_PERIOD, refreshPeriod);
    // restart the region server so that it starts the refresher chore
    restartRegionServer();

    try {
      openRegion(hriSecondary);

      //load some data to primary
      HTU.loadNumericRows(table, f, 0, 1000);
      // assert that we can read back from primary
      Assert.assertEquals(1000, HTU.countRows(table));
      // flush so that region replica can read
      HTU.getHBaseAdmin().flush(table.getTableName());

      // ensure that chore is run
      Threads.sleep(4 * refreshPeriod);

      assertGetRpc(hriSecondary, 42, true);
      assertGetRpc(hriSecondary, 1042, false);

      //load some data to primary
      HTU.loadNumericRows(table, f, 1000, 1100);
      HTU.getHBaseAdmin().flush(table.getTableName());

      HTU.loadNumericRows(table, f, 2000, 2100);
      HTU.getHBaseAdmin().flush(table.getTableName());

      // ensure that chore is run
      Threads.sleep(4 * refreshPeriod);

      assertGetRpc(hriSecondary, 42, true);
      assertGetRpc(hriSecondary, 1042, true);
      assertGetRpc(hriSecondary, 2042, true);

      // ensure that we are see the 3 store files
      HRegion secondaryRegion = getRS().getFromOnlineRegions(hriSecondary.getEncodedName());
      Assert.assertEquals(3, secondaryRegion.getStore(f).getStorefilesCount());

      // force compaction
      HTU.compact(table.getName(), true);

      long wakeUpTime = System.currentTimeMillis() + 4 * refreshPeriod;
      while (System.currentTimeMillis() < wakeUpTime) {
        assertGetRpc(hriSecondary, 42, true);
        assertGetRpc(hriSecondary, 1042, true);
        assertGetRpc(hriSecondary, 2042, true);
        Threads.sleep(10);
      }

      // ensure that we see the compacted file only
      Assert.assertEquals(1, secondaryRegion.getStore(f).getStorefilesCount());

    } finally {
      HTU.deleteNumericRows(table, HConstants.CATALOG_FAMILY, 0, 1000);
      closeRegion(hriSecondary);
    }
  }
}
