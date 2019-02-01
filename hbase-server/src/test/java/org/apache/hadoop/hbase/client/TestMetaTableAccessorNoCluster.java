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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

/**
 * Test MetaTableAccessor but without spinning up a cluster.
 * We mock regionserver back and forth (we do spin up a zk cluster).
 */
@Category({MiscTests.class, MediumTests.class})
public class TestMetaTableAccessorNoCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMetaTableAccessorNoCluster.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestMetaTableAccessorNoCluster.class);
  private static final  HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Abortable ABORTABLE = new Abortable() {
    boolean aborted = false;
    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      this.aborted = true;
      throw new RuntimeException(e);
    }
    @Override
    public boolean isAborted()  {
      return this.aborted;
    }
  };

  @Before
  public void before() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @After
  public void after() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @Test
  public void testGetHRegionInfo() throws IOException {
    assertNull(MetaTableAccessor.getRegionInfo(new Result()));

    List<Cell> kvs = new ArrayList<>();
    Result r = Result.create(kvs);
    assertNull(MetaTableAccessor.getRegionInfo(r));

    byte [] f = HConstants.CATALOG_FAMILY;
    // Make a key value that doesn't have the expected qualifier.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.SERVER_QUALIFIER, f));
    r = Result.create(kvs);
    assertNull(MetaTableAccessor.getRegionInfo(r));
    // Make a key that does not have a regioninfo value.
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f,
      HConstants.REGIONINFO_QUALIFIER, f));
    RegionInfo hri = MetaTableAccessor.getRegionInfo(Result.create(kvs));
    assertTrue(hri == null);
    // OK, give it what it expects
    kvs.clear();
    kvs.add(new KeyValue(HConstants.EMPTY_BYTE_ARRAY, f, HConstants.REGIONINFO_QUALIFIER,
      RegionInfo.toByteArray(RegionInfoBuilder.FIRST_META_REGIONINFO)));
    hri = MetaTableAccessor.getRegionInfo(Result.create(kvs));
    assertNotNull(hri);
    assertTrue(RegionInfo.COMPARATOR.compare(hri, RegionInfoBuilder.FIRST_META_REGIONINFO) == 0);
  }

  /**
   * Test that MetaTableAccessor will ride over server throwing
   * "Server not running" IOEs.
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-3446">HBASE-3446</a>
   */
  @Test
  public void testRideOverServerNotRunning()
      throws IOException, InterruptedException, ServiceException {
    // Need a zk watcher.
    ZKWatcher zkw = new ZKWatcher(UTIL.getConfiguration(),
      this.getClass().getSimpleName(), ABORTABLE, true);
    // This is a servername we use in a few places below.
    ServerName sn = ServerName.valueOf("example.com", 1234, System.currentTimeMillis());

    ConnectionImplementation connection = null;
    try {
      // Mock an ClientProtocol. Our mock implementation will fail a few
      // times when we go to open a scanner.
      final ClientProtos.ClientService.BlockingInterface implementation =
        Mockito.mock(ClientProtos.ClientService.BlockingInterface.class);
      // When scan called throw IOE 'Server not running' a few times
      // before we return a scanner id.  Whats WEIRD is that these
      // exceptions do not show in the log because they are caught and only
      // printed if we FAIL.  We eventually succeed after retry so these don't
      // show.  We will know if they happened or not because we will ask
      // mockito at the end of this test to verify that scan was indeed
      // called the wanted number of times.
      List<Cell> kvs = new ArrayList<>();
      final byte [] rowToVerify = Bytes.toBytes("rowToVerify");
      kvs.add(new KeyValue(rowToVerify,
        HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER,
          RegionInfo.toByteArray(RegionInfoBuilder.FIRST_META_REGIONINFO)));
      kvs.add(new KeyValue(rowToVerify,
        HConstants.CATALOG_FAMILY, HConstants.SERVER_QUALIFIER,
        Bytes.toBytes(sn.getHostAndPort())));
      kvs.add(new KeyValue(rowToVerify,
        HConstants.CATALOG_FAMILY, HConstants.STARTCODE_QUALIFIER,
        Bytes.toBytes(sn.getStartcode())));
      final List<CellScannable> cellScannables = new ArrayList<>(1);
      cellScannables.add(Result.create(kvs));
      final ScanResponse.Builder builder = ScanResponse.newBuilder();
      for (CellScannable result : cellScannables) {
        builder.addCellsPerResult(((Result)result).size());
      }
      Mockito.when(implementation.scan((RpcController) Mockito.any(), (ScanRequest) Mockito.any()))
          .thenThrow(new ServiceException("Server not running (1 of 3)"))
          .thenThrow(new ServiceException("Server not running (2 of 3)"))
          .thenThrow(new ServiceException("Server not running (3 of 3)"))
          .thenAnswer(new Answer<ScanResponse>() {
            @Override
            public ScanResponse answer(InvocationOnMock invocation) throws Throwable {
              ((HBaseRpcController) invocation.getArgument(0)).setCellScanner(CellUtil
                  .createCellScanner(cellScannables));
              return builder.setScannerId(1234567890L).setMoreResults(false).build();
            }
          });
      // Associate a spied-upon Connection with UTIL.getConfiguration.  Need
      // to shove this in here first so it gets picked up all over; e.g. by
      // HTable.
      connection = HConnectionTestingUtility.getSpiedConnection(UTIL.getConfiguration());

      // Fix the location lookup so it 'works' though no network.  First
      // make an 'any location' object.
      final HRegionLocation anyLocation =
        new HRegionLocation(RegionInfoBuilder.FIRST_META_REGIONINFO, sn);
      final RegionLocations rl = new RegionLocations(anyLocation);
      // Return the RegionLocations object when locateRegion
      // The ugly format below comes of 'Important gotcha on spying real objects!' from
      // http://mockito.googlecode.com/svn/branches/1.6/javadoc/org/mockito/Mockito.html
      Mockito.doReturn(rl).when(connection).locateRegion((TableName) Mockito.any(),
        (byte[]) Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean(), Mockito.anyInt());

      // Now shove our HRI implementation into the spied-upon connection.
      Mockito.doReturn(implementation).when(connection).getClient(Mockito.any());

      // Scan meta for user tables and verify we got back expected answer.
      NavigableMap<RegionInfo, Result> hris =
        MetaTableAccessor.getServerUserRegions(connection, sn);
      assertEquals(1, hris.size());
      assertTrue(RegionInfo.COMPARATOR.compare(hris.firstEntry().getKey(),
        RegionInfoBuilder.FIRST_META_REGIONINFO) == 0);
      assertTrue(Bytes.equals(rowToVerify, hris.firstEntry().getValue().getRow()));
      // Finally verify that scan was called four times -- three times
      // with exception and then on 4th attempt we succeed
      Mockito.verify(implementation, Mockito.times(4)).scan((RpcController) Mockito.any(),
        (ScanRequest) Mockito.any());
    } finally {
      if (connection != null && !connection.isClosed()) {
        connection.close();
      }
      zkw.close();
    }
  }
}
