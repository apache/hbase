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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.exceptions.RegionServerStoppedException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService.BlockingInterface;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

/**
 * Test client behavior w/o setting up a cluster.
 * Mock up cluster emissions.
 */
public class TestClientNoCluster {
  private static final Log LOG = LogFactory.getLog(TestClientNoCluster.class);
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    this.conf = HBaseConfiguration.create();
    // Run my HConnection overrides.  Use my little HConnectionImplementation below which
    // allows me insert mocks and also use my Registry below rather than the default zk based
    // one so tests run faster and don't have zk dependency.
    this.conf.set("hbase.client.connection.impl", NoClusterConnection.class.getName());
    this.conf.set("hbase.client.registry.impl", SimpleRegistry.class.getName());
  }

  /**
   * Simple cluster registry inserted in place of our usual zookeeper based one.
   */
  static class SimpleRegistry implements Registry {
    final ServerName META_HOST = new ServerName("10.10.10.10", 60010, 12345);

    @Override
    public void init(HConnection connection) {
    }

    @Override
    public HRegionLocation getMetaRegionLocation() throws IOException {
      return new HRegionLocation(HRegionInfo.FIRST_META_REGIONINFO, META_HOST);
    }

    @Override
    public String getClusterId() {
      return HConstants.CLUSTER_ID_DEFAULT;
    }

    @Override
    public boolean isTableOnlineState(byte[] tableName, boolean enabled)
    throws IOException {
      return enabled;
    }

    @Override
    public int getCurrentNrHRS() throws IOException {
      return 1;
    }
  }

  @Test
  public void testDoNotRetryMetaScanner() throws IOException {
    MetaScanner.metaScan(this.conf, null);
  }

  @Test
  public void testDoNotRetryOnScan() throws IOException {
    // Go against meta else we will try to find first region for the table on construction which
    // means we'll have to do a bunch more mocking.  Tests that go against meta only should be
    // good for a bit of testing.
    HTable table = new HTable(this.conf, HConstants.META_TABLE_NAME);
    ResultScanner scanner = table.getScanner(HConstants.CATALOG_FAMILY);
    try {
      Result result = null;
      while ((result = scanner.next()) != null) {
        LOG.info(result);
      }
    } finally {
      scanner.close();
      table.close();
    }
  }

  /**
   * Override to shutdown going to zookeeper for cluster id and meta location.
   */
  static class NoClusterConnection extends HConnectionManager.HConnectionImplementation {
    final ClientService.BlockingInterface stub;

    NoClusterConnection(Configuration conf, boolean managed) throws IOException {
      super(conf, managed);
      // Mock up my stub so open scanner returns a scanner id and then on next, we throw
      // exceptions for three times and then after that, we return no more to scan.
      this.stub = Mockito.mock(ClientService.BlockingInterface.class);
      long sid = 12345L;
      try {
        Mockito.when(stub.scan((RpcController)Mockito.any(),
            (ClientProtos.ScanRequest)Mockito.any())).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).build()).
          thenThrow(new ServiceException(new RegionServerStoppedException("From Mockito"))).
          thenReturn(ClientProtos.ScanResponse.newBuilder().setScannerId(sid).
            setMoreResults(false).build());
      } catch (ServiceException e) {
        throw new IOException(e);
      }
    }

    @Override
    public BlockingInterface getClient(ServerName sn) throws IOException {
      return this.stub;
    }
  }
}